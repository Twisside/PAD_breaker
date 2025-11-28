const axios = require('axios');
const registry = require('./registry');
const persistence = require('./persistence');

class Broker {

    // 1. Send to a specific service (Load Balanced)
    async sendToService(serviceName, payload, path = '') {
        const cleanPath = path && !path.startsWith('/') ? `/${path}` : path || '';
        return this.attemptRequest(serviceName, payload, 'POST', 0, cleanPath);
    }

    // 2. Publish to a Topic (Fan-out)
    async publishToTopic(topicName, payload) {
        // Save message for durability
        persistence.addToTopic(topicName, payload);

        // Get subscribers
        const subscribers = registry.getSubscribers(topicName);

        if (!subscribers || subscribers.length === 0) {
            console.log(`ℹ️ Broker: No subscribers for topic '${topicName}'`);
            return [];
        }

        console.log(`📨 Broker: Forwarding '${topicName}' to ${subscribers.length} subscribers...`);

        // Send to all subscribers in parallel
        const results = await Promise.allSettled(
            subscribers.map(sub =>
                this.sendToService(sub.serviceName, payload, sub.endpoint)
            )
        );

        return results;
    }

    // Internal Helper: Retry Logic & Circuit Breaker Check
    async attemptRequest(serviceName, payload, method, attempts = 0, path = '') {
        const instance = registry.getNextAvailableInstance(serviceName);

        if (!instance) {
            const errorMsg = `No healthy instances available for service: ${serviceName}`;
            console.error(`❌ Broker: ${errorMsg}`);
            persistence.addToDLC(payload, errorMsg);
            throw new Error(errorMsg);
        }

        const baseURL = instance.url.endsWith('/') ? instance.url.slice(0, -1) : instance.url;
        const fullURL = `${baseURL}${path}`;

        try {
            const response = await axios({
                method: method,
                url: fullURL,
                data: payload,
                timeout: 5000
            });
            registry.reportSuccess(instance);
            return response.data;
        } catch (error) {
            console.error(`⚠️ Broker: Failed to reach ${fullURL} - ${error.message}`);
            registry.reportFailure(instance);

            if (attempts < 2) {
                console.log(`🔄 Broker: Retrying ${serviceName}...`);
                return this.attemptRequest(serviceName, payload, method, attempts + 1, path);
            } else {
                persistence.addToDLC(payload, `Max retries reached for ${serviceName}`);
                throw error;
            }
        }
    }

    // 3. Two-Phase Commit (Simplified)
    async twoPhaseCommit(transactionData) {
        const { services, data } = transactionData;
        console.log("🔄 2PC: Phase 1 - Prepare");

        try {
            for (const svc of services) {
                await this.attemptRequest(svc, { type: '2PC_PREPARE', data }, 'POST', 0, '/2pc/prepare');
            }
        } catch (e) {
            console.log("❌ 2PC: Phase 1 Failed. Rolling back.");
            await this.broadcastRollback(services, data);
            return { status: 'aborted', reason: e.message };
        }

        console.log("✅ 2PC: Phase 2 - Commit");
        try {
            for (const svc of services) {
                await this.attemptRequest(svc, { type: '2PC_COMMIT', data }, 'POST', 0, '/2pc/commit');
            }
            return { status: 'committed' };
        } catch (e) {
            return { status: 'error', reason: "Partial Commit Failure" };
        }
    }

    async broadcastRollback(services, data) {
        for (const svc of services) {
            try {
                await this.attemptRequest(svc, { type: '2PC_ROLLBACK', data }, 'POST', 0, '/2pc/rollback');
            } catch (e) { /* Best effort */ }
        }
    }
}

module.exports = new Broker();