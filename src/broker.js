const axios = require('axios');
const registry = require('./registry');
const persistence = require('./persistence');

class Broker {

    // Updated: Accepts 'path' to append to the service URL
    async sendToService(serviceName, payload, path = '') {
        // Ensure path starts with / if provided
        const cleanPath = path && !path.startsWith('/') ? `/${path}` : path || '';
        return this.attemptRequest(serviceName, payload, 'POST', 0, cleanPath);
    }

    async publishToTopic(topicName, payload) {
        persistence.addToTopic(topicName, payload);

        // NEW: Get dynamic subscribers from registry
        const subscribers = registry.getSubscribers(topicName);
        // subscribers = [ { serviceName: 'inventory', endpoint: '/update-stock' }, ... ]

        if (!subscribers || subscribers.length === 0) {
            console.log(`No subscribers for topic ${topicName}`);
            return [];
        }

        const results = await Promise.allSettled(
            subscribers.map(sub =>
                this.sendToService(sub.serviceName, payload, sub.endpoint)
            )
        );
        return results;
    }

    // Removed hardcoded getSubscribersForTopic (now handled by registry)

    // Updated: Accepts 'path' to append to URL
    async attemptRequest(serviceName, payload, method, attempts = 0, path = '') {
        const instance = registry.getNextAvailableInstance(serviceName);

        if (!instance) {
            persistence.addToDLC(payload, `No healthy instances for ${serviceName}`);
            throw new Error(`Service Unavailable: ${serviceName}`);
        }

        // Construct full URL
        // Remove trailing slash from base URL to avoid double slashes
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
            console.error(`Failed to reach ${fullURL}`);
            registry.reportFailure(instance);

            if (attempts < 3) {
                console.log(`Rerouting request for ${serviceName}...`);
                return this.attemptRequest(serviceName, payload, method, attempts + 1, path);
            } else {
                persistence.addToDLC(payload, `Max retries reached for ${serviceName}`);
                throw error;
            }
        }
    }

    async twoPhaseCommit(transactionData) {
        const { services, data } = transactionData;
        console.log("2PC: Phase 1 - Prepare");

        try {
            // Phase 1: Prepare
            // Note: For 2PC, we assume a standard endpoint '/2pc/prepare' exists on services
            for (const svc of services) {
                await this.attemptRequest(svc, { type: '2PC_PREPARE', data }, 'POST', 0, '/2pc/prepare');
            }
        } catch (e) {
            console.log("2PC: Phase 1 Failed. Rolling back.");
            await this.broadcastRollback(services, data);
            return { status: 'aborted', reason: e.message };
        }

        console.log("2PC: Phase 2 - Commit");
        try {
            // Phase 2: Commit
            for (const svc of services) {
                await this.attemptRequest(svc, { type: '2PC_COMMIT', data }, 'POST', 0, '/2pc/commit');
            }
            return { status: 'committed' };
        } catch (e) {
            console.error("CRITICAL: Commit phase failed partially");
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