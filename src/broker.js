const axios = require('axios');
const registry = require('./registry');
const persistence = require('./persistence');

class Broker {

    // Grade 3: Subscriber-based (Direct to Service)
    async sendToService(serviceName, payload) {
        return this.attemptRequest(serviceName, payload, 'POST');
    }

    // Grade 3: Topic-based (Fan-out)
    async publishToTopic(topicName, payload) {
        persistence.addToTopic(topicName, payload);

        // In a real app, you'd have a subscription map.
        // Here we assume topics map to services based on config or broadcast.
        // For demo, let's say 'order_created' topic goes to 'inventory' and 'shipping'.
        const subscribers = this.getSubscribersForTopic(topicName);

        const results = await Promise.allSettled(
            subscribers.map(svc => this.sendToService(svc, payload))
        );
        return results;
    }

    getSubscribersForTopic(topic) {
        // Hardcoded map for Grade 3 demonstration
        const map = {
            'order_created': ['inventory-service', 'shipping-service'],
            'payment_processed': ['order-service', 'email-service']
        };
        return map[topic] || [];
    }

    // Grade 4: HA & Rerouting logic
    async attemptRequest(serviceName, payload, method, attempts = 0) {
        const instance = registry.getNextAvailableInstance(serviceName);

        if (!instance) {
            // Grade 6: Dead Letter Channel
            persistence.addToDLC(payload, `No healthy instances for ${serviceName}`);
            throw new Error(`Service Unavailable: ${serviceName}`);
        }

        try {
            const response = await axios({
                method: method,
                url: instance.url,
                data: payload,
                timeout: 5000
            });
            registry.reportSuccess(instance);
            return response.data;
        } catch (error) {
            console.error(`Failed to reach ${instance.url}`);
            registry.reportFailure(instance);

            // Grade 4: Reroute to another instance recursively
            if (attempts < 3) {
                console.log(`Rerouting request for ${serviceName}...`);
                return this.attemptRequest(serviceName, payload, method, attempts + 1);
            } else {
                // Grade 6: DLC on final failure
                persistence.addToDLC(payload, `Max retries reached for ${serviceName}`);
                throw error;
            }
        }
    }

    // Grade 7: 2 Phase Commit
    async twoPhaseCommit(transactionData) {
        const { services, data } = transactionData;
        // services = ['order-service', 'inventory-service']

        console.log("2PC: Phase 1 - Prepare");
        try {
            // Phase 1: Ask all to Prepare
            for (const svc of services) {
                await this.attemptRequest(svc, { type: '2PC_PREPARE', data }, 'POST');
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
                await this.attemptRequest(svc, { type: '2PC_COMMIT', data }, 'POST');
            }
            return { status: 'committed' };
        } catch (e) {
            // This is a critical failure state (Split Brain risk), usually requires manual intervention or complex recovery
            console.error("CRITICAL: Commit phase failed partially");
            return { status: 'error', reason: "Partial Commit Failure" };
        }
    }

    async broadcastRollback(services, data) {
        for (const svc of services) {
            try {
                await this.attemptRequest(svc, { type: '2PC_ROLLBACK', data }, 'POST');
            } catch (e) { /* Best effort rollback */ }
        }
    }
}

module.exports = new Broker();