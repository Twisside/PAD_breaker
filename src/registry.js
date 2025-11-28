const axios = require('axios');
class ServiceRegistry {
    constructor() {
        this.services = {}; // { serviceName: [ { url, healthy, ... } ] }
        this.subscriptions = {}; // { topic: [ { serviceName, endpoint } ] }
        this.counters = {}; // For Round-Robin load balancing
    }

    register(serviceName, url, healthURL) {
        if (!this.services[serviceName]) {
            this.services[serviceName] = [];
            this.counters[serviceName] = 0;
        }
        // Prevent duplicates
        const exists = this.services[serviceName].find(s => s.url === url);
        if (!exists) {
            this.services[serviceName].push({
                url,
                healthURL,
                healthy: true,
                failures: 0,
                cooldown: 0
            });
            console.log(`✅ Registry: Registered '${serviceName}' at ${url}`);
        }
    }

    subscribe(serviceName, topic, endpoint) {
        if (!this.subscriptions[topic]) {
            this.subscriptions[topic] = [];
        }
        const exists = this.subscriptions[topic].find(s => s.serviceName === serviceName && s.endpoint === endpoint);
        if (!exists) {
            this.subscriptions[topic].push({ serviceName, endpoint });
            console.log(`✅ Registry: '${serviceName}' subscribed to '${topic}'`);
        }
    }

    getSubscribers(topic) {
        return this.subscriptions[topic] || [];
    }

    getNextAvailableInstance(serviceName) {
        const instances = this.services[serviceName];
        if (!instances || instances.length === 0) return null;

        for (let i = 0; i < instances.length; i++) {
            const index = (this.counters[serviceName] + i) % instances.length;
            const instance = instances[index];

            // Check health / cooldown
            if (!instance.healthy) {
                if (Date.now() > instance.cooldown) {
                    instance.healthy = true;
                    instance.failures = 0;
                } else {
                    continue;
                }
            }

            // Update Round-Robin counter
            this.counters[serviceName] = (this.counters[serviceName] + 1) % instances.length;
            return instance;
        }
        return null;
    }

    reportSuccess(instance) {
        instance.failures = 0;
        instance.healthy = true;
    }

    reportFailure(instance) {
        instance.failures++;
        if (instance.failures >= 3) {
            instance.healthy = false;
            instance.cooldown = Date.now() + 10000; // 10s cooldown
            console.log(`⚠️ Circuit Breaker: Instance ${instance.url} marked unhealthy.`);
        }
    }
}

module.exports = new ServiceRegistry();