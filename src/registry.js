const axios = require('axios');

class ServiceRegistry {
    constructor() {
        // Map: ServiceName -> [ { url, healthy, failures, lastFailure } ]
        this.services = {};
        this.counters = {}; // For Round Robin
    }

    register(serviceName, url, healthURL) {
        if (!this.services[serviceName]) {
            this.services[serviceName] = [];
            this.counters[serviceName] = 0;
        }
        this.services[serviceName].push({
            url,
            healthURL,
            healthy: true,
            failures: 0,
            cooldown: 0
        });
        console.log(`Registered ${serviceName} at ${url}, with healthURL ${healthURL}`);
    }

    // Grade 2: Load Balancing (Round Robin)
    // Grade 2: Circuit Breaker Logic
    getNextAvailableInstance(serviceName) {
        const instances = this.services[serviceName];
        if (!instances || instances.length === 0) return null;

        const start = this.counters[serviceName] % instances.length;

        // Loop through instances to find a healthy one (Grade 4: HA)
        for (let i = 0; i < instances.length; i++) {
            const index = (start + i) % instances.length;
            const instance = instances[index];

            // Circuit Breaker Check
            if (!instance.healthy) {
                if (Date.now() > instance.cooldown) {
                    // Half-open state: try to use it
                    instance.failures = 0;
                    instance.healthy = true;
                } else {
                    continue; // Skip unhealthy
                }
            }

            this.counters[serviceName]++; // Increment RR counter
            return instance;
        }
        return null; // No healthy instances
    }

    reportSuccess(instance) {
        instance.failures = 0;
        instance.healthy = true;
    }

    reportFailure(instance) {
        instance.failures++;
        // Circuit Breaker Threshold
        if (instance.failures >= 3) {
            instance.healthy = false;
            instance.cooldown = Date.now() + 10000; // 10s cooldown
            console.log(`Circuit Breaker Tripped for ${instance.url}`);
        }
    }
}

module.exports = new ServiceRegistry();