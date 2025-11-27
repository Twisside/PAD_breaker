const axios = require('axios');

class ServiceRegistry {
    constructor() {
        // Map: ServiceName -> [ { url, healthy, failures, lastFailure } ]
        this.services = {};
        this.counters = {};

        // NEW: Map: TopicName -> [ { serviceName, endpoint } ]
        this.subscriptions = {};
    }

    register(serviceName, url, healthURL) {
        if (!this.services[serviceName]) {
            this.services[serviceName] = [];
            this.counters[serviceName] = 0;
        }
        // Prevent duplicate registration for demo simplicity
        const exists = this.services[serviceName].find(s => s.url === url);
        if (!exists) {
            this.services[serviceName].push({
                url,
                healthURL,
                healthy: true,
                failures: 0,
                cooldown: 0
            });
            console.log(`Registered ${serviceName} at ${url}`);
        }
    }

    // NEW: Subscribe a service to a topic with a specific endpoint
    subscribe(serviceName, topic, endpoint) {
        if (!this.subscriptions[topic]) {
            this.subscriptions[topic] = [];
        }
        // Avoid duplicates
        const exists = this.subscriptions[topic].find(s => s.serviceName === serviceName && s.endpoint === endpoint);
        if (!exists) {
            this.subscriptions[topic].push({ serviceName, endpoint });
            console.log(`Service '${serviceName}' subscribed to '${topic}' at endpoint '${endpoint}'`);
        }
    }

    getSubscribers(topic) {
        return this.subscriptions[topic] || [];
    }

    getNextAvailableInstance(serviceName) {
        const instances = this.services[serviceName];
        if (!instances || instances.length === 0) return null;

        const start = this.counters[serviceName] % instances.length;

        for (let i = 0; i < instances.length; i++) {
            const index = (start + i) % instances.length;
            const instance = instances[index];

            if (!instance.healthy) {
                if (Date.now() > instance.cooldown) {
                    instance.failures = 0;
                    instance.healthy = true;
                } else {
                    continue;
                }
            }

            this.counters[serviceName]++;
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
            instance.cooldown = Date.now() + 10000;
            console.log(`Circuit Breaker Tripped for ${instance.url}`);
        }
    }

    // NEW: Helper to export state for Workers
    getState() {
        return {
            services: this.services,
            subscriptions: this.subscriptions,
            counters: this.counters
        };
    }

    // NEW: Helper to import state in Workers
    setState(state) {
        this.services = state.services;
        this.subscriptions = state.subscriptions;
        this.counters = state.counters;
    }
}

module.exports = new ServiceRegistry();