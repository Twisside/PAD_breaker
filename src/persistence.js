const fs = require('fs');
const path = require('path');

const DATA_FILE = path.join(__dirname, '../data/storage.json');

class Persistence {
    constructor() {
        this.data = {
            queues: {},
            topics: {},
            deadLetter: []
        };
        // Create file if it doesn't exist
        if (!fs.existsSync(path.dirname(DATA_FILE))) {
            fs.mkdirSync(path.dirname(DATA_FILE), { recursive: true });
        }
        this.load();
    }

    load() {
        if (fs.existsSync(DATA_FILE)) {
            try {
                const fileData = fs.readFileSync(DATA_FILE, 'utf8');
                // Only parse if file is not empty
                if (fileData) {
                    this.data = JSON.parse(fileData);
                }
            } catch (e) {
                console.error("Failed to load persistence file:", e.message);
            }
        }
    }

    save() {
        fs.writeFileSync(DATA_FILE, JSON.stringify(this.data, null, 2));
    }

    addToQueue(queueName, message) {
        this.load(); // Reload before modifying to get latest state
        if (!this.data.queues[queueName]) this.data.queues[queueName] = [];
        this.data.queues[queueName].push(message);
        this.save();
    }

    addToTopic(topicName, message) {
        this.load(); // Reload before modifying
        if (!this.data.topics[topicName]) this.data.topics[topicName] = [];
        this.data.topics[topicName].push(message);
        this.save();
    }

    addToDLC(message, reason) {
        this.load(); // Reload before modifying
        this.data.deadLetter.push({ ...message, failureReason: reason, timestamp: new Date() });
        this.save();
    }

    getTopics() {
        this.load(); // <--- CRITICAL: Reload from disk to see Worker changes
        return Object.keys(this.data.topics);
    }

    getDLC() {
        this.load(); // <--- CRITICAL: Reload from disk
        return this.data.deadLetter;
    }
}

module.exports = new Persistence();