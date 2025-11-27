const fs = require('fs');
const path = require('path');

const DATA_FILE = path.join(__dirname, '../data/storage.json');

class Persistence {
    constructor() {
        // Default state
        this.data = {
            queues: {},
            topics: {},
            deadLetter: []
        };

        // Create directory if it doesn't exist
        if (!fs.existsSync(path.dirname(DATA_FILE))) {
            fs.mkdirSync(path.dirname(DATA_FILE), { recursive: true });
        }

        // Initial load
        this.load();
    }

    load() {
        if (fs.existsSync(DATA_FILE)) {
            try {
                const fileData = fs.readFileSync(DATA_FILE, 'utf8');
                if (fileData.trim()) { // Check if string is not just whitespace
                    const parsed = JSON.parse(fileData);

                    // Safety Check: Ensure parsed data is actually an object
                    if (parsed && typeof parsed === 'object') {
                        // Merge with defaults to ensure keys exist
                        this.data = {
                            queues: parsed.queues || {},
                            topics: parsed.topics || {},
                            deadLetter: parsed.deadLetter || []
                        };
                    }
                }
            } catch (e) {
                console.error("Failed to load persistence file (using in-memory defaults):", e.message);
                // On error, we keep 'this.data' as is (the default state from constructor)
            }
        }
    }

    save() {
        try {
            // Write atomically: write to temp file then rename (prevents partial reads)
            const tempFile = `${DATA_FILE}.tmp`;
            fs.writeFileSync(tempFile, JSON.stringify(this.data, null, 2));
            fs.renameSync(tempFile, DATA_FILE);
        } catch (e) {
            console.error("Failed to save persistence file:", e.message);
        }
    }

    addToQueue(queueName, message) {
        this.load();
        if (!this.data.queues[queueName]) this.data.queues[queueName] = [];
        this.data.queues[queueName].push(message);
        this.save();
    }

    addToTopic(topicName, message) {
        this.load();
        if (!this.data.topics[topicName]) this.data.topics[topicName] = [];
        this.data.topics[topicName].push(message);
        this.save();
    }

    addToDLC(message, reason) {
        this.load();
        this.data.deadLetter.push({ ...message, failureReason: reason, timestamp: new Date() });
        this.save();
    }

    getTopics() {
        // this.load();
        // Safety check: ensure 'topics' exists before getting keys
        return this.data.topics ? Object.keys(this.data.topics) : [];
    }

    getDLC() {
        // this.load();
        return this.data.deadLetter || [];
    }
}

module.exports = new Persistence();