const express = require('express');
const bodyParser = require('body-parser');
const { Worker } = require('worker_threads');
const path = require('path');
const crypto = require('crypto'); // Built-in Node.js module for UUIDs
const registry = require('./registry');
const persistence = require('./persistence');

const app = express();
app.use(bodyParser.json());

const PORT = 8380;

// Helper to offload to thread
function runInWorker(type, payload) {
    return new Promise((resolve, reject) => {
        const registryState = registry.getState();

        const worker = new Worker(path.join(__dirname, 'worker.js'), {
            workerData: {
                type,
                payload,
                registryState
            }
        });
        worker.on('message', resolve);
        worker.on('error', reject);
        worker.on('exit', (code) => {
            if (code !== 0) reject(new Error(`Worker stopped with exit code ${code}`));
        });
    });
}

// --- Admin / Setup Routes ---

app.post('/register', (req, res) => {
    const { serviceName, url, healthURL } = req.body;
    registry.register(serviceName, url, healthURL);
    res.send('Registered');
});

app.post('/subscribe/:topic', (req, res) => {
    const { topic } = req.params;
    const { service, endpoint } = req.body;

    if (!service || !endpoint) {
        return res.status(400).json({ error: "Missing 'service' or 'endpoint' in request body" });
    }

    registry.subscribe(service, topic, endpoint);
    res.json({ status: 'subscribed', topic, service, endpoint });
});

app.get('/topics', (req, res) => {
    res.json(persistence.getTopics());
});

app.get('/dlc', (req, res) => {
    res.json(persistence.getDLC());
});

// --- Communication Routes ---

app.post('/send/:service', async (req, res) => {
    try {
        const result = await runInWorker('direct', {
            service: req.params.service,
            path: req.body.path,
            body: req.body.data
        });

        if(result.status === 'error') throw new Error(result.message);
        res.json(result.data);
    } catch (e) {
        res.status(500).json({ error: e.message });
    }
});

// UPDATED: Topic Publish with Standard Envelope
app.post('/publish/:topic', async (req, res) => {
    try {
        const rawBody = req.body;

        // Construct the Standard Envelope
        // We validate that 'msg' exists, or we treat the whole body as 'msg' if strictly needed.
        // However, based on your request, we expect the sender to conform to the structure.

        const envelope = {
            url: rawBody.url || "", // The sender's URL (optional if not provided)
            method: rawBody.method || "POST",
            msg: rawBody.msg || rawBody, // Fallback: if 'msg' is missing, assume the whole body is the message
            reply_to: rawBody.reply_to || "",
            correlation_id: rawBody.correlation_id || crypto.randomUUID() // Auto-generate if missing
        };

        const result = await runInWorker('topic', {
            topic: req.params.topic,
            body: envelope // Pass the envelope as the payload
        });

        res.json({
            status: 'Published',
            correlation_id: envelope.correlation_id,
            details: result
        });
    } catch (e) {
        res.status(500).json({ error: e.message });
    }
});

app.post('/transaction/2pc', async (req, res) => {
    try {
        const result = await runInWorker('2pc', { body: req.body });
        res.json(result.data);
    } catch (e) {
        res.status(500).json({ error: e.message });
    }
});

app.listen(PORT, () => {
    console.log(`Message Broker running on port ${PORT}`);
});