const express = require('express');
const bodyParser = require('body-parser');
const crypto = require('crypto'); // Built-in Node.js module for UUIDs
const registry = require('./registry');
const persistence = require('./persistence');
const broker = require('./broker');

const app = express();
app.use(bodyParser.json());

const PORT = 8380;

// --- Root Route (Health Check) ---
app.get('/', (req, res) => {
    res.json({
        status: "Online",
        message: "PAD Message Broker is running (JSON Mode)",
        endpoints: [
            "POST /register",
            "POST /subscribe/:topic",
            "POST /publish/:topic",
            "POST /send/:service",
            "GET /topics",
            "GET /dlc"
        ]
    });
});

// --- Admin / Setup Routes ---

app.post('/register', (req, res) => {
    const { serviceName, url, healthURL } = req.body;
    if (!serviceName || !url) {
        return res.status(400).send("Missing serviceName or url");
    }
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
    try {
        res.json(persistence.getTopics());
    } catch (e) {
        res.status(500).json({ error: "Persistence Error", details: e.message });
    }
});

app.get('/dlc', (req, res) => {
    res.json(persistence.getDLC());
});

// --- Communication Routes ---

// 1. Direct Service-to-Service Communication
app.post('/send/:service', async (req, res) => {
    try {
        const result = await broker.sendToService(req.params.service, req.body.data, req.body.path);
        res.json(result);
    } catch (e) {
        res.status(500).json({ error: e.message });
    }
});

// 2. Pub/Sub Topic Publishing (Standard JSON Envelope)
app.post('/publish/:topic', async (req, res) => {
    try {
        const rawBody = req.body;

        // Construct the Standard Envelope
        const envelope = {
            url: rawBody.url || "", // The sender's URL (optional)
            method: rawBody.method || "POST",
            // If 'msg' is present, use it. Otherwise, treat the whole body as the message.
            msg: rawBody.msg !== undefined ? rawBody.msg : rawBody,
            reply_to: rawBody.reply_to || "",
            correlation_id: rawBody.correlation_id || crypto.randomUUID()
        };

        const result = await broker.publishToTopic(req.params.topic, envelope);

        res.json({
            status: 'Published',
            correlation_id: envelope.correlation_id,
            details: result
        });
    } catch (e) {
        console.error("Publish Error:", e);
        res.status(500).json({ error: e.message });
    }
});

// 3. Two-Phase Commit Transaction
app.post('/transaction/2pc', async (req, res) => {
    try {
        const result = await broker.twoPhaseCommit(req.body);
        res.json(result);
    } catch (e) {
        res.status(500).json({ error: e.message });
    }
});

// --- Start Server ---
const server = app.listen(PORT, '0.0.0.0', () => {
    console.log(`✅ Message Broker running on http://localhost:${PORT}`);
});

// Handle Port Conflicts (Zombie Processes)
server.on('error', (e) => {
    if (e.code === 'EADDRINUSE') {
        console.error(`❌ FATAL ERROR: Port ${PORT} is already in use!`);
        console.error(`   Run 'taskkill /F /PID <PID>' (Windows) or 'kill -9 <PID>' (Linux) to fix it.`);
    } else {
        console.error("❌ Server Error:", e);
    }
});