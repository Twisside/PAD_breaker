const express = require('express');
const bodyParser = require('body-parser');
const crypto = require('crypto');
const protobuf = require('protobufjs');
const path = require('path');

const registry = require('./registry');
const persistence = require('./persistence');
const broker = require('./broker');

const app = express();
const PORT = 8380;

// --- 1. Load Protocol Buffer Schema ---
const protoRoot = protobuf.loadSync(path.join(__dirname, 'envelope.proto'));
const MessageEnvelope = protoRoot.lookupType("broker.MessageEnvelope");

// --- 2. Middleware ---
// Parse JSON for standard requests
app.use(bodyParser.json());
// Parse Raw Buffers for Protobuf requests (identified by Content-Type)
app.use(bodyParser.raw({ type: 'application/x-protobuf', limit: '10mb' }));

// --- Root Route ---
app.get('/', (req, res) => {
    res.json({ status: "Online", protocols: ["JSON", "Protobuf"] });
});

// --- Admin Routes (Unchanged) ---
app.get('/topics', (req, res) => res.json(persistence.getTopics()));
app.get('/dlc', (req, res) => res.json(persistence.getDLC()));

app.post('/register', (req, res) => {
    registry.register(req.body.serviceName, req.body.url, req.body.healthURL);
    res.send('Registered');
});

app.post('/subscribe/:topic', (req, res) => {
    registry.subscribe(req.body.service, req.params.topic, req.body.endpoint);
    res.json({ status: 'subscribed' });
});

// --- UPDATED: Publish Route supporting Protobuf ---
app.post('/publish/:topic', async (req, res) => {
    try {
        let envelope = {};

        // Check Content-Type to determine how to parse
        if (req.get('Content-Type') === 'application/x-protobuf') {

            // 1. Decode Binary Buffer
            const buffer = req.body;
            if (!Buffer.isBuffer(buffer) || buffer.length === 0) {
                throw new Error("Empty or invalid buffer received");
            }

            try {
                // Decode protobuf to JS object
                const decodedMessage = MessageEnvelope.decode(buffer);
                // Convert to standard JS object (handling defaults)
                envelope = MessageEnvelope.toObject(decodedMessage, {
                    defaults: true,
                    longs: String,
                    enums: String,
                    bytes: String,
                });
            } catch (decodeError) {
                return res.status(400).json({ error: "Invalid Protobuf format", details: decodeError.message });
            }

        } else {
            // Fallback to existing JSON logic
            const rawBody = req.body;
            envelope = {
                url: rawBody.url || "",
                method: rawBody.method || "POST",
                msg: typeof rawBody.msg === 'object' ? JSON.stringify(rawBody.msg) : (rawBody.msg || ""),
                reply_to: rawBody.reply_to || "",
                correlation_id: rawBody.correlation_id
            };
        }

        // Ensure Correlation ID exists
        if (!envelope.correlation_id) {
            envelope.correlation_id = crypto.randomUUID();
        }

        // Pass to Broker (Broker processes it as a standard JS object)
        const result = await broker.publishToTopic(req.params.topic, envelope);

        res.json({
            status: 'Published',
            format: req.get('Content-Type') === 'application/x-protobuf' ? 'Protobuf' : 'JSON',
            correlation_id: envelope.correlation_id,
            details: result
        });

    } catch (e) {
        console.error(e);
        res.status(500).json({ error: e.message });
    }
});

// --- Start Server ---
const server = app.listen(PORT, '0.0.0.0', () => {
    console.log(`Broker running on http://localhost:${PORT}`);
});