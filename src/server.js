const express = require('express');
const bodyParser = require('body-parser');
const { Worker } = require('worker_threads');
const path = require('path');
const registry = require('./registry');
const persistence = require('./persistence');

const app = express();
app.use(bodyParser.json());

const PORT = 8380;

// Helper to offload to thread (Grade 2)
function runInWorker(type, payload) {
    return new Promise((resolve, reject) => {
        const worker = new Worker(path.join(__dirname, 'worker.js'), {
            workerData: { type, payload }
        });
        worker.on('message', resolve);
        worker.on('error', reject);
        worker.on('exit', (code) => {
            if (code !== 0) reject(new Error(`Worker stopped with exit code ${code}`));
        });
    });
}

// --- Admin / Setup Routes ---

// Register a service instance (Gateway or Service calls this on startup)
app.post('/register', (req, res) => {
    const { serviceName, url } = req.body;
    registry.register(serviceName, url);
    res.send('Registered');
});

// Grade 8: Fetch list of topics
app.get('/topics', (req, res) => {
    res.json(persistence.getTopics());
});

// Grade 8: Fetch Dead Letter Channel
app.get('/dlc', (req, res) => {
    res.json(persistence.getDLC());
});

// --- Communication Routes ---

// Grade 3: Subscriber-based (Gateway knows target)
app.post('/send/:service', async (req, res) => {
    try {
        // Grade 2: Thread-per-Request implementation
        const result = await runInWorker('direct', {
            service: req.params.service,
            body: req.body
        });

        if(result.status === 'error') throw new Error(result.message);
        res.json(result.data);
    } catch (e) {
        res.status(500).json({ error: e.message });
    }
});

// Grade 3: Topic-based (Service fires event)
app.post('/publish/:topic', async (req, res) => {
    try {
        const result = await runInWorker('topic', {
            topic: req.params.topic,
            body: req.body
        });
        res.json({ status: 'Published', details: result });
    } catch (e) {
        res.status(500).json({ error: e.message });
    }
});

// Grade 7: 2 Phase Commit Transaction
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