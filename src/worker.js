const { parentPort, workerData } = require('worker_threads');
const broker = require('./broker');
const registry = require('./registry');

// Re-hydrate registry if needed or pass necessary connection data
// Note: In a real complex app, sharing state (Circuit Breaker status)
// between threads is hard. Here we assume the worker handles the *execution*
// logic but might share state via SharedArrayBuffer or simply by being short-lived.

// For this assignment, we simulate the processing isolation:
async function processRequest() {
    const { type, payload } = workerData;

    try {
        let result;
        if (type === 'direct') {
            result = await broker.sendToService(payload.service, payload.body);
        } else if (type === 'topic') {
            result = await broker.publishToTopic(payload.topic, payload.body);
        } else if (type === '2pc') {
            result = await broker.twoPhaseCommit(payload.body);
        }
        parentPort.postMessage({ status: 'success', data: result });
    } catch (error) {
        parentPort.postMessage({ status: 'error', message: error.message });
    }
}

processRequest();