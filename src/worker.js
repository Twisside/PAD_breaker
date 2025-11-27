const { parentPort, workerData } = require('worker_threads');
const broker = require('./broker');
const registry = require('./registry');

async function processRequest() {
    const { type, payload, registryState } = workerData;

    // CRITICAL: Hydrate this thread's registry with data from the main thread
    if (registryState) {
        registry.setState(registryState);
    }

    try {
        let result;
        if (type === 'direct') {
            // Pass the specific path (endpoint) to the broker
            result = await broker.sendToService(payload.service, payload.body, payload.path);
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