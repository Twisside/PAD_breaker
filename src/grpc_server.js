const grpc = require('@grpc/grpc-js');
const protoLoader = require('@grpc/proto-loader');
const path = require('path');
const persistence = require('./persistence'); // Reuse your existing persistence

const PROTO_PATH = path.join(__dirname, 'broker.proto');
const packageDefinition = protoLoader.loadSync(PROTO_PATH, {
    keepCase: true, longs: String, enums: String, defaults: true, oneofs: true
});
const brokerProto = grpc.loadPackageDefinition(packageDefinition).broker;

// Store active streams: { "topic_name": [call1, call2] }
const subscribers = {};

function subscribe(call) {
    const { service_name, topic } = call.request;
    console.log(`🔌 gRPC: ${service_name} subscribed to ${topic}`);

    if (!subscribers[topic]) {
        subscribers[topic] = [];
    }

    // Add this client's stream to the list
    subscribers[topic].push(call);

    // Handle client disconnect
    call.on('cancelled', () => {
        console.log(`🔌 gRPC: ${service_name} disconnected from ${topic}`);
        subscribers[topic] = subscribers[topic].filter(c => c !== call);
    });
}

function publish(call, callback) {
    const { topic, envelope } = call.request;

    // 1. Persist (Reuse your existing logic)
    persistence.addToTopic(topic, envelope);

    // 2. Broadcast to all active gRPC streams for this topic
    const activeStreams = subscribers[topic] || [];

    if (activeStreams.length > 0) {
        console.log(`📨 gRPC: Forwarding message on '${topic}' to ${activeStreams.length} subscribers`);
        activeStreams.forEach(stream => {
            stream.write(envelope);
        });
    } else {
        console.log(`⚠️ gRPC: No active subscribers for '${topic}'`);
    }

    callback(null, { success: true, message: "Message processed via gRPC" });
}

function startGrpcServer() {
    const server = new grpc.Server();
    server.addService(brokerProto.MessageBroker.service, {
        Publish: publish,
        Subscribe: subscribe
    });

    const PORT = '0.0.0.0:50051';
    server.bindAsync(PORT, grpc.ServerCredentials.createInsecure(), () => {
        console.log(`🚀 gRPC Broker running on ${PORT}`);
    });
}

startGrpcServer();