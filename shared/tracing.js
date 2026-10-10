const { trace } = require('@opentelemetry/api');
const { NodeSDK } = require('@opentelemetry/sdk-node');

let sdk;

function initTracing() {
  if (!sdk) {
    sdk = new NodeSDK({
      serviceName: 'flowmaster',
      instrumentations: []
    });
    sdk.start();
  }
  return sdk;
}

module.exports = {
  initTracing,
  tracer: trace.getTracer('flowmaster')
};
