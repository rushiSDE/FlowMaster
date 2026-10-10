const client = require('prom-client');

const register = new client.Registry();
client.collectDefaultMetrics({ register });

const workflowCounter = new client.Counter({
  name: 'flowmaster_workflows_total',
  help: 'Total number of workflow submissions by outcome',
  labelNames: ['status'],
  registers: [register]
});

const failedWorkflowCounter = new client.Counter({
  name: 'flowmaster_workflow_failures_total',
  help: 'Number of failed workflows',
  labelNames: ['step'],
  registers: [register]
});

const retryCounter = new client.Counter({
  name: 'flowmaster_retries_total',
  help: 'Number of retries triggered for workflow steps',
  labelNames: ['step'],
  registers: [register]
});

const processingLatency = new client.Histogram({
  name: 'flowmaster_processing_latency_ms',
  help: 'Workflow processing latency in milliseconds',
  buckets: [25, 50, 100, 250, 500, 1000, 2000],
  registers: [register]
});

module.exports = {
  register,
  workflowCounter,
  failedWorkflowCounter,
  retryCounter,
  processingLatency
};
