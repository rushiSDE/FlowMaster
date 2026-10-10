const { createApp, processWorkflowEvent, recoverPendingWorkflows } = require('./app');
const logger = require('../../../shared/logger');
const { createConsumer } = require('../../../shared/kafka');

const app = createApp();
const port = Number(process.env.ORCHESTRATOR_PORT || 3001);

const consumerBundle = createConsumer('orchestrator-group', ['flowmaster.verification.results', 'flowmaster.credit.results'], async ({ message }) => {
  const payload = JSON.parse(message.value.toString());
  logger.info({ payload }, 'Workflow result received');

  const event = payload.payload || payload;
  const workflowId = event.workflowId || payload.workflowId;
  if (!workflowId) return;

  await processWorkflowEvent(workflowId, {
    eventId: event.eventId || `${workflowId}:${Date.now()}`,
    workflowId,
    stepName: event.stepName,
    status: event.status,
    reason: event.reason || null,
    payload: event.payload || {}
  });
});

consumerBundle.start().catch((error) => {
  logger.error({ err: error }, 'Orchestrator consumer failed to start');
});

app.listen(port, async () => {
  logger.info({ port }, 'Orchestrator service listening');
  try {
    const recovered = await recoverPendingWorkflows();
    logger.info({ recoveredCount: recovered.length }, 'Pending workflows recovered');
  } catch (error) {
    logger.warn({ err: error }, 'Recovering pending workflows failed');
  }
});
