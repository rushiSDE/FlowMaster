const { createApp, processCreditCommand } = require('./app');
const logger = require('../../../shared/logger');
const { createConsumer } = require('../../../shared/kafka');

const app = createApp();
const port = Number(process.env.CREDIT_PORT || 3003);
const consumerBundle = createConsumer('credit-group', ['flowmaster.credit.commands'], async ({ message }) => {
  const payload = JSON.parse(message.value.toString());
  logger.info({ payload }, 'Credit command received');
  processCreditCommand(payload);
});

consumerBundle.start().catch((error) => {
  logger.error({ err: error }, 'Credit consumer failed to start');
});

app.listen(port, () => {
  logger.info({ port }, 'Credit service listening');
});
