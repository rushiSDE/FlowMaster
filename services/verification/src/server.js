const { createApp, processVerificationCommand } = require('./app');
const logger = require('../../../shared/logger');
const { createConsumer } = require('../../../shared/kafka');

const app = createApp();
const port = Number(process.env.VERIFICATION_PORT || 3002);
const consumerBundle = createConsumer('verification-group', ['flowmaster.verification.commands'], async ({ message }) => {
  const payload = JSON.parse(message.value.toString());
  logger.info({ payload }, 'Verification command received');
  processVerificationCommand(payload);
});

consumerBundle.start().catch((error) => {
  logger.error({ err: error }, 'Verification consumer failed to start');
});

app.listen(port, () => {
  logger.info({ port }, 'Verification service listening');
});
