const { Kafka } = require('kafkajs');
const logger = require('./logger');

function getKafkaConfig() {
  const brokers = (process.env.KAFKA_BROKERS || 'localhost:9092').split(',').map((entry) => entry.trim()).filter(Boolean);

  return {
    clientId: process.env.KAFKA_CLIENT_ID || 'flowmaster',
    brokers
  };
}

function createProducer() {
  const config = getKafkaConfig();
  const kafka = new Kafka(config);
  return kafka.producer();
}

function createConsumer(groupId, topics, eachMessageHandler) {
  const kafka = new Kafka(getKafkaConfig());
  const consumer = kafka.consumer({ groupId });

  return {
    consumer,
    topics,
    eachMessageHandler,
    async start() {
      await consumer.connect();
      await Promise.all(topics.map((topic) => consumer.subscribe({ topic, fromBeginning: false })));
      await consumer.run({
        eachMessage: eachMessageHandler
      });
      return consumer;
    }
  };
}

async function publishEvent({ topic, key, payload, correlationId }) {
  const producer = createProducer();

  try {
    await producer.connect();
    await producer.send({
      topic,
      messages: [
        {
          key: key || 'workflow-event',
          value: JSON.stringify({
            correlationId,
            timestamp: new Date().toISOString(),
            payload
          })
        }
      ]
    });

    logger.info({ topic, key, correlationId }, 'Kafka event published');
    return { ok: true };
  } catch (error) {
    logger.warn({ err: error, topic, key, correlationId }, 'Kafka publish failed; event kept in process memory');
    return { ok: false, error: error.message };
  } finally {
    await producer.disconnect().catch(() => undefined);
  }
}

module.exports = {
  getKafkaConfig,
  createProducer,
  createConsumer,
  publishEvent
};
