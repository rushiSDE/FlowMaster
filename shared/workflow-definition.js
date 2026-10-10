const fs = require('fs');
const path = require('path');
const crypto = require('crypto');
const YAML = require('yaml');
const Joi = require('joi');

const stepSchema = Joi.object({
  name: Joi.string().required(),
  service: Joi.string().required(),
  event: Joi.string().required(),
  timeoutMs: Joi.number().integer().min(100).default(3000),
  retries: Joi.number().integer().min(0).default(2),
  compensation: Joi.object({
    service: Joi.string().required(),
    event: Joi.string().required()
  }).optional()
});

const workflowSchema = Joi.object({
  name: Joi.string().required(),
  version: Joi.string().required(),
  steps: Joi.array().min(1).items(stepSchema).required()
}).required();

function validateWorkflowDefinition(definition) {
  const { error, value } = workflowSchema.validate(definition, { abortEarly: false });

  if (error) {
    const details = error.details.map((d) => d.message).join('; ');
    throw new Error(`Invalid workflow definition: ${details}`);
  }

  return value;
}

function loadWorkflowDefinition(filePath) {
  const resolvedPath = path.resolve(process.cwd(), filePath);

  if (!fs.existsSync(resolvedPath)) {
    throw new Error(`Workflow definition not found: ${resolvedPath}`);
  }

  const raw = fs.readFileSync(resolvedPath, 'utf8');
  const parsed = YAML.parse(raw);

  return validateWorkflowDefinition(parsed);
}

function createSagaState(definition, context = {}) {
  const workflowId = `wf_${Date.now()}_${crypto.randomBytes(4).toString('hex')}`;

  return {
    workflowId,
    workflowName: definition.name,
    version: definition.version,
    status: 'pending',
    currentStep: 0,
    customerId: context.customerId || 'anonymous',
    amount: context.amount || 0,
    startedAt: new Date().toISOString(),
    updatedAt: new Date().toISOString(),
    steps: definition.steps.map((step, index) => ({
      index,
      name: step.name,
      service: step.service,
      event: step.event,
      status: 'pending',
      retries: 0,
      timeoutMs: step.timeoutMs,
      compensation: step.compensation || null
    }))
  };
}

module.exports = {
  validateWorkflowDefinition,
  loadWorkflowDefinition,
  createSagaState
};
