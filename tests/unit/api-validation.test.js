const request = require('supertest');
const { createApp } = require('../../services/orchestrator/src/app');

describe('Orchestrator API', () => {
  it('returns 400 for invalid workflow payload', async () => {
    const app = createApp();

    const response = await request(app)
      .post('/workflows')
      .send({ customerId: '', amount: -1 });

    expect(response.status).toBe(400);
    expect(response.body).toHaveProperty('error');
  });

  it('returns service health status', async () => {
    const app = createApp();

    const response = await request(app).get('/health');

    expect(response.status).toBe(200);
    expect(response.body.status).toBe('ok');
  });
});
