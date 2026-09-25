import http from 'k6/http';
import { check } from 'k6';

export const options = {
  scenarios: {
    default: {
      executor: 'ramping-vus',
      startVUs: 1,
      stages: [
        { duration: '10s', target: 300 }, // Ramp up
        { duration: '20s', target: 300 }, // Hold
        { duration: '10s', target: 0 },   // Ramp down
      ],
    },
  },
  thresholds: {
    http_req_duration: ['p(99)<25'], // Native might have slight variance
  },
};

const numNodes = __ENV.NUM_NODES ? parseInt(__ENV.NUM_NODES) : 1;

export default function () {
  // Simulate an ideal load balancer by picking a random native port
  // Nodes are running on 8081, 8082, 8083, etc.
  const targetPort = 8080 + Math.floor(Math.random() * numNodes) + 1;
  const url = `http://127.0.0.1:${targetPort}/v1/check`;
  
  const userIP = `native_user_${Math.floor(Math.random() * 500000)}`;

  const payload = JSON.stringify([
    {
      key: userIP,
      algorithm: 'SLIDING_WINDOW_LOG',
      limit: 100,
      window: 60,
    }
  ]);

  const params = {
    headers: { 'Content-Type': 'application/json' },
  };

  const res = http.post(url, payload, params);

  check(res, {
    'status is 200 or 429': (r) => r.status === 200 || r.status === 429,
  });
}
