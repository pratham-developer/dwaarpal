import http from 'k6/http';
import { check, sleep } from 'k6';

// Soak Test Configuration: Flat 500 RPS for 1 hour
export const options = {
  scenarios: {
    constant_request_rate: {
      executor: 'constant-arrival-rate',
      rate: 500, // 500 requests per second
      timeUnit: '1s', // per second
      duration: '1h', // 1 hour soak test
      preAllocatedVUs: 100, // pool of VUs to sustain the rate
      maxVUs: 500, // burst max VUs if latency spikes
    },
  },
  thresholds: {
    http_req_failed: ['rate<0.01'], // less than 1% errors
  },
};

export default function () {
  const url = 'http://localhost/v1/check'; // Assuming Nginx routing
  
  // High variance in IPs to simulate massive unique visitors over the hour
  const userIP = `soak_user_${Math.floor(Math.random() * 500000)}`;

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
