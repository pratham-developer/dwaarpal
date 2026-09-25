import http from 'k6/http';
import { check, sleep } from 'k6';

// k6 Load Test Configuration
export const options = {
  stages: [
    { duration: '10s', target: 50 },  // Ramp-up to 50 virtual users
    { duration: '20s', target: 300 }, // Ramp-up to 300 virtual users (Spike)
    { duration: '10s', target: 0 },   // Ramp-down
  ],
  thresholds: {
    // 99% of requests must finish within 15ms
    http_req_duration: ['p(99)<15'],
  },
};

export default function () {
  // Hitting the Nginx Load Balancer on port 80 by default, or an override
  const url = __ENV.TARGET_URL ? __ENV.TARGET_URL : 'http://localhost/v1/check';
  
  // Read target hit rate from env (default 0% if not specified)
  const targetHitRate = __ENV.HIT_RATE ? parseInt(__ENV.HIT_RATE) : 0;
  
  let userIP;
  // Determine if this request should be an intentional L1 Hit or Miss
  if (Math.random() * 100 < targetHitRate) {
      // Intentional Hit: Re-use the same hot IP
      userIP = `hot_ip_user`;
  } else {
      // Intentional Miss: Generate a completely unique IP
      userIP = `unique_ip_${__VU}_${__ITER}_${Math.floor(Math.random() * 1000000)}`;
  }

  const payload = JSON.stringify([
    {
      key: userIP,
      algorithm: 'TOKEN_BUCKET',
      limit: 100,
      window: 60,
    }
  ]);

  const params = {
    headers: {
      'Content-Type': 'application/json',
    },
  };

  const res = http.post(url, payload, params);

  // Validate the response
  check(res, {
    'status is 200 or 429': (r) => r.status === 200 || r.status === 429,
  });

  // Tiny sleep to prevent local socket exhaustion during tests
  sleep(0.01);
}
