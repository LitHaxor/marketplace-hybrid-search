// Load test: searches and new car ads at the same time, at a fixed rate.
//
//   k6 run -e STAGE=stage1 -e SEARCHES=1000 -e INGESTS=500 -e DURATION=5m loadtest/search-and-ingest.js
//
// "constant-arrival-rate" = an open model: new requests start on schedule whether or not earlier ones
// have finished, like real users. If the API slows down, requests pile up (more VUs in flight); if k6 runs
// out of VUs, it counts "dropped_iterations" instead of quietly sending less. Both show up in the report.
import http from 'k6/http';
import { check } from 'k6';

const API = __ENV.API_URL || 'http://localhost:8080';
const STAGE = __ENV.STAGE || 'adhoc';
const DURATION = __ENV.DURATION || '5m';
const SEARCHES = Number(__ENV.SEARCHES || 1000); // total searches in DURATION
const INGESTS = Number(__ENV.INGESTS || 500); // total new ads in DURATION
const REPORT_DIR = __ENV.REPORT_DIR || 'loadtest/reports';

const queries = JSON.parse(open('./queries.json'));
const seconds = parseDuration(DURATION);

export const options = {
  discardResponseBodies: true, // we measure the API, not k6 parsing JSON
  summaryTrendStats: ['avg', 'med', 'p(90)', 'p(95)', 'p(99)', 'max'],
  scenarios: {
    search: {
      executor: 'constant-arrival-rate',
      exec: 'search',
      rate: SEARCHES,
      timeUnit: DURATION, // SEARCHES spread evenly over DURATION
      duration: DURATION,
      preAllocatedVUs: vusFor(SEARCHES / seconds, 0.2),
      maxVUs: Math.min(400, vusFor(SEARCHES / seconds, 2.0)), // capped: past this, k6 drops iterations
    },
    ingest: {
      executor: 'constant-arrival-rate',
      exec: 'ingest',
      rate: INGESTS,
      timeUnit: DURATION,
      duration: DURATION,
      preAllocatedVUs: vusFor(INGESTS / seconds, 0.1),
      maxVUs: Math.min(200, vusFor(INGESTS / seconds, 2.0)),
    },
  },
  // Targets (the report marks them pass/fail; the test always runs to the end)
  thresholds: {
    'http_req_duration{scenario:search}': ['p(95)<500', 'p(99)<1000'],
    'http_req_duration{scenario:ingest}': ['p(95)<300'],
    'http_req_failed{scenario:search}': ['rate<0.01'],
    'http_req_failed{scenario:ingest}': ['rate<0.01'],
  },
};

// Little's law: requests in flight = rate × time each one takes
function vusFor(perSecond, assumedSeconds) {
  return Math.max(2, Math.ceil(perSecond * assumedSeconds));
}

export function search() {
  const params = { q: pick(Math.random() < 0.6 ? queries.exact : queries.intents) };
  if (Math.random() < 0.3) params.maxPrice = pick([5000, 10000, 15000, 20000, 30000]);
  if (Math.random() < 0.2) params.minYear = pick([2008, 2012, 2015, 2018]);
  const query = Object.entries(params).map(([k, v]) => `${k}=${encodeURIComponent(v)}`).join('&');
  const response = http.get(`${API}/search?${query}`, { tags: { name: 'GET /search' } });
  check(response, { 'search 200': (r) => r.status === 200 });
}

export function ingest() {
  const make = pick(['ford', 'toyota', 'chevrolet', 'honda', 'nissan', 'jeep', 'ram', 'bmw', 'subaru', 'volkswagen']);
  const body = JSON.stringify({
    price: 1000 + Math.floor(Math.random() * 50000),
    year: 2000 + Math.floor(Math.random() * 23),
    manufacturer: make,
    model: pick(['base', 'sport', 'limited', 'touring', 'premium', 'lx', 'ex', 'se']),
    description: `Load test ad. ${pick(['One owner', 'Clean title', 'New tires', 'Low miles', 'Garage kept'])}, ${pick(['runs great', 'well maintained', 'no accidents', 'service records available'])}.`,
    lat: 30 + Math.random() * 15,
    lon: -120 + Math.random() * 45,
  });
  const response = http.post(`${API}/vehicles`, body, {
    headers: { 'Content-Type': 'application/json' },
    tags: { name: 'POST /vehicles' },
  });
  check(response, { 'ingest 201': (r) => r.status === 201 });
}

// The whole result as JSON (for the report), plus a short text summary on screen
export function handleSummary(data) {
  const m = data.metrics;
  const line = (scenario) => {
    const d = m[`http_req_duration{scenario:${scenario}}`] || m.http_req_duration;
    const f = m[`http_req_failed{scenario:${scenario}}`];
    return `${scenario.padEnd(7)} p50 ${ms(d, 'med')}  p95 ${ms(d, 'p(95)')}  p99 ${ms(d, 'p(99)')}  failed ${f ? (f.values.rate * 100).toFixed(2) : '?'}%`;
  };
  const dropped = m.dropped_iterations ? m.dropped_iterations.values.count : 0;
  return {
    [`${REPORT_DIR}/${STAGE}.json`]: JSON.stringify(data, null, 1),
    stdout: `\n[${STAGE}] ${SEARCHES} searches + ${INGESTS} ingests in ${DURATION}\n  ${line('search')}\n  ${line('ingest')}\n  dropped iterations (k6 ran out of VUs): ${dropped}\n`,
  };
}

function ms(trend, stat) {
  return trend && trend.values[stat] !== undefined ? `${trend.values[stat].toFixed(0)}ms` : '?';
}

function pick(items) {
  return items[Math.floor(Math.random() * items.length)];
}

function parseDuration(text) {
  const match = /^(\d+)(s|m|h)$/.exec(text);
  return Number(match[1]) * { s: 1, m: 60, h: 3600 }[match[2]];
}
