import {
  classifyStatus,
  describeStatus,
  parseRetryAfterMs,
} from '../../src/modules/webhooks/response-classification';

describe('classifyStatus (retry contract)', () => {
  it.each([200, 201, 202, 204, 299])('%i is SUCCESS', (status) => {
    expect(classifyStatus(status)).toBe('SUCCESS');
  });

  it.each([408, 429, 500, 502, 503, 504, 599])('%i is RETRYABLE', (status) => {
    expect(classifyStatus(status)).toBe('RETRYABLE');
  });

  it.each([300, 301, 302, 307, 308])('redirect %i is NON_RETRYABLE (never followed)', (status) => {
    expect(classifyStatus(status)).toBe('NON_RETRYABLE');
  });

  it.each([400, 401, 403, 404, 409, 410, 422])('%i is NON_RETRYABLE', (status) => {
    expect(classifyStatus(status)).toBe('NON_RETRYABLE');
  });
});

describe('describeStatus', () => {
  it('names redirects as such', () => {
    expect(describeStatus(302)).toBe('redirect');
  });

  it('keeps the bounded http_<status> shape', () => {
    expect(describeStatus(503)).toBe('http_503');
  });

  it('marks an unfinished response', () => {
    expect(describeStatus(503, 'timeout')).toBe('timeout_http_503');
    expect(describeStatus(503, 'stream_error')).toBe('stream_error_http_503');
  });
});

describe('parseRetryAfterMs', () => {
  it('converts delta-seconds to milliseconds', () => {
    expect(parseRetryAfterMs('7')).toBe(7_000);
    expect(parseRetryAfterMs(' 0 ')).toBe(0);
  });

  it.each([null, '', 'abc', '-5', '1.5', 'Wed, 21 Oct 2026 07:28:00 GMT', '99999999'])(
    'falls back to normal backoff for %p',
    (raw) => {
      expect(parseRetryAfterMs(raw)).toBeNull();
    },
  );
});
