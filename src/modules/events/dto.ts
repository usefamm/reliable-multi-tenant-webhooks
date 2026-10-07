import { z } from 'zod';
import { isUuid } from '../../common/ids';

/**
 * Validation for POST /events (PDF contract).
 *  - endpointId: a UUID identifying a preconfigured endpoint owned by the caller.
 *  - eventType: non-empty string, <= 100 characters.
 *  - payload: a JSON object (not an array/scalar).
 * The raw body size (<= 64 KiB) is enforced by the express JSON parser (-> 413)
 * before this schema runs, so a too-large body never reaches validation.
 */
export const PublishEventSchema = z
  .object({
    endpointId: z
      .string()
      .refine(isUuid, { message: 'endpointId must be a valid UUID' }),
    eventType: z
      .string()
      .min(1, 'eventType must be a non-empty string')
      .max(100, 'eventType must be at most 100 characters'),
    payload: z
      .object({})
      .passthrough()
      .refine((v) => !Array.isArray(v), { message: 'payload must be a JSON object' }),
  })
  // The top-level contract is closed: an unrecognised field is a mistake, not
  // something to ignore. Silently dropping a caller's `url` or `secret` would
  // let them believe they had steered the destination; rejecting it says the
  // destination comes from the endpoints table and nowhere else. `payload`
  // stays open because customer business data is arbitrary by definition.
  .strict();

export type PublishEventInput = z.infer<typeof PublishEventSchema>;

/** Parse and validate the request body, throwing a 400 HttpError on failure. */
import { badRequest } from '../../common/errors';
export function parsePublishEventBody(body: unknown): PublishEventInput {
  const result = PublishEventSchema.safeParse(body);
  if (!result.success) {
    const first = result.error.issues[0];
    const path = first?.path.join('.') || '(body)';
    throw badRequest(`Invalid request body: ${path}: ${first?.message ?? 'invalid'}`);
  }
  return result.data;
}
