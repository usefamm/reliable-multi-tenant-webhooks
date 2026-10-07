import { z } from 'zod';
import { badRequest } from '../../common/errors';

/**
 * Validation for POST /ops/deliveries/:id/redrive.
 * `reason` is mandatory: a redrive is a privileged operator action that spends
 * fresh delivery effort, so it must be attributable and explainable in the audit
 * trail. The length bound keeps the audit row and logs sane.
 */
export const RedriveBodySchema = z
  .object({
    reason: z
      .string()
      .min(1, 'reason must be a non-empty string')
      .max(500, 'reason must be at most 500 characters')
      // "   " is technically non-empty but is not an explainable action.
      .regex(/\S/, 'reason must contain a non-whitespace character')
      .transform((value) => value.trim()),
  })
  .strict();

export type RedriveInput = z.infer<typeof RedriveBodySchema>;

export function parseRedriveBody(body: unknown): RedriveInput {
  const result = RedriveBodySchema.safeParse(body);
  if (!result.success) {
    const first = result.error.issues[0];
    const path = first?.path.join('.') || '(body)';
    throw badRequest(`Invalid request body: ${path}: ${first?.message ?? 'invalid'}`);
  }
  return result.data;
}
