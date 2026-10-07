import { Body, Controller, Headers, HttpCode, Param, Post } from '@nestjs/common';
import { badRequest, notFound } from '../../common/errors';
import { isUuid } from '../../common/ids';
import { CurrentPrincipal, OperatorOnly } from '../auth/decorators';
import type { Principal } from '../auth/principal';
import { RedriveService, type RedriveResult } from './redrive.service';
import { parseRedriveBody } from './dto';

/**
 * Operator surface. Everything here requires the operator token: a tenant token
 * gets 403 (the guard enforces this), and an operator token cannot reach the
 * tenant-scoped routes, so the two privilege levels never mix.
 */
@Controller('ops/deliveries')
export class OperationsController {
  constructor(private readonly redrives: RedriveService) {}

  @Post(':id/redrive')
  @HttpCode(202)
  @OperatorOnly()
  async redrive(
    @CurrentPrincipal() principal: Principal,
    @Param('id') id: string,
    @Body() body: unknown,
    @Headers('idempotency-key') idempotencyKey: string | undefined,
  ): Promise<RedriveResult> {
    // A malformed id can never name a delivery: same 404 as an unknown one.
    if (!isUuid(id)) {
      throw notFound('Delivery not found');
    }
    if (!idempotencyKey || idempotencyKey.trim().length === 0) {
      throw badRequest('Idempotency-Key header is required');
    }
    if (idempotencyKey.length > 200) {
      throw badRequest('Idempotency-Key header is too long (max 200 characters)');
    }
    const { reason } = parseRedriveBody(body);

    // Audit records the operator label, never the raw bearer token.
    return this.redrives.redrive({
      deliveryId: id,
      reason,
      idempotencyKey,
      operator: principal.label,
    });
  }
}
