import { Body, Controller, Get, Headers, HttpCode, Param, Post } from '@nestjs/common';
import { EventsService, type PublishResult } from './events.service';
import { parsePublishEventBody } from './dto';
import { badRequest, notFound } from '../../common/errors';
import { isUuid } from '../../common/ids';
import { CurrentPrincipal } from '../auth/decorators';
import type { Principal } from '../auth/principal';
import { tenantIdOf } from '../auth/principal';

/**
 * Event publication and status API. All routes are tenant-scoped: identity comes
 * from the authenticated principal, never from the request body.
 */
@Controller()
export class EventsController {
  constructor(private readonly events: EventsService) {}

  @Post('events')
  @HttpCode(202)
  async publish(
    @CurrentPrincipal() principal: Principal,
    @Body() body: unknown,
    @Headers('idempotency-key') idempotencyKey: string | undefined,
  ): Promise<PublishResult> {
    // Idempotency-Key is required: publication dedup is a core guarantee.
    if (!idempotencyKey || idempotencyKey.trim().length === 0) {
      throw badRequest('Idempotency-Key header is required');
    }
    if (idempotencyKey.length > 200) {
      throw badRequest('Idempotency-Key header is too long (max 200 characters)');
    }
    const input = parsePublishEventBody(body);
    return this.events.publish(tenantIdOf(principal), input, idempotencyKey);
  }

  @Get('events/:id')
  async getEvent(@CurrentPrincipal() principal: Principal, @Param('id') id: string) {
    if (!isUuid(id)) {
      // A malformed id can never belong to the tenant: 404 without leaking.
      throw notFound('Event not found');
    }
    return this.events.getEvent(tenantIdOf(principal), id);
  }
}
