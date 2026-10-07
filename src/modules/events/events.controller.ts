import { Body, Controller, Get, Headers, HttpCode, Inject, Param, Post } from '@nestjs/common';
import { EventsService, type PublishResult } from './events.service';
import { parsePublishEventBody } from './dto';
import { badRequest, notFound } from '../../common/errors';
import { isUuid } from '../../common/ids';
import { LOGGER } from '../../api/tokens';
import type { Logger } from '../../common/logger';
import { CurrentPrincipal, CurrentRequestId } from '../auth/decorators';
import type { Principal } from '../auth/principal';
import { tenantIdOf } from '../auth/principal';

/**
 * Event publication and status API. All routes are tenant-scoped: identity comes
 * from the authenticated principal, never from the request body.
 */
@Controller()
export class EventsController {
  constructor(
    private readonly events: EventsService,
    @Inject(LOGGER) private readonly logger: Logger,
  ) {}

  @Post('events')
  @HttpCode(202)
  async publish(
    @CurrentPrincipal() principal: Principal,
    @CurrentRequestId() requestId: string,
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
    const result = await this.events.publish(tenantIdOf(principal), input, idempotencyKey);

    // The one place the caller's requestId meets the eventId/deliveryId that the
    // workers log later, so an accepted event can be traced across processes
    // without querying the database. Identifiers and event type only: the payload
    // and the endpoint secret are never handed to the logger.
    this.logger.info(
      {
        requestId,
        eventId: result.eventId,
        deliveryId: result.deliveryId,
        endpointId: input.endpointId,
        eventType: input.eventType,
      },
      'event accepted',
    );

    return result;
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
