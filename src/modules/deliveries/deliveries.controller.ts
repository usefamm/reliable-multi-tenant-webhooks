import { Controller, Get, Query } from '@nestjs/common';
import { DeliveriesService } from './deliveries.service';
import { parseDeliveryQuery } from './dto';
import { CurrentPrincipal } from '../auth/decorators';
import type { Principal } from '../auth/principal';
import { tenantIdOf } from '../auth/principal';

/**
 * GET /deliveries - tenant-scoped, paginated, state-filterable delivery listing.
 * Identity comes from the authenticated principal; the query string only carries
 * pagination/filter options, never a tenantId.
 */
@Controller('deliveries')
export class DeliveriesController {
  constructor(private readonly deliveries: DeliveriesService) {}

  @Get()
  async list(
    @CurrentPrincipal() principal: Principal,
    @Query() query: Record<string, unknown>,
  ) {
    const parsed = parseDeliveryQuery(query);
    return this.deliveries.list(tenantIdOf(principal), parsed);
  }
}
