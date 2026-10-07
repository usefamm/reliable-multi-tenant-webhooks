import { Controller, Get } from '@nestjs/common';
import { OperatorOnly } from '../auth/decorators';
import { StatusService, type OpsStatus } from './status.service';

/**
 * Operator-only operational view. Deliberately not tenant-scoped: these counters
 * describe the shared queue, and a tenant must not infer another tenant's volume
 * from them. Tenant-scoped reads live on /deliveries.
 */
@Controller('ops')
export class StatusController {
  constructor(private readonly statusService: StatusService) {}

  @Get('status')
  @OperatorOnly()
  getStatus(): Promise<OpsStatus> {
    return this.statusService.snapshot();
  }
}
