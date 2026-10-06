import { Controller, Get } from '@nestjs/common';
import { Public } from '../modules/auth/auth.guard';

@Controller()
export class HealthController {
  @Public()
  @Get('health')
  health(): { status: 'ok'; uptimeSec: number } {
    return { status: 'ok', uptimeSec: Math.round(process.uptime()) };
  }
}
