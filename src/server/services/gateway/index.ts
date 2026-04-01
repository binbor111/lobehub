import debug from 'debug';

import { platformRegistry } from '../bot/platforms';
import { BOT_CONNECT_QUEUE_EXPIRE_MS, BotConnectQueue } from './botConnectQueue';
import { createGatewayManager, getGatewayManager } from './GatewayManager';
import { getMessageGatewayClient } from './MessageGatewayClient';
import { BOT_RUNTIME_STATUSES, updateBotRuntimeStatus } from './runtimeStatus';

const log = debug('lobe-server:service:gateway');

const isVercel = !!process.env.VERCEL_ENV;

/** Platforms that have a real adapter in the external message-gateway. */
const MESSAGE_GATEWAY_PLATFORMS = new Set(['discord']);

export class GatewayService {
  /**
   * Check if the external message-gateway is configured.
   * When enabled, connection management is delegated to the Cloudflare Worker
   * instead of managing connections in-process.
   */
  get useMessageGateway(): boolean {
    return getMessageGatewayClient().isConfigured;
  }

  async ensureRunning(): Promise<void> {
    // When using external message-gateway, no local GatewayManager needed
    if (this.useMessageGateway) {
      log('Using external message-gateway, skipping local GatewayManager');
      return;
    }

    const existing = getGatewayManager();
    if (existing?.isRunning) {
      log('GatewayManager already running');
      return;
    }

    const manager = createGatewayManager({ definitions: platformRegistry.listPlatforms() });
    await manager.start();

    log('GatewayManager started');
  }

  async stop(): Promise<void> {
    const manager = getGatewayManager();
    if (!manager) return;

    await manager.stop();
    log('GatewayManager stopped');
  }

  async startClient(
    platform: string,
    applicationId: string,
    userId: string,
  ): Promise<'started' | 'queued'> {
    // ─── Delegate persistent platforms to external message-gateway if configured ───
    const definition = platformRegistry.getPlatform(platform);
    const isPersistent = definition?.connectionMode === 'persistent';

    if (this.useMessageGateway && isPersistent && MESSAGE_GATEWAY_PLATFORMS.has(platform)) {
      return this.startClientViaGateway(platform, applicationId, userId);
    }

    // ─── Legacy: in-process connection management ───
    if (isVercel) {
      const definition = platformRegistry.getPlatform(platform);
      const connectionMode = definition?.connectionMode || 'webhook';

      if (connectionMode === 'persistent') {
        // Persistent platforms (e.g. Discord gateway or WeChat long-polling) cannot run in a
        // serverless function — queue for the long-running cron gateway.
        const queue = new BotConnectQueue();
        await queue.push(platform, applicationId, userId);
        await updateBotRuntimeStatus(
          {
            applicationId,
            platform,
            status: BOT_RUNTIME_STATUSES.queued,
          },
          {
            ttlMs: BOT_CONNECT_QUEUE_EXPIRE_MS,
          },
        );
        log('Queued connect %s:%s', platform, applicationId);
        return 'queued';
      }

      // Webhook-based platforms only need a single HTTP call,
      // so we can run directly in a Vercel serverless function.
      const manager = createGatewayManager({ definitions: platformRegistry.listPlatforms() });
      await manager.startClient(platform, applicationId, userId);
      log('Started client %s:%s (direct)', platform, applicationId);
      return 'started';
    }

    let manager = getGatewayManager();
    if (!manager?.isRunning) {
      log('GatewayManager not running, starting automatically...');
      await this.ensureRunning();
      manager = getGatewayManager();
    }

    await manager!.startClient(platform, applicationId, userId);
    log('Started client %s:%s', platform, applicationId);
    return 'started';
  }

  async stopClient(platform: string, applicationId: string): Promise<void> {
    // ─── Delegate persistent platforms to external message-gateway if configured ───
    const definition = platformRegistry.getPlatform(platform);
    const isPersistent = definition?.connectionMode === 'persistent';

    if (this.useMessageGateway && isPersistent && MESSAGE_GATEWAY_PLATFORMS.has(platform)) {
      return this.stopClientViaGateway(platform, applicationId);
    }

    // ─── Legacy: in-process connection management ───
    if (isVercel) {
      const definition = platformRegistry.getPlatform(platform);
      const connectionMode = definition?.connectionMode || 'webhook';
      if (connectionMode === 'persistent') {
        const queue = new BotConnectQueue();
        await queue.remove(platform, applicationId);
      }
    }

    const manager = getGatewayManager();
    if (manager?.isRunning) {
      await manager.stopClient(platform, applicationId);
      log('Stopped client %s:%s', platform, applicationId);
    }

    await updateBotRuntimeStatus({
      applicationId,
      platform,
      status: BOT_RUNTIME_STATUSES.disconnected,
    });
  }

  // ─── External Message Gateway Integration ───

  /**
   * Start a connection via the external message-gateway Cloudflare Worker.
   * This offloads all persistent connection management (WebSocket, Socket.IO,
   * webhook registration) to the edge, eliminating the need for in-process
   * connection management on Vercel serverless or self-hosted instances.
   */
  private async startClientViaGateway(
    platform: string,
    applicationId: string,
    userId: string,
  ): Promise<'started'> {
    const client = getMessageGatewayClient();

    // Load credentials from DB
    const { getServerDB } = await import('@/database/core/db-adaptor');
    const { AgentBotProviderModel } = await import('@/database/models/agentBotProvider');
    const { KeyVaultsGateKeeper } = await import('@/server/modules/KeyVaultsEncrypt');

    const serverDB = await getServerDB();
    const gateKeeper = await KeyVaultsGateKeeper.initWithEnvKey();
    const model = new AgentBotProviderModel(serverDB, userId, gateKeeper);
    const provider = await model.findEnabledByApplicationId(platform, applicationId);

    if (!provider) {
      log('No enabled provider found for %s:%s', platform, applicationId);
      throw new Error(`No enabled provider found for ${platform}:${applicationId}`);
    }

    const appUrl = process.env.APP_URL || '';
    const webhookPath = `/api/agent/webhooks/${platform}/${applicationId}`;

    await client.connect({
      connectionId: provider.id,
      credentials: provider.credentials,
      platform,
      userId,
      webhookPath,
    });

    await updateBotRuntimeStatus({
      applicationId,
      platform,
      status: BOT_RUNTIME_STATUSES.connected,
    });

    log('Started client via message-gateway %s:%s', platform, applicationId);
    return 'started';
  }

  private async stopClientViaGateway(platform: string, applicationId: string): Promise<void> {
    const client = getMessageGatewayClient();

    // We need the provider ID (connectionId) to disconnect
    // Look up by platform + applicationId
    const { getServerDB } = await import('@/database/core/db-adaptor');
    const { AgentBotProviderModel } = await import('@/database/models/agentBotProvider');

    const serverDB = await getServerDB();
    const provider = await AgentBotProviderModel.findByPlatformAndAppId(
      serverDB,
      platform,
      applicationId,
    );

    if (provider) {
      try {
        await client.disconnect(provider.id);
      } catch (err) {
        log('Disconnect via message-gateway failed: %O', err);
      }
    }

    await updateBotRuntimeStatus({
      applicationId,
      platform,
      status: BOT_RUNTIME_STATUSES.disconnected,
    });

    log('Stopped client via message-gateway %s:%s', platform, applicationId);
  }
}
