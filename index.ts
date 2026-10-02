/**
 * ============================================================================
 * PLATFORM  : chatpak-ar Engine
 * DESKRIPSI : Single-File Enterprise Conversational Marketing Engine
 * ARCHITECTURE: Event-Driven, Microservices Ready, High-Concurrency Queue
 * ============================================================================
 * 
 * 📐 ARCHITECTURE CANVAS (Dapat dirender otomatis di GitHub / Mermaid Canvas):
 * 
 * ```mermaid
 * graph LR
 *     IG[Instagram API / Webhook] -->|HTTP POST| API[Express API Server]
 *     API -->|Enqueue Event <10ms| Q[(Redis / BullMQ Queue)]
 *     Q -->|Process Job| Worker[Flow Engine Worker]
 *     Worker <-->|Get/Set State| State[(Redis State Cache)]
 *     Worker -->|Send Direct Message| MetaAPI[Meta Graph API Messaging]
 * ```
 */

import express, { Request, Response } from 'express';
import dotenv from 'dotenv';
import axios from 'axios';
import Redis from 'ioredis';
import { Queue, Worker, Job } from 'bullmq';

dotenv.config();

// ============================================================================
// 1. DATA TYPES & INTERFACES
// ============================================================================

export interface WebhookEventPayload {
  eventId: string;
  platform: 'instagram' | 'whatsapp';
  eventType: 'COMMENT' | 'DIRECT_MESSAGE';
  senderId: string;
  recipientId: string;
  text: string;
  timestamp: number;
}

export interface UserState {
  userId: string;
  currentStep: string | null;
  contextData: Record<string, any>;
}

// ============================================================================
// 2. CONFIGURATION & DATABASE CONNECTIONS
// ============================================================================

const PORT = process.env.PORT || 3000;
const REDIS_HOST = process.env.REDIS_HOST || 'localhost';
const REDIS_PORT = parseInt(process.env.REDIS_PORT || '6379', 10);
const VERIFY_TOKEN = process.env.META_WEBHOOK_VERIFY_TOKEN || 'chatpakar_secret';
const META_PAGE_ACCESS_TOKEN = process.env.META_PAGE_ACCESS_TOKEN || 'MOCK_TOKEN';

const redisConnection = { host: REDIS_HOST, port: REDIS_PORT };
const redisClient = new Redis(redisConnection);

// ============================================================================
// 3. SERVICE LAYER (META GRAPH API & FLOW LOGIC ENGINE)
// ============================================================================

class InstagramApiService {
  private graphApiUrl = 'https://graph.facebook.com/v19.0';

  async sendDirectMessage(recipientId: string, message: string, accessToken: string): Promise<boolean> {
    try {
      const response = await axios.post(
        `${this.graphApiUrl}/me/messages`,
        {
          recipient: { id: recipientId },
          message: { text: message }
        },
        {
          params: { access_token: accessToken }
        }
      );
      return response.status === 200;
    } catch (error: any) {
      console.error('[Meta Graph API Error]:', error.response?.data || error.message);
      return false;
    }
  }
}

class FlowEngineService {
  private instagramApi: InstagramApiService;

  constructor() {
    this.instagramApi = new InstagramApiService();
  }

  async processEvent(event: WebhookEventPayload): Promise<void> {
    const stateKey = `state:${event.platform}:${event.senderId}`;
    console.log(`[FlowEngine] Evaluating text: "${event.text}" from User: ${event.senderId}`);

    const keywordMatch = event.text.trim().toUpperCase();

    // Logic Trigger: Pemicu Otomatisasi Kata Kunci "PROMO" / "MAU"
    if (keywordMatch === 'PROMO' || keywordMatch === 'MAU') {
      const replyMessage = `Halo! Terima kasih telah menghubungi chatpak-ar.\n\nGunakan kode kupon "CHATPAK100" untuk diskon 100%!`;

      const sent = await this.instagramApi.sendDirectMessage(
        event.senderId,
        replyMessage,
        META_PAGE_ACCESS_TOKEN
      );

      if (sent) {
        await redisClient.set(
          stateKey,
          JSON.stringify({ currentStep: 'AWAITING_EMAIL', timestamp: Date.now() })
        );
        console.log(`[FlowEngine] DM successfully dispatched to ${event.senderId}`);
      }
    } else {
      console.log(`[FlowEngine] Text does not match trigger criteria.`);
    }
  }
}

// ============================================================================
// 4. QUEUE & ASYNCHRONOUS WORKER SYSTEM (BULLMQ)
// ============================================================================

export const eventQueue = new Queue<WebhookEventPayload>('inbound-events', { connection: redisConnection });
const flowEngine = new FlowEngineService();

export const eventWorker = new Worker<WebhookEventPayload>(
  'inbound-events',
  async (job: Job<WebhookEventPayload>) => {
    console.log(`[Queue Worker] Processing Event ID: ${job.data.eventId}`);
    await flowEngine.processEvent(job.data);
  },
  { connection: redisConnection }
);

eventWorker.on('failed', (job, err) => {
  console.error(`[Queue Worker Error] Job ${job?.id} failed:`, err.message);
});

// ============================================================================
// 5. SERVER HTTP & API ENDPOINTS (EXPRESS)
// ============================================================================

const app = express();
app.use(express.json());

// Healthcheck Endpoint
app.get('/health', (req: Request, res: Response) => {
  res.status(200).json({ status: 'OK', system: 'chatpak-ar Engine', timestamp: new Date() });
});

// Meta Webhook Verification Challenge Endpoint
app.get('/webhook/instagram', (req: Request, res: Response) => {
  const mode = req.query['hub.mode'];
  const token = req.query['hub.verify_token'];
  const challenge = req.query['hub.challenge'];

  if (mode === 'subscribe' && token === VERIFY_TOKEN) {
    console.log('[Webhook Verification] Success!');
    return res.status(200).send(challenge);
  }
  return res.sendStatus(403);
});

// Ingest Webhook (Inbound Event Receiver - Repons Cepat < 10ms)
app.post('/webhook/instagram', async (req: Request, res: Response) => {
  try {
    const body = req.body;

    if (body.object === 'instagram') {
      for (const entry of body.entry) {
        for (const messagingEvent of entry.messaging || []) {
          const payload: WebhookEventPayload = {
            eventId: `evt_${Date.now()}_${Math.random().toString(36).substring(2, 7)}`,
            platform: 'instagram',
            eventType: 'DIRECT_MESSAGE',
            senderId: messagingEvent.sender.id,
            recipientId: messagingEvent.recipient.id,
            text: messagingEvent.message?.text || '',
            timestamp: messagingEvent.timestamp
          };

          // Masukkan payload ke antrean secara asinkron (non-blocking)
          await eventQueue.add('process-ig-event', payload);
        }
      }
    }

    return res.status(200).send('EVENT_RECEIVED');
  } catch (error) {
    console.error('[Webhook Ingestion Error]:', error);
    return res.sendStatus(500);
  }
});

// Start Application Server
app.listen(PORT, () => {
  console.log(`🚀 chatpak-ar Engine running on port ${PORT}`);
});

