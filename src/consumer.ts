import amqp from 'amqplib';
import { config } from './config';
import { Order } from './types';

class OrderConsumer {
  private channel: amqp.Channel | null = null;
  private consumerId: string;

  constructor(consumerId: string) {
    this.consumerId = consumerId;
  }

  async connect() {
    try {
      const connection = await amqp.connect(config.amqpUrl);
      this.channel = await connection.createChannel();
      
      // Enable prefetch untuk load balancing
      await this.channel.prefetch(1);
      
      await this.channel.assertExchange(config.exchange, 'direct', { durable: true });
      await this.channel.assertQueue(config.queue, { durable: true });
      
      console.log(`Consumer ${this.consumerId} connected to RabbitMQ`);
      
      // Setup consumer
      await this.consume();
    } catch (error) {
      console.error(`Consumer ${this.consumerId} error connecting:`, error);
      // Reconnect after 5 seconds
      setTimeout(() => this.connect(), 5000);
    }
  }

  private async consume() {
    if (!this.channel) {
      throw new Error('Channel not established');
    }

    try {
      await this.channel.consume(config.queue, async (msg) => {
        if (!msg) return;

        try {
          const order: Order = JSON.parse(msg.content.toString());
          console.log(`Consumer ${this.consumerId} processing order: ${order.marketplace}_${order.id}_${order.status}`);
          
          // Simulasi processing
          await this.processOrder(order);
          
          // Acknowledge message
          this.channel?.ack(msg);
          
          console.log(`Consumer ${this.consumerId} completed order: ${order.marketplace}_${order.id}_${order.status}`);
        } catch (error) {
          console.error(`Consumer ${this.consumerId} processing error:`, error);
          // Reject message dan requeue
          this.channel?.reject(msg, true);
        }
      });
    } catch (error) {
      console.error(`Consumer ${this.consumerId} consume error:`, error);
      // Reconnect on error
      setTimeout(() => this.connect(), 5000);
    }
  }

  private async processOrder(order: Order): Promise<void> {
    // Simulasi processing time 1-3 detik
    const processingTime = Math.random() * 2000 + 1000;
    await new Promise(resolve => setTimeout(resolve, processingTime));
    
    // Simulasi random error (10% chance)
    if (Math.random() < 0.1) {
      throw new Error('Random processing error');
    }
  }
}

export { OrderConsumer };
