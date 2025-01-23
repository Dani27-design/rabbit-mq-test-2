import dotenv from 'dotenv';
dotenv.config();

export const config = {
  amqpUrl: process.env.AMQP_URL || '',
  exchange: 'orders.exchange',
  queue: 'orders.processing',
  dlx: {
    exchange: 'orders.dlx.exchange',
    queue: 'orders.dlx.queue',
    messageTTL: 60000, // 60 seconds
  }
};