import dotenv from 'dotenv';
dotenv.config();

export const config = {
  amqpUrl: process.env.AMQP_URL || '',
  exchange: 'orders.exchange',
  queue: 'orders.processing',
};