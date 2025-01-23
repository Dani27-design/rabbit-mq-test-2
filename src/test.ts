import axios from "axios";

type Order = {
  id: string;
  marketplace: "shopee" | "tokopedia" | "lazada";
  state:
    | "UNPAID"
    | "PENDING"
    | "READY_TO_SHIP"
    | "GETTING_SHIPPING_PARAMS"
    | "SHIP_ORDER_REQUESTED"
    | "PROCESSED"
    | "RETRY_SHIP"
    | "GETTING_TRACKING_NUMBER"
    | "CREATING_SHIPPING_DOC"
    | "DOWNLOADING_SHIPPING_DOC"
    | "SHIPPED"
    | "TO_CONFIRM_RECEIVE"
    | "IN_CANCEL"
    | "CANCELLED"
    | "TO_RETURN"
    | "COMPLETED";
  createdAt: Date;
  ordersn: string;
};

const statusFlow: Order["state"][] = [
  "UNPAID",
  "PENDING",
  "READY_TO_SHIP",
  "GETTING_SHIPPING_PARAMS",
  "SHIP_ORDER_REQUESTED",
  "PROCESSED",
  "RETRY_SHIP",
  "GETTING_TRACKING_NUMBER",
  "CREATING_SHIPPING_DOC",
  "DOWNLOADING_SHIPPING_DOC",
  "SHIPPED",
  "TO_CONFIRM_RECEIVE",
  "IN_CANCEL",
  "CANCELLED",
  "TO_RETURN",
  "COMPLETED",
];

const generateNewOrder = (): Order => {
  const marketplaceRandomize = ["shopee", "tokopedia", "lazada"][
    Math.floor(Math.random() * 3)
  ] as "shopee" | "tokopedia" | "lazada";
  const ordersnRandomize = Math.random().toString(36).substring(7);
  return {
    id: `${marketplaceRandomize}_${ordersnRandomize}`,
    marketplace: marketplaceRandomize,
    state: "UNPAID",
    createdAt: new Date(),
    ordersn: ordersnRandomize,
  };
};

const sendBatchOrders = async (batchSize: number) => {
  const orders = Array(batchSize)
    .fill(null)
    .map(() => {
      const order = generateNewOrder();
      const currentIndex = statusFlow.indexOf(order.state);
      const nextIndex = currentIndex + 1;

      if (Math.random() < 0.2) {
        order.state = "CANCELLED";
      } else if (nextIndex < statusFlow.length) {
        order.state = statusFlow[nextIndex];
      }
      return order;
    });

  await Promise.all(orders.map(order => sendOrder(order)));
};

// Send 1000 requests per second
const REQUESTS_PER_SECOND = 1000;
const BATCH_INTERVAL = 1000; // 1 second in milliseconds

setInterval(() => {
  sendBatchOrders(REQUESTS_PER_SECOND);
}, BATCH_INTERVAL);


const sendOrder = async (order: Order) => {
    try {
      const response = await axios.post(
        `http://localhost:9191/webhook/${order.marketplace}`,
        order,
        {
          headers: {
            "Content-Type": "application/json",
          },
        }
      );
      console.log(`Order sent:`, response.data, "Order:", order);
    } catch (error: any) {
      console.error("Error sending order:", error.message);
    }
  };