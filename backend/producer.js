const { Kafka } = require("kafkajs");
const stompit = require("stompit");
const async = require("async");
const dotenv = require("dotenv");

// Load environment variables
dotenv.config();

// Configuration for the Kafka brokers
const kafkaConfig = new Kafka({
  brokers: ["localhost:9092"],
});

// Create Kafka producer
const kafkaProducer = new Kafka({
  clientId: "rail_app_producer",
  ...kafkaConfig,
}).producer();

const initKafkaProducer = async () => {
  try {
    await kafkaProducer.connect();
    console.log("Producer connected successfully");
  } catch (error) {
    console.error("Error connecting Kafka producer:", error.message);
    process.exit(1);
  }
};

// Initialize Kafka producer
initKafkaProducer();

// Stompit connection manager
const connectOptions = {
  host: process.env.OPEN_DATA_FEED_HOST,
  port: 61618,
  connectHeaders: {
    "heart-beat": "15000,15000",
    "client-id": "",
    host: "/",
    login: process.env.OPEN_DATA_FEED_USERNAME,
    passcode: process.env.OPEN_DATA_FEED_PASSWORD,
  },
};

const reconnectOptions = {
  initialReconnectDelay: 10,
  maxReconnectDelay: 30000,
  useExponentialBackOff: true,
  maxReconnects: 30,
  randomize: false,
};

const connectionManager = new stompit.ConnectFailover(
  [connectOptions],
  reconnectOptions
);

connectionManager.connect((error, client, reconnect) => {
  if (error) {
    console.error("Terminal error gave up reconnecting:", error.message);
    return;
  }

  client.on("error", (error) => {
    console.error("Connection lost. Reconnecting...", error.message);
    reconnect();
  });

  const headers = {
    destination: "/topic/TRAIN_MVT_ALL_TOC",
    "activemq.subscriptionName": "somename-train_mvt",
    ack: "client-individual",
  };

  client.subscribe(headers, (error, message) => {
    if (error) {
      console.error("Subscription failed:", error.message);
      return;
    }

    message.readString("utf-8", async (error, body) => {
      if (error) {
        console.error("Failed to read a message", error.message);
        return;
      }

      if (body) {
        try {
          const data = JSON.parse(body);
          async.each(data, async (item) => {
            const timestamp = new Date().toISOString();
            if (item.header) {
              if (item.header.msg_type === "0001") {
                //   Train activation
                const stanox =
                  item.body.tp_origin_stanox ||
                  item.body.sched_origin_stnaox ||
                  "N/A";
                console.log(
                  timestamp,
                  "- Train",
                  item.body.train_id,
                  "activated at stanox",
                  stanox
                );

                //   Send message to Kafka
                await sendToKafka("train_activation", {
                  timestamp,
                  trainId: item.body.train_id,
                  stanox,
                });
              } else if (item.header.msg_type === "0002") {
                //   Train cancellation
                const stanox = item.body.loc_stanox || "N/A";
                const reasonCode = item.body.canx_reason_code || "N/A";
                console.log(
                  timestamp,
                  "- Train",
                  item.body.train_id,
                  "cancelled, Cancellation reason: ",
                  reasonCode,
                  "at stanox",
                  stanox
                );
                //   Send message to Kafka
                await sendToKafka("train_cancellation", {
                  timestamp,
                  trainId: item.body.train_id,
                  stanox,
                  reasonCode,
                });
              }
            }
          });
        } catch (error) {
          console.error("Failed to parse JSON", error.message);
        }
      }
      client.ack(message);
    });
  });
});

// Add a log statemeent inside sendToKafka function to confirm message are being sent
async function sendToKafka(topic, message) {
  try {
    await kafkaProducer.send({
      topic,
      messages: [{ value: JSON.stringify(message) }],
    });
    console.log(`Message sent to Kafka topic ${topic}:`, message);
  } catch (error) {
    console.error("Error sending message to Kafka:", error.message);
  }
}
