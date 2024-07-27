const { Kafka } = require("kafkajs");

exports.kafka = new Kafka({
  clientId: "my-app",
  brokers: ["localhost:9092"],
});

async function init() {
  const admin = exports.kafka.admin();
  console.log("Admin connecting...");
  await admin.connect();
  console.log("Admin connected");

  console.log("Creating topics [train_activation, train_cancellation]");
  await admin.createTopics({
    topics: [
      { topic: "train_activation", numPartitions: 2 },
      { topic: "train_cancellation", numPartitions: 2 },
    ],
  });
  console.log(
    "Topics created successfully [train_activation, train_cancellation]"
  );
  console.log("Disconnecting admin");
  await admin.disconnect();
}

init();
