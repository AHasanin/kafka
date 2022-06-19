const { Kafka } = require('kafkajs')
const avro = require("avsc");


module.exports = () => {
  const type = avro.Type.forSchema({
    type: "record",
    fields: [
      { name: "userId", type: "string" },
      { name: "productId", type: "string" },
      { name: "status", type: "string" },
      { name: "price", type: "int" },
      { name: "creditCard", type: "string" },
      { name: "_id", type: "string" },

    ],
  });

  const kafka = new Kafka({
    brokers: ['my-cluster-kafka-brokers:9092'],
    retry: {
      initialRetryTime: 100,
      retries: 8
    }

  })

  const consumer = kafka.consumer({ groupId: 'group1' })
  const producer = kafka.producer();

  const run = async () => {
    await producer.connect();

    // Consuming
    await consumer.connect()
    await consumer.subscribe({ topic: 'Order.events', fromBeginning: true });

    await consumer.run({
      eachMessage: async ({ topic, partition, message }) => {
        console.log({
          partition,
          offset: message.offset,
          value: message.value.toString(),
        });
        const value = message.value.toString();
        const userId = value.match(/userId=(.*?),/)[1];
        const productId = value.match(/productId=(.*?),/)[1];
        const status = value.match(/status=(.*?),/)[1];
        const price = value.match(/price=(.*?),/)[1];
        const creditCard = value.match(/creditCard=(.*?),/)[1];
        const _id = value.match(/_id=(.*?)}/)[1];
        const messageObj = {
          userId,
          productId,
          status,
          price : Number(price),
          creditCard,
          _id
        };
        console.log(messageObj);
        if (messageObj.price > 200) {
          console.log('inHere');
          const encodedValue = type.toBuffer(messageObj);
          await producer.send({
            topic: 'high-trans',
            messages: [
              { value: encodedValue },
            ],
          });
        }

      },
    })
  }

  run().catch(console.error)
}
