const { Kafka } = require('kafkajs')


module.exports =  () =>{
    const kafka = new Kafka({
        brokers: ['my-cluster-kafka-brokers:9092'],
        retry: {
            initialRetryTime: 100,
            retries: 8
          }

    })

function connectProducer(kafka){
    global.kafkaProducer = kafka.producer()
    kafkaProducer.connect()
    .then(() => {
        logger.info("Producer Connected Successfully");
        kafkaProducer.send({
            topic: 'greeting',
            messages: [
              { value: JSON.stringify({name:'hello'})},
            ],
            
          })
    })
    .catch((error) => {
        console.log(error)
        logger.error("Producer Connection failed");
    });
}
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
        value:  message.value.toString(),
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
        price,
        creditCard,
        _id
      };
      if(messageObj.price > 200){
          console.log('inHere');
        await producer.send({
            topic: 'high-trans',
            messages: [
            { value: JSON.stringify()},
            ],
        });
      }
    
    },
  })
}
 
run().catch(console.error)
}