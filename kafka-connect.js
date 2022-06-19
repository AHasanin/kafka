const { Kafka } = require('kafkajs')


module.exports =  () =>{
    const kafka = new Kafka({
        brokers: ['my-cluster-kafka-brokers:9092'],
        retry: {
            initialRetryTime: 100,
            retries: 8
          }

    })
//     const admin = kafka.admin()
//     admin.connect()
//     .then(() => {
//         // admin.createTopics({
//         //     waitForLeaders: true,
//         //     type: 'NOT_CONTROLLER',
//         //     topics: [
//         //         {
//         //             topic: 'greeting',
//         //         }
//         //     ]
//         // }).then(()=>{
//         //     console.log('produce connect')
//         //     connectProducer(kafka);
//         // }).catch((err)=>{
//         //     console.log('ssssssssssssssssssssss', err)
//         // })
//         connectProducer(kafka);       
//     })
//     .catch((error) => {
//         console.log(error)
//         logger.error("Admin Connection failed");
       
//     });
  

       
// }

// function connectProducer(kafka){
//     global.kafkaProducer = kafka.producer()
//     kafkaProducer.connect()
//     .then(() => {
//         logger.info("Producer Connected Successfully");
//         kafkaProducer.send({
//             topic: 'greeting',
//             messages: [
//               { value: JSON.stringify({name:'hello'})},
//             ],
            
//           })
//     })
//     .catch((error) => {
//         console.log(error)
//         logger.error("Producer Connection failed");
//     });

const consumer = kafka.consumer({ groupId: 'group1' })
 
const run = async () => {
//   await producer.connect()
//   await producer.send({
//     topic: 'test-topic',
//     messages: [
//       { value: 'Hello KafkaJS user!' },
//     ],
//   })
 
  // Consuming
  await consumer.connect()
  await consumer.subscribe({ topic: 'Order.events', fromBeginning: true })
 
  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      console.log({
        partition,
        offset: message.offset,
        value: message.value.toString(),
      })
//         const val = JSON.parse(message.value.toString())
//         console.log(typeof val);
//         console.log(val.price)
    },
  })
}
 
run().catch(console.error)
}
// const consumer = kafka.consumer({ groupId: 'test-group' })
 
// const run = async () => {
//   // Producing
//   await producer.connect()
//   await producer.send({
//     topic: 'test-topic',
//     messages: [
//       { value: 'Hello KafkaJS user!' },
//     ],
//   })
 
//   // Consuming
//   await consumer.connect()
//   await consumer.subscribe({ topic: 'test-topic', fromBeginning: true })
 
//   await consumer.run({
//     eachMessage: async ({ topic, partition, message }) => {
//       console.log({
//         partition,
//         offset: message.offset,
//         value: message.value.toString(),
//       })
//     },
//   })
// }
 
// run().catch(console.error)
