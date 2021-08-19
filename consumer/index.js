// import kafka library and eventType
const Kafka = require('node-rdkafka');
const {avroType} = require('../eventType');

// init consumer instance 
// group id & borker 
const consumer = new Kafka.KafkaConsumer({
    'group.id': 'kafka',
    'metadata.broker.list': 'localhost:9092',
  }, {});
// start connection 
consumer.connect();
// start receiving data 
consumer.on("ready",()=>{
    console.log('consumer ready');
    consumer.subscribe(['test']);
    consumer.consume();
}).on('data',(data)=>{
    console.log(`receive data ${avroType.fromBuffer(data.value)}`)
})