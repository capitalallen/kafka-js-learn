// import nodejs kafka library 
const Kafka = require('node-rdkafka');
const {avroType} = require('../eventType');

// init kafka producer 
// specify broker address
const stream = Kafka.Producer.createWriteStream({
    'metadata.broker.list':'localhost:9092'
}, {}, {
    topic: 'test'
});

// handle sending message error 
stream.on('error',(err)=>{
    console.log('error in our Kafka stream');
    console.log(err);
})

// send random message 
const sendRandomMessage = () =>{
    const category = getRandomAnimal();
    const noise = getRandomNoise();
    const event = {
        category,
        noise
    };
    // serialize message 
    const serlized_message = avroType.toBuffer(event);
    // send message 
    const success = stream.write(serlized_message);
    if (success) {
        console.log(`message queued (${JSON.stringify(event)})`);
      } else {
        console.log('Too many messages in the queue already..');
      }
}

// generate random message 
function getRandomAnimal() {
    const categories = ['CAT', 'DOG'];
    return categories[Math.floor(Math.random() * categories.length)];
  }
  
  function getRandomNoise(animal) {
    if (animal === 'CAT') {
      const noises = ['meow', 'purr'];
      return noises[Math.floor(Math.random() * noises.length)];
    } else if (animal === 'DOG') {
      const noises = ['bark', 'woof'];
      return noises[Math.floor(Math.random() * noises.length)];
    } else {
      return 'silence..';
    }
  }
  

// send message every 3 seconds 
setInterval(() => {
    sendRandomMessage();
  }, 5000);
  