const { kafka } = require('./kafka');

async function init(){
    const consumer = kafka.consumer({groupId: 'user-1'});

    console.log('Connect to consumer');
    await consumer.connect();

    console.log('Subscribe to topic');
    await consumer.subscribe({ topics: ['rider-update'], fromBeginning: true });

    console.log('loggin message');
    await consumer.run({
        eachMessage: async ({ topic, partition, message, heartbeat, pause }) => {
            console.log(`Topic: ${topic} partition: ${partition}`, message.value.toString());
        },
    })
}

init();