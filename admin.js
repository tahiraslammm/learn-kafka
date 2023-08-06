const { kafka } = require('./kafka')

async function init() {
    const admin = kafka.admin();

    console.log('Connecting to admin');
    await admin.connect();

    console.log('List all the topics');
    const listOfAllTopics = await admin.listTopics();
    console.log(listOfAllTopics);

    console.log('Creating topic');
    await admin.createTopics({
        topics: [{
            topic: 'rider-update',
            numPartitions: 2
        }]
    });
    console.log('Topic created successfully');

    console.log('Disconnecting from admin');
    await admin.disconnect();
}

init();