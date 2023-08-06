const { kafka } = require('./kafka');
const readline = require("readline");

const rl = readline.createInterface({
    input: process.stdin,
    output: process.stdout,
});

async function init() {
    const producer = kafka.producer();

    console.log('Connecting to Producer');
    await producer.connect();

    rl.setPrompt('> ');
    rl.prompt();

    rl.on('line', async function (line) {
        const [name, location] = line.split(' ');

        console.log('Sending a message');
        await producer.send({
            topic: 'rider-update',
            messages: [
                {
                    partition: location.toLowerCase() === "north" ? 0 : 1,
                    key: 'location-update',
                    value: JSON.stringify({ name, location }),
                },
            ]
        });
        console.log("message sent successfully");
    }).on('close', async () => {
        await producer.disconnect();
    })
}

init();