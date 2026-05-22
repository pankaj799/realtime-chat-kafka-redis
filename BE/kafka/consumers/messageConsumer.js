const kafka = require('../client');
const { redisClient } = require('../../config/redisClient');

const consumer = kafka.consumer({
    groupId: 'message-group',
});

const sleep = (ms) =>
    new Promise((resolve) =>
        setTimeout(resolve, ms)
    );

const runConsumer = async (io) => {

    let connected = false;

    // =========================
    // CONNECT WITH RETRY
    // =========================
    while (!connected) {

        try {

            await consumer.connect();

            connected = true;

            console.log('Kafka Consumer Connected');

        } catch (error) {

            console.log(
                'Kafka consumer retrying in 5 seconds...'
            );

            console.log(error.message);

            await sleep(5000);
        }
    }

    // =========================
    // SUBSCRIBE TOPIC
    // =========================
    await consumer.subscribe({
        topic: 'chat-messages',
        fromBeginning: false, // IMPORTANT: avoid replay spam on restart
    });

    // =========================
    // PROCESS MESSAGES
    // =========================
    await consumer.run({
        eachMessage: async ({ message }) => {

            try {

                const data = JSON.parse(
                    message.value.toString()
                );

                console.log(
                    'Kafka Consumer Received:',
                    data
                );

                // =========================
                // SAVE CHAT HISTORY (Redis)
                // =========================
                await redisClient.rPush(
                    'chat_messages',
                    JSON.stringify(data)
                );

                // =========================
                // SAVE MESSAGE STATUS
                // =========================
                await redisClient.hSet(
                    `message:${data.id}`,
                    {
                        sender: data.sender,
                        receiver: data.receiver,
                        text: data.text,
                        status: data.status,
                    }
                );

                // =========================
                // EMIT TO RECEIVER
                // =========================
                io.to(data.receiver).emit(
                    'receive_message',
                    data
                );

                // =========================
                // EMIT TO SENDER (sync UI)
                // =========================
                io.to(data.sender).emit(
                    'receive_message',
                    data
                );

            } catch (error) {

                console.error(
                    'Kafka Consumer Error:',
                    error
                );
            }
        },
    });
};

module.exports = runConsumer;