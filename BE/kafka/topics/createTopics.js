const kafka = require('../client');

const admin = kafka.admin();

const createTopics = async () => {

    try {

        await admin.connect();

        console.log('Kafka Admin Connected');

        await admin.createTopics({
            topics: [
                {
                    topic: 'chat-messages',
                    numPartitions: 3,
                    replicationFactor: 3,
                },
                {
                    topic: 'notifications',
                    numPartitions: 2,
                    replicationFactor: 3,
                },
            ],
        });

        console.log('Topics Created');

        await admin.disconnect();

    } catch (error) {

        console.error(
            'Create Topics Error:',
            error
        );
    }
};

module.exports = createTopics;