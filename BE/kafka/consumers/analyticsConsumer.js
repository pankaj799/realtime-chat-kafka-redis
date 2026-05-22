const kafka = require('../client');

const consumer = kafka.consumer({
    groupId: 'analytics-group',
});

const runAnalyticsConsumer = async () => {

    await consumer.connect();

    console.log(
        'Analytics Consumer Connected'
    );

    await consumer.subscribe({
        topic: 'chat-messages',
        fromBeginning: false,
    });

    await consumer.run({
        eachMessage: async ({ message }) => {

            try {

                const data = JSON.parse(
                    message.value.toString()
                );

                console.log(
                    'Analytics Event:',
                    {
                        sender: data.sender,
                        receiver: data.receiver,
                        time: data.time,
                    }
                );

                // HERE:
                // MongoDB analytics
                // ClickHouse
                // ElasticSearch
                // Metrics
                // Dashboard

            } catch (error) {

                console.error(
                    'Analytics Consumer Error:',
                    error
                );
            }
        },
    });
};

module.exports = runAnalyticsConsumer;