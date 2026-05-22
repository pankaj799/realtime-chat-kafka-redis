const kafka = require('../client');

const consumer = kafka.consumer({
    groupId: 'notification-group',
});

const runNotificationConsumer = async () => {

    await consumer.connect();

    console.log(
        'Notification Consumer Connected'
    );

    await consumer.subscribe({
        topic: 'notifications',
        fromBeginning: false,
    });

    await consumer.run({
        eachMessage: async ({ message }) => {

            try {

                const data = JSON.parse(
                    message.value.toString()
                );

                console.log(
                    'Send Push Notification:',
                    data
                );

                // Firebase Push
                // APNS
                // Email
                // SMS

            } catch (error) {

                console.error(
                    'Notification Consumer Error:',
                    error
                );
            }
        },
    });
};

module.exports = runNotificationConsumer;