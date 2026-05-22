const kafka = require('../client');
const { producer } = require('../producer');

const consumer = kafka.consumer({
    groupId: 'moderation-group',
});

const bannedWords = [
    'badword',
    'abuse',
];

const runModerationConsumer = async () => {

    await consumer.connect();

    console.log(
        'Moderation Consumer Connected'
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

                const containsBadWord =
                    bannedWords.some((word) =>
                        data.text
                            .toLowerCase()
                            .includes(word)
                    );

                if (containsBadWord) {

                    console.log(
                        'Moderation Blocked:',
                        data.text
                    );

                    return;
                }

                // SEND NOTIFICATION EVENT
                await producer.send({
                    topic: 'notifications',
                    messages: [
                        {
                            key: data.receiver,
                            value: JSON.stringify({
                                type: 'NEW_MESSAGE',
                                receiver: data.receiver,
                                sender: data.sender,
                            }),
                        },
                    ],
                });

            } catch (error) {

                console.error(
                    'Moderation Consumer Error:',
                    error
                );
            }
        },
    });
};

module.exports = runModerationConsumer;