const kafka = require('./client');

const producer = kafka.producer();

const sleep = (ms) =>
    new Promise((resolve) =>
        setTimeout(resolve, ms)
    );

const connectProducer = async () => {

    let connected = false;

    while (!connected) {

        try {

            await producer.connect();

            connected = true;

            console.log(
                'Kafka Producer Connected'
            );

        } catch (error) {

            console.log(
                'Kafka not ready, retrying in 5 seconds...'
            );

            console.log(error.message);

            await sleep(5000);
        }
    }
};

module.exports = {
    producer,
    connectProducer,
};