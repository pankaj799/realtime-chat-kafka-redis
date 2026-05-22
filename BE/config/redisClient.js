const { createClient } = require('redis');

const redisClient = createClient({
    url: 'redis://redis:6379',
});

redisClient.on('error', (err) => {
    console.log('Redis Error:', err);
});

const connectRedis = async () => {

    if (!redisClient.isOpen) {

        await redisClient.connect();

        console.log('Redis Connected');
    }
};

module.exports = {
    redisClient,
    connectRedis,
};