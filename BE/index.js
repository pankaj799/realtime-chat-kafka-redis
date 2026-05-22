const express = require('express');
const cors = require('cors');
const { Server } = require('socket.io');
const http = require('http');
const { producer, connectProducer } = require('./kafka/producer');

const { redisClient, connectRedis } = require('./config/redisClient');
const { createAdapter } = require('@socket.io/redis-adapter');

// kafka Consumers
const runMessageConsumer = require('./kafka/consumers/messageConsumer');
const runAnalyticsConsumer = require('./kafka/consumers/analyticsConsumer');
const runModerationConsumer = require('./kafka/consumers/moderationConsumer');
const runNotificationConsumer = require('./kafka/consumers/notificationConsumer');

const createTopics = require('./kafka/topics/createTopics');

const PORT = process.env.PORT || 5000;

const app = express();

app.use(cors());

const server = http.createServer(app);

const io = new Server(server, {
  cors: {
    origin: 'http://localhost:3000',
    methods: ['GET', 'POST'],
  },
});

async function startServer() {
    try {

        // =========================
        // CONNECT REDIS
        // =========================

        await connectRedis();

        // =========================
        // REDIS PUB/SUB
        // =========================

        const pubClient = redisClient;

        const subClient = pubClient.duplicate();

        await subClient.connect();

        io.adapter(
            createAdapter(
                pubClient,
                subClient
            )
        );

        console.log(
            `Redis Adapter Connected on ${PORT}`
        );

        await connectProducer();

        // Initikated Kafka Topics

        await createTopics();

        // Inintialize Kafka Consumers

        runMessageConsumer(io);

        runAnalyticsConsumer();

        runModerationConsumer();

        runNotificationConsumer();

        // =========================
        // GET ONLINE USERS API
        // =========================

        app.get('/online-users', async (req, res) => {
            try {
                const users = await redisClient.sMembers('online_users');
                res.json({ users });
            } catch (error) {
                console.error("Error fetching online users:", error);
                res.status(500).json({ error: 'Internal Server Error' });
            }
        });

        // =========================
        // GET CHAT HISTORY API
        // =========================

        app.get('/chat-history', async (req, res) => {
            try {
                const messages = await redisClient.lRange('chat_messages', 0, -1);
                res.json(messages.map((m) => JSON.parse(m)));
            } catch (error) {
                console.error("Error fetching chat history:", error);
                res.status(500).json({ error: 'Internal Server Error' });
            }
        });


        io.on("connection", (socket) => {
            console.log(`User connected on ${PORT}: ${socket.id}`);

            // =========================
            // USER ONLINE
            // =========================

            socket.on("user_online", async (userId)=>{
                try {
                    socket.userId = userId;
                    
                    // USER ROOM
                    socket.join(userId);

                    //STORE ONLINE USERS IN REDIS SET
                    await redisClient.sAdd("online_users", userId);
                    console.log(`User ${userId} is online on server ${PORT}`);

                    const users = await redisClient.sMembers("online_users");
                    io.emit("online_users", users);
                }catch (error) {
                    console.error("Error in user_online event:", error);
                }
            });

            // =========================
            // SEND MESSAGE
            // =========================

            socket.on('send_message', async (data) => {

                try{

                    const message = {
                        id: Date.now().toString(),
                        sender: data.sender,
                        receiver: data.receiver,
                        text: data.text,
                        time: data.time,
                        status: 'sent',
                    };

                    // =========================
                    // PRODUCE TO KAFKA
                    // =========================

                    await producer.send({
                        topic: 'chat-messages',
                        messages: [
                            {
                                key: message.receiver,
                                value: JSON.stringify(message),
                            },
                        ],
                    });

                    console.log('Message:', message);

                    // Save message history
                    // await redisClient.rPush('chat_messages',JSON.stringify(message));

                    // Save message status
                    // await redisClient.hSet(
                    //     `message:${message.id}`,
                    //     {
                    //         sender: message.sender,
                    //         receiver: message.receiver,
                    //         text: message.text,
                    //         status: 'sent',
                    //     }
                    // );

                    // OPTIONAL TEMP CHAT HISTORY
                    // await redisClient.rPush(
                    //     'chat_messages',
                    //     JSON.stringify(message)
                    // );

                    // // Broadcast message
                    // io.emit('receive_message', message);

                    // =========================
                    // SEND ONLY TO RECEIVER
                    // =========================

                    // io.to(message.receiver)
                    //     .emit('receive_message', message);

                    // ALSO SEND BACK TO SENDER
                    // io.to(message.sender)
                    //     .emit('receive_message', message);

                }catch(error){
                    console.error("Error in send_message event:", error);
                }
            });

            // =========================
            // MESSAGE DELIVERED
            // =========================

            socket.on('message_delivered', async (messageId) => {
                try{
                    await redisClient.hSet(`message:${messageId}`, 'status', 'delivered');
                    io.emit('message_status_update', { messageId, status: 'delivered' });
                } catch (error) {
                    console.error('message_delivered error:', error);
                }
            });

            // =========================
            // MESSAGE READ
            // =========================

            socket.on('message_read', async (messageId) => {
                try{
                    await redisClient.hSet(`message:${messageId}`, 'status', 'read');
                    io.emit('message_status_update', { messageId, status: 'read' });
                }catch (error) {
                    console.error('message_read error:', error);
                }
            });

            // =========================
            // TYPING
            // =========================

            socket.on('typing', async (data) => {
                try {
                    socket.broadcast.emit('typing', {userId: data.userId});
                }catch (error) {
                    console.error('typing error:', error);
                }
            });

            // =========================
            // STOP TYPING
            // =========================

            socket.on('stop_typing', (data) => {
                try{
                    socket.broadcast.emit('stop_typing',{userId: data.userId});
                }catch (error) {
                    console.error('stop_typing error:', error);
                }
            });

            // =========================
                // DISCONNECT
            // =========================

            socket.on("disconnect", async () => {
                try {
                    if (socket.userId) {

                        await redisClient.sRem('online_users', socket.userId);

                        console.log(`${socket.userId} disconnected`);

                        const users = await redisClient.sMembers('online_users');

                        io.emit('online_users', users);
                    }
                    console.log(`Socket disconnected: ${socket.id}`);
                }catch (error) {
                    console.error("Error in disconnect event:", error);
                }

            });
        });


        server.listen(PORT, () => {
            console.log(`Server running on port ${PORT}`);
        });
    }catch (error) {
        console.error("Error starting server:", error);
    }

}

startServer();