# Real-Time Scalable Chat Application

A production-grade, real-time chat application built with **React**, **Node.js**, **Socket.IO**, **Apache Kafka**, and **Redis**, orchestrated using **Docker Compose** with **Nginx** as a reverse proxy/load balancer across multiple backend instances. The project demonstrates event-driven microservice patterns, horizontal scaling, and real-time communication at scale.

---

## Table of Contents

- [Architecture](#architecture)
- [Features](#features)
- [Tech Stack](#tech-stack)
- [Project Structure](#project-structure)
- [How It Works](#how-it-works)
- [Getting Started](#getting-started)
- [Useful Commands](#useful-commands)
  - [Docker Commands](#docker-commands)
  - [Docker Compose Commands](#docker-compose-commands)
  - [Node.js & npm Commands](#nodejs--npm-commands)
  - [React Commands](#react-commands)
  - [Redis CLI Commands](#redis-cli-commands)
  - [Kafka Commands](#kafka-commands)
- [Port Reference](#port-reference)
- [Troubleshooting](#troubleshooting)

---

## Architecture

```
                                    ┌─────────────────────────────────────────────────┐
                                    │              Docker Compose Network              │
┌──────────────┐                    │                                                 │
│              │  HTTP/WS           │  ┌─────────┐     ┌───────────────────────────┐  │
│  React App   │───────────────────►│  │  Nginx  │────►│  chat-server-1 (:5000)    │  │
│ (Port 3000)  │                    │  │ (:80)   │     │  chat-server-2 (:5001)    │  │
│              │◄───────────────────│  │  LB     │────►│  chat-server-3 (:5002)    │  │
└──────────────┘                    │  └─────────┘     └────────────┬──────────────┘  │
                                    │                               │                 │
                                    │          ┌────────────────────┼────────────┐     │
                                    │          │                    │            │     │
                                    │  ┌───────▼───────┐   ┌───────▼──────────┐ │     │
                                    │  │ Redis Server   │   │ Kafka Cluster    │ │     │
                                    │  │ (:6379)        │   │ (3 Brokers)      │ │     │
                                    │  │ - Pub/Sub      │   │ kafka1 (:9092)   │ │     │
                                    │  │ - Chat History │   │ kafka2 (:9093)   │ │     │
                                    │  │ - Online Users │   │ kafka3 (:9094)   │ │     │
                                    │  │ - Msg Status   │   └───────┬──────────┘ │     │
                                    │  └───────────────┘            │            │     │
                                    │                       ┌───────▼──────────┐ │     │
                                    │  ┌───────────────┐    │ Zookeeper        │ │     │
                                    │  │ RedisInsight   │    │ (:2181)          │ │     │
                                    │  │ (:5540)        │    └─────────────────┘ │     │
                                    │  └───────────────┘                         │     │
                                    └─────────────────────────────────────────────┘     │
                                                                                       │
                                    ┌─────────────────────────────────────────────┐     │
                                    │        Kafka Consumer Groups                │     │
                                    │                                             │     │
                                    │  ┌─────────────────┐  ┌──────────────────┐  │     │
                                    │  │ message-group   │  │ analytics-group  │  │     │
                                    │  │ (Save + Emit)   │  │ (Log analytics)  │  │     │
                                    │  └─────────────────┘  └──────────────────┘  │     │
                                    │  ┌─────────────────┐  ┌──────────────────┐  │     │
                                    │  │ moderation-group│  │ notification-grp │  │     │
                                    │  │ (Filter bad     │  │ (Push notifs)    │  │     │
                                    │  │  words → notify)│  │                  │  │     │
                                    │  └─────────────────┘  └──────────────────┘  │     │
                                    └─────────────────────────────────────────────┘     │
```

### Data Flow

1. **User sends a message** → React emits `send_message` via Socket.IO
2. **Backend produces to Kafka** → Message is published to the `chat-messages` topic (3 partitions, replication factor 3)
3. **Kafka consumers process in parallel:**
   - **Message Consumer** (`message-group`) → Saves to Redis (chat history + message status) → Emits to sender & receiver via Socket.IO
   - **Analytics Consumer** (`analytics-group`) → Logs analytics events (extensible to MongoDB, ClickHouse, etc.)
   - **Moderation Consumer** (`moderation-group`) → Filters banned words → Produces `notifications` topic for clean messages
   - **Notification Consumer** (`notification-group`) → Handles push notifications (extensible to Firebase, APNS, Email, SMS)
4. **Redis Pub/Sub adapter** ensures Socket.IO events are broadcast across all 3 server instances

---

## Features

- **Real-Time Messaging** — Instant message delivery using WebSocket connections via Socket.IO
- **Event-Driven Architecture** — Apache Kafka decouples message processing into independent consumer groups
- **Horizontal Scaling** — 3 backend server instances running behind Nginx load balancer
- **Kafka Multi-Broker Cluster** — 3 Kafka brokers with Zookeeper for fault tolerance and high availability
- **Kafka Consumer Groups** — 4 independent consumers: message delivery, analytics, moderation, and notifications
- **Content Moderation** — Automated filtering of banned words before notification dispatch
- **Redis Pub/Sub Adapter** — Socket.IO Redis adapter ensures messages are broadcast across all server instances
- **Online User Tracking** — Tracks and displays currently active users using Redis Sets
- **Chat History Persistence** — Messages stored in Redis Lists, retrieved on reconnection
- **Message Status Tracking** — Real-time delivery receipts: `Sent ✓`, `Delivered ✓✓`, `Read ✓✓`
- **Typing Indicators** — Shows when another user is currently typing
- **Private Messaging** — 1-to-1 chat using Socket.IO rooms (user joins their own room)
- **Auto-Reconnect** — Username persisted in `localStorage`, auto-rejoin on page refresh
- **RedisInsight Dashboard** — Built-in Redis GUI for monitoring data in real time (Port 5540)
- **Fully Dockerized** — One-command setup with 10 containers using Docker Compose

---

## Tech Stack

| Layer              | Technology                                         |
|--------------------|----------------------------------------------------|
| **Frontend**       | React 19, Socket.IO Client 4.8                     |
| **Backend**        | Node.js 20, Express 5, Socket.IO 4.8               |
| **Message Broker** | Apache Kafka 7.4 (Confluent), 3-broker cluster     |
| **Coordination**   | Apache Zookeeper                                    |
| **Database/Cache** | Redis 7 (Pub/Sub + Data Store)                      |
| **Load Balancer**  | Nginx (reverse proxy + WebSocket upgrade)           |
| **Containerization** | Docker, Docker Compose 3.9                        |
| **Monitoring**     | RedisInsight                                        |

---

## Project Structure

```
Chat-App/
├── docker-compose.yml              # Orchestrates all 10 containers
├── README.md
│
├── BE/                              # Backend (Node.js + Express + Socket.IO + Kafka)
│   ├── Dockerfile                   # Node.js 20 Docker image
│   ├── index.js                     # Main server: Redis adapter, Kafka producers, Socket.IO handlers
│   ├── package.json
│   │
│   ├── config/
│   │   └── redisClient.js           # Redis client connection setup
│   │
│   └── kafka/
│       ├── client.js                # KafkaJS client (connects to 3 brokers)
│       ├── producer.js              # Kafka producer with retry logic
│       │
│       ├── consumers/
│       │   ├── messageConsumer.js   # Saves to Redis + emits to Socket.IO rooms
│       │   ├── analyticsConsumer.js # Logs analytics events
│       │   ├── moderationConsumer.js# Filters banned words, produces to 'notifications'
│       │   └── notificationConsumer.js # Handles push notification dispatch
│       │
│       └── topics/
│           └── createTopics.js      # Creates 'chat-messages' (3 partitions) & 'notifications' (2 partitions)
│
├── FE/
│   └── client/                      # Frontend (React 19 + Socket.IO Client)
│       ├── package.json
│       ├── public/
│       │   ├── index.html
│       │   ├── manifest.json
│       │   └── robots.txt
│       └── src/
│           ├── App.js               # Main chat UI: join, online users, messaging, typing, status
│           ├── App.css
│           ├── index.js             # React entry point
│           └── index.css
│
└── nginx/
    └── nginx.conf                   # Load balancing + WebSocket upgrade proxy config
```

---

## How It Works

### Backend Server Startup Sequence

Each of the 3 chat server instances performs these steps on startup:

1. **Connect to Redis** — Establishes Redis client connection
2. **Setup Redis Pub/Sub Adapter** — Creates pub/sub client pair for Socket.IO cross-instance messaging
3. **Connect Kafka Producer** — Connects with retry logic (5s intervals until Kafka is ready)
4. **Create Kafka Topics** — Creates `chat-messages` (3 partitions, RF=3) and `notifications` (2 partitions, RF=3)
5. **Start 4 Kafka Consumers** — Message, Analytics, Moderation, and Notification consumers
6. **Register REST API Endpoints** — `/online-users` and `/chat-history`
7. **Register Socket.IO Event Handlers** — All real-time event listeners

### Kafka Consumer Pipeline

```
                    ┌──────────────────┐
  send_message ────►│  chat-messages   │──── topic (3 partitions, RF=3)
                    │  Kafka Topic     │
                    └──────┬───────────┘
                           │
              ┌────────────┼────────────────┐
              │            │                │
     ┌────────▼───┐  ┌────▼──────┐  ┌──────▼──────────┐
     │ message-   │  │ analytics-│  │ moderation-      │
     │ group      │  │ group     │  │ group            │
     │            │  │           │  │                  │
     │ Save Redis │  │ Log event │  │ Filter bad words │
     │ Emit to    │  │ (future:  │  │ Produce to       │
     │ Socket.IO  │  │ MongoDB,  │  │ 'notifications'  │
     │ rooms      │  │ metrics)  │  │ topic            │
     └────────────┘  └───────────┘  └────────┬─────────┘
                                              │
                                    ┌─────────▼────────┐
                                    │ notifications    │
                                    │ Kafka Topic      │
                                    └─────────┬────────┘
                                              │
                                    ┌─────────▼────────┐
                                    │ notification-    │
                                    │ group            │
                                    │                  │
                                    │ Push notifs      │
                                    │ (future: Firebase│
                                    │  APNS, Email)    │
                                    └──────────────────┘
```

### Redis Data Structures Used

| Key Pattern           | Type   | Description                           |
|-----------------------|--------|---------------------------------------|
| `online_users`        | Set    | Currently connected usernames         |
| `chat_messages`       | List   | Ordered chat history (JSON strings)   |
| `message:<id>`        | Hash   | Per-message status tracking (sender, receiver, text, status) |

### Nginx Load Balancing

- Distributes incoming HTTP and WebSocket connections across 3 backend instances
- **WebSocket upgrade** headers (`Upgrade`, `Connection`) are properly forwarded
- Socket.IO Redis Adapter ensures cross-instance event synchronization
- Timeouts set to 60s for long-lived WebSocket connections

---

## API Endpoints

| Method | Endpoint         | Description                          |
|--------|------------------|--------------------------------------|
| GET    | `/online-users`  | Returns list of online usernames     |
| GET    | `/chat-history`  | Returns all stored messages as JSON  |

---

## WebSocket Events

| Event                   | Direction       | Description                                      |
|-------------------------|-----------------|--------------------------------------------------|
| `user_online`           | Client → Server | Register user as online, join personal room      |
| `send_message`          | Client → Server | Send message (produces to Kafka)                 |
| `receive_message`       | Server → Client | Deliver message to sender & receiver rooms       |
| `message_delivered`     | Client → Server | Mark message as delivered in Redis               |
| `message_read`          | Client → Server | Mark message as read in Redis                    |
| `message_status_update` | Server → Client | Broadcast updated message status (sent/delivered/read) |
| `online_users`          | Server → Client | Broadcast current online users list              |
| `typing`                | Bidirectional   | Notify that a user is typing                     |
| `stop_typing`           | Bidirectional   | Notify that a user stopped typing                |

---

## Kafka Topics

| Topic            | Partitions | Replication Factor | Producers                | Consumers                                    |
|------------------|------------|--------------------|--------------------------|----------------------------------------------|
| `chat-messages`  | 3          | 3                  | Chat server (on message) | message-group, analytics-group, moderation-group |
| `notifications`  | 2          | 3                  | Moderation consumer      | notification-group                            |

---

## Getting Started

### Prerequisites

- [Docker](https://www.docker.com/get-started) & Docker Compose installed
- [Node.js](https://nodejs.org/) v20+ (for running the frontend locally)

### 1. Clone the Repository

```bash
git clone https://github.com/<your-username>/Chat-App.git
cd Chat-App
```

### 2. Start All Backend Services (10 Containers)

```bash
docker compose up --build -d
```

This spins up:
- **Zookeeper** (Kafka coordination)
- **3 Kafka Brokers** (kafka1, kafka2, kafka3)
- **Redis Server** on port `6379`
- **RedisInsight** on port `5540`
- **3 Chat Server instances** (Node.js + Socket.IO + Kafka)
- **Nginx** load balancer on port `80`

### 3. Start the Frontend

```bash
cd FE/client
npm install
npm start
```

The React app will be available at **http://localhost:3000**.

### 4. Test It Out

Open the app in multiple browser tabs, enter different usernames, select a user, and start chatting in real time!

### 5. Monitor with RedisInsight

Open **http://localhost:5540** and connect to `redis:6379` to inspect keys, chat history, and online users.

---

## Useful Commands

### Docker Commands

```bash
# Check Docker version
docker --version

# List running containers
docker ps

# List all containers (including stopped)
docker ps -a

# View logs of a specific container
docker logs <container-name>
docker logs chat-server-1
docker logs kafka1
docker logs redis-server
docker logs nginx

# Follow logs in real time
docker logs -f chat-server-1

# Show last N lines of logs
docker logs --tail 50 chat-server-1

# Execute a command inside a running container
docker exec -it <container-name> bash
docker exec -it kafka1 bash
docker exec -it redis-server bash
docker exec -it nginx bash

# Inspect container details (IP, ports, env, etc.)
docker inspect <container-name>
docker inspect chat-server-1

# Stop a specific container
docker stop <container-name>

# Start a stopped container
docker start <container-name>

# Restart a container
docker restart <container-name>

# Remove a stopped container
docker rm <container-name>

# Remove all stopped containers
docker container prune

# List Docker images
docker images

# Remove a Docker image
docker rmi <image-id>

# Remove unused images
docker image prune

# Build an image from Dockerfile
docker build -t <image-name> .
docker build -t chat-server ./BE

# View container resource usage (CPU, Memory)
docker stats

# View Docker networks
docker network ls

# Inspect a network
docker network inspect <network-name>

# View Docker volumes
docker volume ls

# Remove unused volumes
docker volume prune

# Remove everything unused (containers, images, networks, volumes)
docker system prune -a --volumes
```

### Docker Compose Commands

```bash
# Start all services in detached mode
docker compose up -d

# Build and start all services
docker compose up --build -d

# Start with forced rebuild (no cache)
docker compose build --no-cache
docker compose up -d

# Stop all services
docker compose down

# Stop all services and remove volumes
docker compose down -v

# Stop all services, remove volumes and images
docker compose down -v --rmi all

# View status of all services
docker compose ps

# View logs of all services
docker compose logs

# View logs of a specific service
docker compose logs chat-server-1
docker compose logs kafka1

# Follow logs of all services
docker compose logs -f

# Restart a specific service
docker compose restart chat-server-1

# Scale a service (e.g., add more chat servers)
docker compose up -d --scale chat-server-1=2

# Execute command in a service container
docker compose exec redis-server redis-cli

# Rebuild a single service
docker compose up --build -d chat-server-1

# View the final composed config
docker compose config

# Pull latest images
docker compose pull
```

### Node.js & npm Commands

```bash
# Check Node.js version
node --version

# Check npm version
npm --version

# Initialize a new Node.js project
npm init -y

# Install all dependencies from package.json
npm install

# Install a specific package
npm install <package-name>
npm install express socket.io cors redis kafkajs

# Install as dev dependency
npm install --save-dev nodemon

# Install Socket.IO Redis Adapter
npm install @socket.io/redis-adapter

# Uninstall a package
npm uninstall <package-name>

# Run the server
node index.js

# Run with nodemon (auto-restart on changes)
npx nodemon index.js

# List installed packages
npm list

# List top-level packages only
npm list --depth=0

# Check for outdated packages
npm outdated

# Update packages
npm update

# Clean npm cache
npm cache clean --force

# Remove node_modules and reinstall
rm -rf node_modules package-lock.json
npm install
```

### React Commands

```bash
# Create a new React app
npx create-react-app client

# Navigate to client directory
cd FE/client

# Install dependencies
npm install

# Install Socket.IO client
npm install socket.io-client

# Start development server (port 3000)
npm start

# Build for production
npm run build

# Run tests
npm test

# Run tests with coverage
npm test -- --coverage

# Eject (WARNING: irreversible)
npm run eject

# Set environment variable (Linux/Mac)
REACT_APP_API_URL=http://localhost npm start

# Set environment variable (Windows PowerShell)
$env:REACT_APP_API_URL="http://localhost"; npm start
```

### Redis CLI Commands

```bash
# Connect to Redis inside Docker
docker exec -it redis-server redis-cli

# Or connect from host (if Redis is port-forwarded)
redis-cli -h localhost -p 6379

# --- Key Operations ---

# List all keys
KEYS *

# Check key type
TYPE <key>
TYPE online_users
TYPE chat_messages

# Delete a key
DEL <key>
DEL online_users

# Check if key exists
EXISTS online_users

# Set TTL on a key (seconds)
EXPIRE <key> <seconds>

# Check TTL
TTL <key>

# --- String Operations ---
SET mykey "hello"
GET mykey

# --- Set Operations (online_users) ---

# View all online users
SMEMBERS online_users

# Add a user
SADD online_users "john"

# Remove a user
SREM online_users "john"

# Count online users
SCARD online_users

# Check if user is online
SISMEMBER online_users "john"

# --- List Operations (chat_messages) ---

# View all messages
LRANGE chat_messages 0 -1

# View last 10 messages
LRANGE chat_messages -10 -1

# View first 5 messages
LRANGE chat_messages 0 4

# Get total message count
LLEN chat_messages

# Push a message
RPUSH chat_messages '{"sender":"test","text":"hello"}'

# Remove first message
LPOP chat_messages

# --- Hash Operations (message:<id> status) ---

# View message status
HGETALL message:<id>
HGETALL message:1716300000000

# Get specific field
HGET message:<id> status

# Set status
HSET message:<id> status "read"

# --- Pub/Sub (for debugging) ---

# Subscribe to all channels
PSUBSCRIBE *

# Subscribe to a specific channel
SUBSCRIBE <channel>

# Publish a message
PUBLISH <channel> "hello"

# --- Monitoring ---

# Watch all commands in real time
MONITOR

# Server info
INFO

# Memory usage
INFO memory

# Connected clients
INFO clients

# Flush all data (CAUTION!)
FLUSHALL

# Flush current database
FLUSHDB
```

### Kafka Commands

```bash
# Enter a Kafka broker container
docker exec -it kafka1 bash
docker exec -it kafka2 bash
docker exec -it kafka3 bash

# --- Topic Management ---

# List all topics
kafka-topics --list --bootstrap-server kafka1:9092

# Create a topic
kafka-topics --create \
  --topic <topic-name> \
  --bootstrap-server kafka1:9092 \
  --partitions 3 \
  --replication-factor 3

# Describe a topic (partitions, replicas, ISR)
kafka-topics --describe \
  --topic chat-messages \
  --bootstrap-server kafka1:9092

kafka-topics --describe \
  --topic notifications \
  --bootstrap-server kafka1:9092

# Delete a topic
kafka-topics --delete \
  --topic <topic-name> \
  --bootstrap-server kafka1:9092

# Alter topic partitions
kafka-topics --alter \
  --topic chat-messages \
  --partitions 6 \
  --bootstrap-server kafka1:9092

# --- Producer (send messages from CLI) ---

# Start interactive producer
kafka-console-producer \
  --topic chat-messages \
  --bootstrap-server kafka1:9092

# Produce with key
kafka-console-producer \
  --topic chat-messages \
  --bootstrap-server kafka1:9092 \
  --property "parse.key=true" \
  --property "key.separator=:"

# --- Consumer (read messages from CLI) ---

# Consume from beginning
kafka-console-consumer \
  --topic chat-messages \
  --bootstrap-server kafka1:9092 \
  --from-beginning

# Consume latest messages only
kafka-console-consumer \
  --topic chat-messages \
  --bootstrap-server kafka1:9092

# Consume with key and value
kafka-console-consumer \
  --topic chat-messages \
  --bootstrap-server kafka1:9092 \
  --from-beginning \
  --property print.key=true \
  --property key.separator=":"

# Consume from a specific partition
kafka-console-consumer \
  --topic chat-messages \
  --bootstrap-server kafka1:9092 \
  --partition 0 \
  --from-beginning

# Consume from notifications topic
kafka-console-consumer \
  --topic notifications \
  --bootstrap-server kafka1:9092 \
  --from-beginning

# --- Consumer Groups ---

# List all consumer groups
kafka-consumer-groups --list \
  --bootstrap-server kafka1:9092

# Describe a consumer group (offsets, lag)
kafka-consumer-groups --describe \
  --group message-group \
  --bootstrap-server kafka1:9092

kafka-consumer-groups --describe \
  --group analytics-group \
  --bootstrap-server kafka1:9092

kafka-consumer-groups --describe \
  --group moderation-group \
  --bootstrap-server kafka1:9092

kafka-consumer-groups --describe \
  --group notification-group \
  --bootstrap-server kafka1:9092

# Reset offsets to earliest
kafka-consumer-groups \
  --bootstrap-server kafka1:9092 \
  --group message-group \
  --topic chat-messages \
  --reset-offsets --to-earliest \
  --execute

# Reset offsets to latest
kafka-consumer-groups \
  --bootstrap-server kafka1:9092 \
  --group message-group \
  --topic chat-messages \
  --reset-offsets --to-latest \
  --execute

# --- Broker & Cluster Info ---

# List broker IDs
zookeeper-shell zookeeper:2181 ls /brokers/ids

# Describe cluster
kafka-metadata --snapshot /var/lib/kafka/data/__cluster_metadata-0/00000000000000000000.log 2>/dev/null || echo "Use kafka-broker-api-versions for broker info"

# Check broker API versions
kafka-broker-api-versions \
  --bootstrap-server kafka1:9092

# --- Log / Data ---

# Check log directories
kafka-log-dirs \
  --describe \
  --bootstrap-server kafka1:9092

# Dump log segment
kafka-dump-log \
  --files /var/lib/kafka/data/chat-messages-0/00000000000000000000.log \
  --print-data-log
```

### Zookeeper Commands

```bash
# Enter Zookeeper container
docker exec -it zookeeper bash

# Start Zookeeper shell
zookeeper-shell localhost:2181

# List registered brokers
ls /brokers/ids

# Get broker details
get /brokers/ids/1
get /brokers/ids/2
get /brokers/ids/3

# List topics in Zookeeper
ls /brokers/topics

# List consumer groups
ls /consumers

# Check cluster controller
get /controller

# Exit Zookeeper shell
quit
```

### Nginx Commands

```bash
# Enter Nginx container
docker exec -it nginx bash

# Test Nginx config syntax
nginx -t

# Reload Nginx config (no downtime)
nginx -s reload

# View Nginx access logs
docker exec -it nginx cat /var/log/nginx/access.log

# View Nginx error logs
docker exec -it nginx cat /var/log/nginx/error.log

# Follow Nginx logs
docker logs -f nginx
```

---

## Port Reference

| Service         | Container Name   | Internal Port | External Port | URL                          |
|-----------------|------------------|---------------|---------------|------------------------------|
| React App       | (local)          | 3000          | 3000          | http://localhost:3000        |
| Nginx LB        | nginx            | 80            | 80            | http://localhost             |
| Chat Server 1   | chat-server-1    | 5000          | —             | (internal only)              |
| Chat Server 2   | chat-server-2    | 5001          | —             | (internal only)              |
| Chat Server 3   | chat-server-3    | 5002          | —             | (internal only)              |
| Redis           | redis-server     | 6379          | 6379          | redis://localhost:6379       |
| RedisInsight    | redis-insight    | 5540          | 5540          | http://localhost:5540        |
| Kafka Broker 1  | kafka1           | 9092          | 9092          | kafka1:9092 (internal)       |
| Kafka Broker 2  | kafka2           | 9092          | 9093          | kafka2:9092 (internal)       |
| Kafka Broker 3  | kafka3           | 9092          | 9094          | kafka3:9092 (internal)       |
| Zookeeper       | zookeeper        | 2181          | 2181          | zookeeper:2181 (internal)    |

---

## Docker Compose Services Summary

| # | Service          | Image                            | Purpose                                    |
|---|------------------|----------------------------------|--------------------------------------------|
| 1 | zookeeper        | confluentinc/cp-zookeeper:7.4.0  | Kafka cluster coordination                 |
| 2 | kafka1           | confluentinc/cp-kafka:7.4.0      | Kafka broker 1                             |
| 3 | kafka2           | confluentinc/cp-kafka:7.4.0      | Kafka broker 2                             |
| 4 | kafka3           | confluentinc/cp-kafka:7.4.0      | Kafka broker 3                             |
| 5 | redis            | redis:7                          | In-memory data store + Pub/Sub             |
| 6 | redisinsight     | redis/redisinsight:latest        | Redis GUI monitoring tool                  |
| 7 | chat-server-1    | ./BE (custom Dockerfile)         | Node.js chat server instance 1             |
| 8 | chat-server-2    | ./BE (custom Dockerfile)         | Node.js chat server instance 2             |
| 9 | chat-server-3    | ./BE (custom Dockerfile)         | Node.js chat server instance 3             |
| 10| nginx            | nginx:latest                     | Reverse proxy & load balancer              |

---

## Troubleshooting

### Kafka not ready / consumers failing
Kafka brokers take a few seconds to elect leaders. The producer and message consumer have built-in retry logic (5s intervals). If consumers fail, check:
```bash
docker logs chat-server-1
docker compose restart chat-server-1 chat-server-2 chat-server-3
```

### Redis connection refused
Ensure the Redis container is running:
```bash
docker ps | grep redis
docker logs redis-server
```

### WebSocket connection failing
Verify Nginx is running and the config is correct:
```bash
docker exec -it nginx nginx -t
docker logs nginx
```

### Messages not appearing across tabs
This usually means the Redis Pub/Sub adapter isn't connected. Check server logs:
```bash
docker logs chat-server-1 | grep "Redis Adapter"
```

### Port already in use
```bash
# Find and kill process using a port (Windows)
netstat -ano | findstr :<PORT>
taskkill /PID <PID> /F

# Find and kill process using a port (Linux/Mac)
lsof -i :<PORT>
kill -9 <PID>
```

### Reset everything and start fresh
```bash
docker compose down -v --rmi all
docker compose up --build -d
```

---

## License

This project is open source and available under the [MIT License](LICENSE).
