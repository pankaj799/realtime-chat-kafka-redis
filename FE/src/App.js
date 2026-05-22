import React, { useEffect, useState } from "react";
import { io } from "socket.io-client";

const socket = io("http://localhost", {
  transports: ["websocket", "polling"],
});

function App() {

  const [message, setMessage] = useState("");
  const [messages, setMessages] = useState([]);
  const [username, setUsername] = useState("");
  const [joined, setJoined] = useState(false);

  const [onlineUsers, setOnlineUsers] = useState([]);
  const [selectedUser, setSelectedUser] = useState("");

  const [typingUser, setTypingUser] = useState("");

  // =========================
  // JOIN CHAT
  // =========================

  const joinChat = () => {

    if (!username.trim()) return;

    socket.emit("user_online", username);

    localStorage.setItem("username", username);

    setJoined(true);
  };

  // =========================
  // FETCH CHAT HISTORY
  // =========================

  const fetchMessages = async () => {

    try {

      const res = await fetch(
        "http://localhost/chat-history"
      );

      const data = await res.json();

      setMessages(data);

    } catch (err) {

      console.log(err);
    }
  };

  // =========================
  // FETCH ONLINE USERS
  // =========================

  const fetchOnlineUsers = async () => {

    try {

      const res = await fetch(
        "http://localhost/online-users"
      );

      const data = await res.json();

      setOnlineUsers(data.users || []);

    } catch (err) {

      console.log(err);
    }
  };

  // =========================
  // SEND MESSAGE
  // =========================

  const sendMessage = () => {

    if (!message.trim()) return;

    if (!selectedUser) {

      alert("Select a user first");

      return;
    }

    const messageData = {
      sender: username,
      receiver: selectedUser,
      text: message,
      time: new Date().toLocaleTimeString(),
    };

    socket.emit(
      "send_message",
      messageData
    );

    setMessage("");

    socket.emit("stop_typing", {
      userId: username,
    });
  };

  // =========================
  // HANDLE TYPING
  // =========================

  const handleTyping = (e) => {

    setMessage(e.target.value);

    socket.emit("typing", {
      userId: username,
    });

    clearTimeout(window.typingTimer);

    window.typingTimer = setTimeout(() => {

      socket.emit("stop_typing", {
        userId: username,
      });

    }, 1000);
  };

  // =========================
  // INITIAL LOAD
  // =========================

  useEffect(() => {

    fetchMessages();
    fetchOnlineUsers();

  }, []);

  // =========================
  // ON REFRESH, AUTO REJOIN
  // =========================

  useEffect(() => {
    const savedUser = localStorage.getItem("username");

    if (savedUser) {
      setUsername(savedUser);
      socket.emit("user_online", savedUser);
    }
  }, []);

  // =========================
  // SOCKET LISTENERS
  // =========================

  useEffect(() => {

    // RECEIVE MESSAGE

    socket.on(
      "receive_message",
      (data) => {

        setMessages((prev) => {

          // PREVENT DUPLICATES

          const exists = prev.find(
            (msg) => msg.id === data.id
          );

          if (exists) return prev;

          return [...prev, data];
        });

        // MESSAGE DELIVERED

        socket.emit(
          "message_delivered",
          data.id
        );

        // MESSAGE READ

        setTimeout(() => {

          socket.emit(
            "message_read",
            data.id
          );

        }, 2000);
      }
    );

    // ONLINE USERS

    socket.on(
      "online_users",
      (users) => {

        setOnlineUsers(users);
      }
    );

    // TYPING

    socket.on("typing", (data) => {

      if (data.userId !== username) {

        setTypingUser(data.userId);
      }
    });

    socket.on(
      "stop_typing",
      () => {

        setTypingUser("");
      }
    );

    // MESSAGE STATUS UPDATE

    socket.on(
      "message_status_update",
      (data) => {

        setMessages((prev) =>
          prev.map((msg) => {

            if (msg.id === data.messageId) {

              return {
                ...msg,
                status: data.status,
              };
            }

            return msg;
          })
        );
      }
    );

    return () => {

      socket.off("receive_message");

      socket.off("online_users");

      socket.off("typing");

      socket.off("stop_typing");

      socket.off(
        "message_status_update"
      );
    };

  }, [username]);

  // =========================
  // FILTER CHAT
  // =========================

  const filteredMessages = messages.filter(
    (msg) => {

      return (

        (msg.sender === username &&
          msg.receiver === selectedUser)

        ||

        (msg.sender === selectedUser &&
          msg.receiver === username)
      );
    }
  );

  // =========================
  // UI
  // =========================

  return (

    <div
      style={{
        maxWidth: "700px",
        margin: "auto",
        padding: "20px",
        fontFamily: "Arial",
      }}
    >

      <h1>Kafka Chat App</h1>

      {/* JOIN */}

      {!joined && (

        <div>

          <input
            type="text"
            placeholder="Enter username"
            value={username}
            onChange={(e) =>
              setUsername(e.target.value)
            }
            style={{
              width: "100%",
              padding: "10px",
              marginBottom: "10px",
            }}
          />

          <button onClick={joinChat}>
            Join Chat
          </button>

        </div>
      )}

      {/* MAIN CHAT */}

      {joined && (

        <>

          {/* ONLINE USERS */}

          <div
            style={{
              marginTop: "20px",
              marginBottom: "20px",
            }}
          >

            <h3>Online Users</h3>

            {onlineUsers
              .filter(
                (user) =>
                  user !== username
              )
              .map((user, i) => (

                <button
                  key={i}
                  onClick={() =>
                    setSelectedUser(user)
                  }
                  style={{
                    margin: "5px",
                    padding: "8px",
                    background:
                      selectedUser === user
                        ? "#4CAF50"
                        : "#ddd",
                    border: "none",
                    borderRadius: "5px",
                    cursor: "pointer",
                  }}
                >
                  🟢 {user}
                </button>
              ))}
          </div>

          {/* SELECTED CHAT */}

          {selectedUser && (

            <h3>
              Chat with {selectedUser}
            </h3>
          )}

          {/* CHAT BOX */}

          <div
            style={{
              border: "1px solid #ccc",
              height: "400px",
              overflowY: "scroll",
              padding: "10px",
              marginBottom: "10px",
              borderRadius: "10px",
            }}
          >

            {filteredMessages.map(
              (msg, index) => (

                <div
                  key={index}
                  style={{
                    marginBottom: "15px",
                    padding: "10px",
                    background:
                      msg.sender === username
                        ? "#DCF8C6"
                        : "#f1f1f1",
                    borderRadius: "10px",
                    textAlign:
                      msg.sender === username
                        ? "right"
                        : "left",
                  }}
                >

                  <strong>
                    {msg.sender}
                  </strong>

                  <span>
                    {" "}
                    ({msg.time})
                  </span>

                  <p>{msg.text}</p>

                  <small>

                    {msg.status === "sent" &&
                      "✓ Sent"}

                    {msg.status ===
                      "delivered" &&
                      "✓✓ Delivered"}

                    {msg.status === "read" &&
                      "✓✓ Read"}

                  </small>

                </div>
              )
            )}

            {/* TYPING */}

            {typingUser && (

              <p>
                ✍️ {typingUser} is typing...
              </p>
            )}

          </div>

          {/* MESSAGE INPUT */}

          <div
            style={{
              display: "flex",
              gap: "10px",
            }}
          >

            <input
              type="text"
              placeholder="Type message..."
              value={message}
              onChange={handleTyping}
              onKeyDown={(e) =>
                e.key === "Enter" &&
                sendMessage()
              }
              style={{
                flex: 1,
                padding: "10px",
              }}
            />

            <button
              onClick={sendMessage}
              style={{
                padding: "10px 20px",
              }}
            >
              Send
            </button>

          </div>

        </>
      )}
    </div>
  );
}

export default App;