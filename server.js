
const http = require("http");
const { WebSocketServer, WebSocket } = require("ws");
const { randomBytes } = require("crypto");

const PORT = process.env.PORT || 3000;
const rooms = new Map();

const server = http.createServer((req, res) => {
  res.writeHead(200, {
    "Content-Type": "text/plain; charset=utf-8"
  });
  res.end("Entry Connect server is running!");
});

const wss = new WebSocketServer({
  server,
  maxPayload: 16 * 1024
});

function send(ws, data) {
  if (ws.readyState === WebSocket.OPEN) {
    ws.send(JSON.stringify(data));
  }
}

function createRoomCode() {
  return "EC-" + randomBytes(5).toString("hex").toUpperCase();
}

function broadcast(room, data, except = null) {
  for (const client of room.clients) {
    if (client !== except) {
      send(client, data);
    }
  }
}

function sendMembers(room) {
  broadcast(room, {
    type: "members",
    count: room.clients.size
  });
}

wss.on("connection", (ws) => {
  ws.roomCode = null;

  send(ws, {
    type: "connected",
    message: "Entry Connect 서버에 연결됐어요."
  });

  ws.on("message", (raw) => {
    let msg;

    try {
      msg = JSON.parse(raw.toString());
    } catch {
      send(ws, {
        type: "error",
        message: "잘못된 메시지 형식이에요."
      });
      return;
    }

    if (!msg || typeof msg.type !== "string") {
      return;
    }

    // 새 협업 방 만들기
    if (msg.type === "create") {
      if (ws.roomCode) {
        send(ws, {
          type: "error",
          message: "이미 방에 참여 중이에요."
        });
        return;
      }

      let code;

      do {
        code = createRoomCode();
      } while (rooms.has(code));

      const room = {
        clients: new Set()
      };

      rooms.set(code, room);
      room.clients.add(ws);
      ws.roomCode = code;

      send(ws, {
        type: "created",
        roomCode: code
      });

      sendMembers(room);
      return;
    }

    // 기존 협업 방 참여하기
    if (msg.type === "join") {
      if (ws.roomCode) {
        send(ws, {
          type: "error",
          message: "먼저 현재 연결을 종료해 주세요."
        });
        return;
      }

      if (typeof msg.roomCode !== "string") {
        send(ws, {
          type: "error",
          message: "방 코드를 입력해 주세요."
        });
        return;
      }

      const code = msg.roomCode.trim().toUpperCase();

      if (!/^EC-[A-F0-9]{10}$/.test(code)) {
        send(ws, {
          type: "error",
          message: "방 코드 형식이 올바르지 않아요."
        });
        return;
      }

      const room = rooms.get(code);

      if (!room) {
        send(ws, {
          type: "error",
          message: "방을 찾을 수 없어요."
        });
        return;
      }

      room.clients.add(ws);
      ws.roomCode = code;

      send(ws, {
        type: "joined",
        roomCode: code
      });

      sendMembers(room);
      return;
    }

    // 같은 방의 다른 참여자에게 블록 변경 메시지 전달
    if (msg.type === "block-change") {
      if (!ws.roomCode) {
        send(ws, {
          type: "error",
          message: "먼저 협업 방에 참여해 주세요."
        });
        return;
      }

      const room = rooms.get(ws.roomCode);

      if (!room || !room.clients.has(ws)) {
        send(ws, {
          type: "error",
          message: "협업 방을 찾을 수 없어요."
        });
        return;
      }

      if (
        !msg.data ||
        typeof msg.data !== "object" ||
        Array.isArray(msg.data)
      ) {
        send(ws, {
          type: "error",
          message: "블록 변경 정보가 올바르지 않아요."
        });
        return;
      }

      broadcast(
        room,
        {
          type: "block-change",
          data: msg.data
        },
        ws
      );

      return;
    }

    send(ws, {
      type: "error",
      message: "아직 지원하지 않는 기능이에요."
    });
  });

  // 연결이 끊어졌을 때 방 인원 갱신
  ws.on("close", () => {
    const code = ws.roomCode;

    if (!code) return;

    const room = rooms.get(code);

    if (!room) return;

    room.clients.delete(ws);
    ws.roomCode = null;

    if (room.clients.size === 0) {
      rooms.delete(code);
    } else {
      sendMembers(room);
    }
  });
});

server.listen(PORT, "0.0.0.0", () => {
  console.log(`Entry Connect server listening on port ${PORT}`);
});
