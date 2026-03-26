// src/server.js

import "dotenv/config";
import app from "./app.js";
import {
  init as initChatbot,
  getVectorstore,
} from "./services/chatbot.service.js";
import { getDbSchema } from "./services/sql.services.js";

const PORT = process.env.PORT || 3001;
const HOST = process.env.HOST || "[IP_ADDRESS]";
import cookieParser from "cookie-parser";
app.use(cookieParser());

async function start() {
  console.log("Starting Chatbot...");

  try {
    await initChatbot();
  } catch (err) {
    console.error("Init failed:", err.message);
    process.exit(1);
  }

  app.get("/health", (_req, res) => {
    const vs = getVectorstore();
    res.json({
      status: "ok",
      pdf_enabled: !!vs,
    });
  });

  app.get("/schema", (_req, res) => {
    res.json({ schema: getDbSchema() });
  });

  app.listen(PORT, HOST, () => {
    console.log(`Server running on http://${HOST}:${PORT}`);
  });
}

start();
