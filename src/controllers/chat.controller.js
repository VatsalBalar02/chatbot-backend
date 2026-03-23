// src/controllers/chat.controller.js

import { chatbot, resetConversation } from "../services/chatbot.service.js";
import { forceRefresh, getCacheStatus } from "../services/candidate.cache.js";
import { log } from "../utils/logger.js";

// ─────────────────────────────────────────────────────────────────────────────
// POST /api/chat
//
// STREAMING (default — Accept: text/event-stream or stream != false in body):
//   Each token:  data: {"type":"token","text":"..."}\n\n
//   On finish:   data: {"type":"done","routeType":"...","dataframe":[...]}\n\n
//                data: [DONE]\n\n
//
// NON-STREAMING (stream: false in body):
//   Returns full JSON: { success, type, answer, dataframe }
// ─────────────────────────────────────────────────────────────────────────────
export async function handleChat(req, res) {
  const { question, stream: streamParam } = req.body;

  if (!question || typeof question !== "string" || !question.trim()) {
    return res.status(400).json({ error: "Question is required." });
  }

  const wantsStream =
    streamParam !== false ||
    (req.headers["accept"] || "").includes("text/event-stream");

  // ── STREAMING ─────────────────────────────────────────────────────────────
  if (wantsStream) {
    res.setHeader("Content-Type", "text/event-stream");
    res.setHeader("Cache-Control", "no-cache");
    res.setHeader("Connection", "keep-alive");
    res.setHeader("X-Accel-Buffering", "no");
    res.flushHeaders();

    const sendEvent = (payload) => {
      res.write(`data: ${JSON.stringify(payload)}\n\n`);
      if (res.flush) res.flush();
    };

    // onChunk receives a plain STRING from ai.service.js / candidate.cache.js
    // and wraps it into the SSE { type:"token", text } shape for the frontend.
    const onChunk = (text) => {
      if (text) sendEvent({ type: "token", text });
    };

    try {
      const result = await chatbot(question.trim(), onChunk);

      // PDF / REPORT / OOS: answer was NOT streamed via onChunk — send it now
      if (result.answer) {
        sendEvent({ type: "token", text: result.answer });
      }

      sendEvent({
        type: "done",
        routeType: result.type,
        dataframe: result.dataframe ?? null,
      });

      res.write("data: [DONE]\n\n");
      res.end();
    } catch (err) {
      log.error(`Stream error: ${err.message}`);
      sendEvent({ type: "error", message: "An error occurred. Please try again." });
      res.write("data: [DONE]\n\n");
      res.end();
    }

    return;
  }

  // ── NON-STREAMING (legacy) ────────────────────────────────────────────────
  try {
    const result = await chatbot(question.trim(), null);
    return res.json({
      success: true,
      type: result.type,
      answer: result.answer,
      dataframe: result.dataframe ?? null,
    });
  } catch (err) {
    log.error(`Chat error: ${err.message}`);
    return res.status(500).json({
      success: false,
      error: "An error occurred while processing your request.",
    });
  }
}

// ─────────────────────────────────────────────────────────────────────────────
// POST /api/reset
// ─────────────────────────────────────────────────────────────────────────────
export function handleReset(req, res) {
  try {
    resetConversation();
    return res.json({ success: true, message: "Conversation reset." });
  } catch (err) {
    log.error(`Reset error: ${err.message}`);
    return res.status(500).json({ success: false, error: "Failed to reset conversation." });
  }
}

// ─────────────────────────────────────────────────────────────────────────────
// POST /api/cache/refresh
// ─────────────────────────────────────────────────────────────────────────────
export async function refreshCache(req, res) {
  try {
    const status = await forceRefresh();
    return res.json({ success: true, status });
  } catch (err) {
    log.error(`Cache refresh error: ${err.message}`);
    return res.status(500).json({ success: false, error: err.message });
  }
}

// ─────────────────────────────────────────────────────────────────────────────
// GET /api/cache/status
// ─────────────────────────────────────────────────────────────────────────────
export async function cacheStatus(req, res) {
  try {
    const status = getCacheStatus();
    return res.json({ success: true, status });
  } catch (err) {
    log.error(`Cache status error: ${err.message}`);
    return res.status(500).json({ success: false, error: err.message });
  }
}