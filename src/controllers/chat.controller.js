// src/controllers/chat.controller.js
// ============================================================
// Improvements applied:
//  1. Request timeout (30 s) — hung LLM/DB calls no longer keep
//     SSE connections open forever
//  2. try/catch/finally — res.end() is always called exactly once
//  3. sendEvent in catch is wrapped in its own try/catch — safe
//     to call even after headers are flushed
//  4. Timeout is cleared on both success AND error paths
//  5. writableEnded guard prevents "write after end" crash
// ============================================================

import { chatbot, resetConversation } from "../services/chatbot.service.js";
import { forceRefresh, getCacheStatus } from "../services/candidate.cache.js";
import { log } from "../utils/logger.js";

const STREAM_TIMEOUT_MS = 30_000; // 30 seconds

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

    // Safe event sender — guards against writing after the stream has ended
    const sendEvent = (payload) => {
      if (res.writableEnded) return;
      try {
        res.write(`data: ${JSON.stringify(payload)}\n\n`);
        if (res.flush) res.flush();
      } catch (writeErr) {
        log.warn(`sendEvent write error (client likely disconnected): ${writeErr.message}`);
      }
    };

    // onChunk receives a plain STRING and wraps it into SSE { type:"token", text }
    const onChunk = (text) => {
      if (text) sendEvent({ type: "token", text });
    };

    // ── Request timeout ──────────────────────────────────────────────────────
    // Prevents a hung DB call or LLM call from keeping the connection open forever.
    let timeoutFired = false;
    const timeoutHandle = setTimeout(() => {
      timeoutFired = true;
      log.warn(`Stream timeout after ${STREAM_TIMEOUT_MS}ms for question: "${question.slice(0, 80)}"`);
      sendEvent({ type: "error", message: "Request timed out. Please try again." });
      if (!res.writableEnded) {
        res.write("data: [DONE]\n\n");
        res.end();
      }
    }, STREAM_TIMEOUT_MS);

    try {
      const result = await chatbot(question.trim(), onChunk);

      // Clear timeout — we got a response in time
      clearTimeout(timeoutHandle);

      // If the timeout already fired, don't try to write again
      if (timeoutFired) return;

      // PDF / REPORT / OOS: answer was NOT streamed via onChunk — send it now
      if (result.answer) {
        sendEvent({ type: "token", text: result.answer });
      }

      sendEvent({
        type: "done",
        routeType: result.type,
        dataframe: result.dataframe ?? null,
      });
    } catch (err) {
      clearTimeout(timeoutHandle);
      log.error(`Stream error: ${err.message}`);
      if (!timeoutFired) {
        // Attempt to notify the client — safe even if headers are already sent
        try {
          sendEvent({ type: "error", message: "An error occurred. Please try again." });
        } catch (_) {
          // Swallow — client disconnected before we could notify
        }
      }
    } finally {
      // Always close the stream exactly once
      if (!res.writableEnded) {
        res.write("data: [DONE]\n\n");
        res.end();
      }
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
// Now also returns lastRefreshError so ops can see if background refresh failed
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