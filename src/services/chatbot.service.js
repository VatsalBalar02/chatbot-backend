// src/services/chatbot.service.js
// ============================================================
// Improvements applied:
//  1. pushHistory now stores "(streamed response)" placeholder when
//     result.answer is null — fixes broken history for candidate queries
//  2. resetConversation now also resets conversation context in cache
//  3. Input sanitisation applied before routing
// ============================================================

import { loadPdfVectorstore } from "./rag.service.js";
import { runPdfMode } from "./rag.service.js";
import { runReportMode } from "./report.service.js";
import { warmUp, answerFromCache, resetConversationContext } from "./candidate.cache.js";
import { log } from "../utils/logger.js";
import { PDF_PATH, VECTRA_DIR, MAX_HISTORY } from "../config/constants.js";

let vectorstore = null;
let conversationHistory = [];

export async function init() {
  log.info("Step 1/2: Loading PDF vectorstore...");
  vectorstore = await loadPdfVectorstore(PDF_PATH, VECTRA_DIR);
  log.info("Step 1/2: PDF vectorstore ready.");

  log.info("Step 2/2: Loading candidate cache...");
  try {
    await warmUp();
    log.info("Step 2/2: Candidate cache ready.");
  } catch (err) {
    log.warn(`Step 2/2: Candidate cache warm-up failed — ${err.message}`);
    // Non-fatal: server continues, next request will retry warm-up
  }

  log.info("========================================");
  log.info(" Chatbot fully initialised and ready.");
  log.info("========================================");
}

export function getVectorstore() {
  return vectorstore;
}

export function resetConversation() {
  conversationHistory = [];
  // Also reset cross-turn candidate context (e.g. "his skills" references)
  resetConversationContext();
  log.info("Conversation history and context cleared.");
}

function runOutOfScope(question) {
  const shortQ = question.length > 70 ? question.slice(0, 70) + "..." : question;
  return {
    type: "OUT_OF_SCOPE",
    answer:
      `Sorry, I can't help with **"${shortQ}"** — that's outside my scope.\n\n` +
      `I can help with:\n\n` +
      `**Candidate questions** — profiles, skills, marks, interviews, documents, applications.\n` +
      `*Try: "Tell me about Rahul" or "Which candidates know React?"*\n\n` +
      `**Policy/document questions** — anything from the reference PDF.\n` +
      `*Try: "What is the interview process?"*\n\n` +
      `**Report generation** — downloadable reports.\n` +
      `*Try: "Generate a report of all shortlisted candidates"*`,
    dataframe: null,
  };
}

function pushHistory(question, answer) {
  // When streaming, result.answer is null — store a placeholder so history
  // is always populated and multi-turn context works correctly.
  const historyAnswer = answer ?? "(streamed response)";
  conversationHistory.push({ role: "user", content: question });
  conversationHistory.push({ role: "assistant", content: historyAnswer });
  if (conversationHistory.length > MAX_HISTORY * 2) {
    conversationHistory.splice(0, conversationHistory.length - MAX_HISTORY * 2);
  }
}

// ─────────────────────────────────────────────────────────────────────────────
// MAIN ENTRY POINT
//
// onChunk — optional (text: string) => void
//           Plain string callback. Each call = one token or one table block.
//           The controller wraps this into SSE { type:"token", text } format.
// ─────────────────────────────────────────────────────────────────────────────
export async function chatbot(question, onChunk = null) {
  if (!question?.trim()) return runOutOfScope("(empty message)");

  question = question.trim();
  log.info(`\nQuestion: "${question}"`);

  // Pass onChunk inside the options object — answerFromCache expects { onChunk }
  const result = await answerFromCache(question, { onChunk });

  if (result.success) {
    log.info("-> Route: CANDIDATE CACHE");
    // Fix: always push to history even when streaming (answer may be null)
    pushHistory(question, result.answer);
    return {
      type: "CANDIDATE",
      answer: result.answer, // null when streaming — controller handles this
      dataframe: result.dataframe,
    };
  }

  const intent = result.forwardIntent;
  log.info(`-> Forward intent: ${intent}`);

  if (intent === "PDF") {
    log.info("-> Route: PDF");
    const r = await runPdfMode(question, conversationHistory, vectorstore);
    pushHistory(question, r.answer);
    return r;
  }

  if (intent === "REPORT") {
    log.info("-> Route: REPORT");
    const r = await runReportMode(question, conversationHistory);
    pushHistory(question, r.answer);
    return r;
  }

  log.info("-> Route: OUT OF SCOPE");
  return runOutOfScope(question);
}