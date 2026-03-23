// src/services/chatbot.service.js
//
// Orchestrator — routes every user question to the right engine.
//
// ─────────────────────────────────────────────────────────────
// DESIGN CHANGE:
//
//   Old approach: chatbot.service.js had its own keyword lists,
//   two-tier routing logic, nameExistsInCache scanning, and a
//   separate classifyIntent call. These were all fragile.
//
//   New approach:
//     • For candidate questions  → answerFromCache() is called.
//       Inside it, analyzeQuestion() (one LLM call) handles ALL
//       understanding: intent, entities, sections needed.
//
//     • For non-candidate questions → a SINGLE classifyIntent()
//       call distinguishes PDF / REPORT / OOS.
//
//     • The routing here is just: "does this look like it could
//       involve candidate data?" — we ask analyzeQuestion and
//       trust its intent field. No hardcoded keyword lists.
// ─────────────────────────────────────────────────────────────

import { loadPdfVectorstore } from "./rag.service.js";
import { classifyIntent } from "./ai.service.js";
import { runPdfMode } from "./rag.service.js";
import { runReportMode } from "./report.service.js";
import { warmUp, answerFromCache } from "./candidate.cache.js";
import { log } from "../utils/logger.js";
import { PDF_PATH, VECTRA_DIR, MAX_HISTORY } from "../config/constants.js";

let vectorstore = null;
let conversationHistory = [];

// ─── Init ─────────────────────────────────────────────────────────────────────
export async function init() {
  log.info("Step 1/2: Loading PDF vectorstore...");
  vectorstore = await loadPdfVectorstore(PDF_PATH, VECTRA_DIR);
  log.info("Step 1/2: PDF vectorstore ready.");

  log.info("Step 2/2: Loading candidate cache...");
  try {
    await warmUp();
    log.info("Step 2/2: Candidate cache ready.");
  } catch (err) {
    log.warn(`Step 2/2: Candidate cache failed — ${err.message}`);
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
  log.info("Conversation history cleared.");
}

// ─── Out-of-scope response ────────────────────────────────────────────────────
function runOutOfScope(question) {
  const shortQ =
    question.length > 70 ? question.slice(0, 70) + "..." : question;
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

// ─── History ──────────────────────────────────────────────────────────────────
function pushHistory(question, answer) {
  conversationHistory.push({ role: "user", content: question });
  conversationHistory.push({ role: "assistant", content: answer });
  if (conversationHistory.length > MAX_HISTORY * 2) {
    conversationHistory.splice(0, conversationHistory.length - MAX_HISTORY * 2);
  }
}

// ─────────────────────────────────────────────────────────────────────────────
// MAIN ENTRY POINT
// ─────────────────────────────────────────────────────────────────────────────
//
// Flow:
//   1. Try candidate cache first — answerFromCache internally calls
//      analyzeQuestion which returns intent. If intent is CANDIDATE or COUNT,
//      it answers. If intent is PDF/REPORT/OOS it returns that intent signal.
//
//   2. Use the intent signal to route to PDF, REPORT, or OOS.
//
// This means we have EXACTLY ONE LLM call for routing on the candidate path,
// and EXACTLY ONE LLM call (classifyIntent) on the non-candidate path.
// No double-calling, no keyword hacks, no fragile guards.

export async function chatbot(question) {
  if (!question?.trim()) return runOutOfScope("(empty message)");

  question = question.trim();
  log.info(`\nQuestion: "${question}"`);

  // ── Single call to answerFromCache — analyzeQuestion inside determines all ─
  // If intent is CANDIDATE or COUNT  → result.success = true, answer is ready.
  // If intent is PDF/REPORT/OOS      → result.success = false, result.forwardIntent is set.
  const result = await answerFromCache(question);

  if (result.success) {
    log.info("→ Route: CANDIDATE CACHE");
    pushHistory(question, result.answer);
    return {
      type: "CANDIDATE",
      answer: result.answer,
      dataframe: result.dataframe,
    };
  }

  // ── analyzeQuestion decided this is not a candidate question ──────────────
  const intent = result.forwardIntent;
  log.info(`→ Forward intent: ${intent}`);

  if (intent === "PDF") {
    log.info("→ Route: PDF");
    const r = await runPdfMode(question, conversationHistory, vectorstore);
    pushHistory(question, r.answer);
    return r;
  }

  if (intent === "REPORT") {
    log.info("→ Route: REPORT");
    const r = await runReportMode(question, conversationHistory);
    pushHistory(question, r.answer);
    return r;
  }

  log.info("→ Route: OUT OF SCOPE");
  return runOutOfScope(question);
}
