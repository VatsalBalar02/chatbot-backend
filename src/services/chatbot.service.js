import { loadPdfVectorstore } from "./rag.service.js";
import { runPdfMode } from "./rag.service.js";
import { runReportMode } from "./report.service.js";
import {
  warmUp,
  answerFromCache,
  resetConversationContext,
} from "./candidate.cache.js";
import { log } from "../utils/logger.js";
import { PDF_PATH, VECTRA_DIR, MAX_HISTORY } from "../config/constants.js";

let vectorstore = null;
const conversationHistories = new Map();

function getConversationHistory(sessionId) {
  if (!conversationHistories.has(sessionId)) {
    conversationHistories.set(sessionId, []);
  }
  return conversationHistories.get(sessionId);
}

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
  }

  log.info("========================================");
  log.info(" Chatbot fully initialised and ready.");
  log.info("========================================");
}

export function getVectorstore() {
  return vectorstore;
}

export function resetConversation(sessionId = "default") {
  conversationHistories.delete(sessionId);
  resetConversationContext(sessionId);
  log.info(
    `Conversation history and context cleared for session: ${sessionId}`,
  );
}

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

function pushHistory(question, answer, sessionId = "default") {
  const history = getConversationHistory(sessionId);
  const historyAnswer = answer ?? "(streamed response)";
  history.push({ role: "user", content: question });
  history.push({ role: "assistant", content: historyAnswer });
  if (history.length > MAX_HISTORY * 2) {
    history.splice(0, history.length - MAX_HISTORY * 2);
  }
}

// ─────────────────────────────────────────────────────────────────────────────
// MAIN ENTRY POINT
// ─────────────────────────────────────────────────────────────────────────────
export async function chatbot(question, onChunk = null, sessionId = "default") {
  if (!question?.trim()) return runOutOfScope("(empty message)");

  question = question.trim();

  const originalQuestion = question;
  log.info(`\nQuestion: "${originalQuestion}"`);

  const result = await answerFromCache(question, { onChunk, sessionId });

  if (result.success) {
    log.info("-> Route: CANDIDATE CACHE");
    pushHistory(originalQuestion, result.answer, sessionId);
    return {
      type: "CANDIDATE",
      answer: result.answer,
      dataframe: result.dataframe,
    };
  }

  const intent = result.forwardIntent;
  log.info(`-> Forward intent: ${intent}`);

  if (intent === "PDF") {
    log.info("-> Route: PDF");
    const r = await runPdfMode(question, conversationHistory, vectorstore);
    pushHistory(originalQuestion, r.answer, sessionId);
    return r;
  }

  if (intent === "REPORT") {
    log.info("-> Route: REPORT");
    const r = await runReportMode(question, conversationHistory);
    pushHistory(originalQuestion, r.answer, sessionId);
    return r;
  }

  log.info("-> Final Route: OUT OF SCOPE");
  return runOutOfScope(originalQuestion);
}
