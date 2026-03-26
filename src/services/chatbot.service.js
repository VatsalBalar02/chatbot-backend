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
  }

  log.info("========================================");
  log.info(" Chatbot fully initialised and ready.");
  log.info("========================================");
}

export function getVectorstore() {
  return vectorstore;
}

export function resetConversation(sessionId = "default") {
  conversationHistory = [];
  resetConversationContext(sessionId);
  log.info(`Conversation history and context cleared for session: ${sessionId}`);
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
  const historyAnswer = answer ?? "(streamed response)";
  conversationHistory.push({ role: "user", content: question });
  conversationHistory.push({ role: "assistant", content: historyAnswer });
  if (conversationHistory.length > MAX_HISTORY * 2) {
    conversationHistory.splice(0, conversationHistory.length - MAX_HISTORY * 2);
  }
}

// ── Pronoun expander ─────────────────────────────────────────────────────────
// Expands "his skills" → "Dilip Prajapat skills" using last known candidate
// This runs BEFORE answerFromCache so GPT never sees raw pronouns
// ─────────────────────────────────────────────────────────────────────────────
const PRONOUN_RE = /\b(he|she|they|his|her|their|them|this person|the candidate|this candidate)\b/i;
const POSSESSIVE_RE = /\b(his|her|their)\b/gi;
const SUBJECT_RE = /\b(he|she|they|them|this person|the candidate|this candidate)\b/gi;

// let lastKnownCandidate = null; // track here in service layer too as safety net

const sessionCandidates = new Map();

function expandPronouns(question, sessionId = "default") {
  if (!PRONOUN_RE.test(question)) return question;

  // Check both local map AND cache session for candidate name
  const lastKnownCandidate = sessionCandidates.get(sessionId) ?? null;
  if (!lastKnownCandidate) {
    log.warn(`[PronounExpand] Pronoun detected but no candidate in session "${sessionId}" — skipping expansion`);
    return question;
  }

  // Remove pronouns and prepend candidate name
  const stripped = question
    .replace(POSSESSIVE_RE, "")   // remove "his", "her", "their"
    .replace(SUBJECT_RE, "")      // remove "he", "she", "they" etc
    .replace(/\s+/g, " ")
    .trim();

  const expanded = `${lastKnownCandidate} ${stripped}`.trim();
  log.info(`[PronounExpand] "${question}" → "${expanded}"`);
  return expanded;
}

// ─────────────────────────────────────────────────────────────────────────────
// MAIN ENTRY POINT
// ─────────────────────────────────────────────────────────────────────────────
export async function chatbot(question, onChunk = null, sessionId = "default") {
  if (!question?.trim()) return runOutOfScope("(empty message)");

  question = question.trim();

  const originalQuestion = question;
  question = expandPronouns(question, sessionId);
    log.info(`[PronounDebug] original: "${originalQuestion}" | expanded: "${question}" | session: "${sessionId}"`);

  log.info(`\nQuestion: "${originalQuestion}"${question !== originalQuestion ? ` → expanded: "${question}"` : ""}`);

  const result = await answerFromCache(question, { onChunk, sessionId });

  if (result.success) {
    log.info("-> Route: CANDIDATE CACHE");

    // Track last resolved candidate for pronoun expansion
    if (result.isSingleCandidate && result.entities?.candidateName) {
      // Single candidate by name — track for pronoun follow-up
      sessionCandidates.set(sessionId, result.entities.candidateName);
      log.info(`[PronounTrack] session="${sessionId}" lastKnownCandidate = "${result.entities.candidateName}"`);
    } else if (!result.isSingleCandidate && result.dataframe?.length === 1) {
      // List/filter query that returned exactly 1 result — track that candidate
      const onlyCandidate = result.dataframe[0]?.FullName;
      if (onlyCandidate) {
        sessionCandidates.set(sessionId, onlyCandidate);
        log.info(`[PronounTrack] Single result from list query — session="${sessionId}" lastKnownCandidate = "${onlyCandidate}"`);
      }
    } else {
      // Multiple results — clear tracking so stale candidate is not used
      sessionCandidates.delete(sessionId);
      log.info(`[PronounTrack] Multiple results — cleared lastKnownCandidate for session="${sessionId}"`);
    }


    pushHistory(originalQuestion, result.answer);
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
    pushHistory(originalQuestion, r.answer);
    return r;
  }

  if (intent === "REPORT") {
    log.info("-> Route: REPORT");
    const r = await runReportMode(question, conversationHistory);
    pushHistory(originalQuestion, r.answer);
    return r;
  }

log.info("-> Final Route: OUT OF SCOPE");
return runOutOfScope(originalQuestion);
}