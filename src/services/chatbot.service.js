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

export function resetConversation() {
  conversationHistory = [];
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

let lastKnownCandidate = null; // track here in service layer too as safety net

function expandPronouns(question) {
  if (!PRONOUN_RE.test(question)) return question;
  if (!lastKnownCandidate) return question;

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
export async function chatbot(question, onChunk = null) {
  if (!question?.trim()) return runOutOfScope("(empty message)");

  question = question.trim();

  // ── Step 1: expand pronouns before any routing ───────────────────────────
  const originalQuestion = question;
  question = expandPronouns(question);

  log.info(`\nQuestion: "${originalQuestion}"${question !== originalQuestion ? ` → expanded: "${question}"` : ""}`);

  // ── Step 2: route to cache ───────────────────────────────────────────────
  const result = await answerFromCache(question, { onChunk });

  if (result.success) {
    log.info("-> Route: CANDIDATE CACHE");

    // Track last resolved candidate for pronoun expansion
    if (result.isSingleCandidate && result.entities?.candidateName) {
      lastKnownCandidate = result.entities.candidateName;
      log.info(`[PronounTrack] lastKnownCandidate = "${lastKnownCandidate}"`);
    }

    // Clear candidate tracking when switching to list queries
    if (!result.isSingleCandidate) {
      lastKnownCandidate = null;
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

  // ── OOS fallback ─────────────────────────────────────────────────────────
  // Only retry if the original question had a pronoun and we expanded it —
  // meaning the expanded version was already tried above. If it still failed,
  // it is genuinely OOS. Do NOT retry the same question blindly.
  log.info("-> Final Route: OUT OF SCOPE");
  return runOutOfScope(originalQuestion);
}