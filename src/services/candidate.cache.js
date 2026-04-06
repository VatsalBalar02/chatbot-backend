// candidate.cache.js

import { getPool } from "../db/connection.js";
import { getOpenAIClient, generateNaturalAnswer } from "./ai.service.js";
import { log } from "../utils/logger.js";
import {
  refineResponseType,
  // resolveResponseType,
  handleResponseType,
} from "./response.engine.js";

const SP_NAME = "dbo.sp_GetAllCandidateDetails_Full";
const CACHE_TTL_MS = 30 * 60 * 1000;
const MAX_CANDIDATES_TO_LLM = 50;
const NAME_LIST_CHAR_CAP = 3000;

const ANALYSIS_CACHE_TTL = 5 * 60 * 1000;
const ANALYSIS_CACHE_MAX = 200;

const ALL_SECTIONS = [
  "root",
  "Profile",
  "Skills",
  "Education",
  "Experience",
  "Applications",
  "Applications.Interviews",
  "Documents",
  "Resumes",
];

let candidateStore = null;
let refreshTimer = null;
let warmUpPromise = null;
let lastRefreshError = null;
let candidateIndex = null;
let lastResolvedContext = {};

const analysisCache = new Map();

const sessions = new Map();
const MAX_HISTORY = 10;
const SESSION_TTL_MS = 60 * 60 * 1000; // 1 hour
const PRONOUN_RE =
  /\b(he|she|his|her|this person|the candidate|this candidate)\b/i;
// Prevent memory leak — clean up sessions idle for over 1 hour
setInterval(
  () => {
    const now = Date.now();
    for (const [id, session] of sessions.entries()) {
      if (now - session.lastActive > SESSION_TTL_MS) {
        sessions.delete(id);
        log.info(`[SessionCleanup] Expired session: ${id}`);
      }
    }
  },
  15 * 60 * 1000,
); // runs every 15 minutes

function getSession(sessionId) {
  if (!sessions.get(sessionId)) {
    sessions.set(sessionId, {
      lastTopic: null,
      lastResolvedContext: {},
      contextMemory: {
        candidateName: null,
        lastIntent: null,
        lastTopic: null,
        lastSections: null,
        mode: null,
        lastFilters: { entities: {}, filters: {} },
      },
      conversationMemory: [],
      lastActive: Date.now(),
      // queryGraph: [],
      // lastNodeId: null,
    });
  }
  const session = sessions.get(sessionId);
  session.lastActive = Date.now(); // refresh on every access
  return session;
}

function addToMemory(session, role, content) {
  if (!content) return;
  session.conversationMemory.push({ role, content });
  if (session.conversationMemory.length > MAX_HISTORY) {
    session.conversationMemory.shift();
  }
}

function buildConversationContext(session) {
  return session.conversationMemory
    .map((m) => `${m.role}: ${m.content}`)
    .join("\n");
}

function detectTopics(question) {
  const q = question.toLowerCase();

  const TOPIC_MAP = {
    SKILLS: [
      "skill",
      "skills",
      "technology",
      "technologies",
      "tech stack",
      "stack",
      "framework",
      "frameworks",
      "tools",
      "expertise",
    ],

    EDUCATION: [
      "education",
      "degree",
      "qualification",
      "university",
      "college",
      "study",
      "studies",
    ],

    EXPERIENCE: [
      "experience",
      "work",
      "worked",
      "working",
      "company",
      "companies",
      "job history",
      "career",
    ],

    INTERVIEW: [
      "interview",
      "interviews",
      "marks",
      "score",
      "feedback",
      "qualified",
      "result",
    ],

    JOB: [
      "job",
      "jobs",
      "application",
      "applications",
      "applied",
      "shortlist",
      "accepted",
      "hired",
    ],

    PROFILE: [
      "born",
      "birth",
      "dob",
      "age",
      "gender",
      "city",
      "location",
      "state",
      "language",
      "linkedin",
      "portfolio",
      "studying",
      "profile",
    ],

    DOCUMENTS: [
      "document",
      "documents",
      "aadhar",
      "pan",
      "salary slip",
      "offer letter",
      "itr",
      "bank",
      "proof",
    ],

    RESUMES: ["resume", "resumes", "cv", "curriculum vitae"],
  };

  const topics = [];

  for (const [topic, keywords] of Object.entries(TOPIC_MAP)) {
    if (keywords.some((keyword) => new RegExp(`\\b${keyword}\\b`).test(q))) {
      topics.push(topic);
    }
  }

  return topics;
}

// ─────────────────────────────────────────────────────────────────────────────
// CONTEXT ENGINE — single source of truth, no graph
// ─────────────────────────────────────────────────────────────────────────────

function classifyQuery(analysis) {
  if (analysis.entities?.candidateName) return "ENTITY";
  const hasFilters = Object.values(analysis.filters || {}).some(
    (v) => v != null,
  );
  const hasEntities = Object.values(analysis.entities || {}).some(
    (v) => v != null && v !== "",
  );
  if (hasFilters || hasEntities) return "FILTER";
  return "GENERIC";
}

function resolveAnalysis(analysis, session, question) {
  const isPronoun = PRONOUN_RE.test(question);
  const queryType = classifyQuery(analysis);

  // ── PRONOUN: inherit candidate name from context, clear all filters ───────
  if (isPronoun && !analysis.entities?.candidateName) {
    const resolvedName =
      session.contextMemory.candidateName ||
      session.lastResolvedContext?.candidateName ||
      null;
    if (resolvedName) {
      log.info(`[Context] Pronoun → resolved to "${resolvedName}"`);
      return {
        entities: { candidateName: resolvedName },
        filters: {},
      };
    }
    // No context to resolve — return as-is, handled downstream
    return { entities: analysis.entities, filters: analysis.filters };
  }

  // ── ENTITY query: full reset, entity dominates everything ─────────────────
  if (queryType === "ENTITY") {
    log.info(
      `[Context] ENTITY query → full reset, candidate: "${analysis.entities.candidateName}"`,
    );
    session.contextMemory.lastFilters = { entities: {}, filters: {} };
    return {
      entities: cleanObj(analysis.entities),
      filters: {},
    };
  }

  // ── FILTER/GENERIC with strong new entity (city/state/skill/job) → reset ──
  // ── FILTER/GENERIC with strong new entity (city/state/skill/job) ─────────
  const hasNewStrongEntity =
    analysis.entities?.city ||
    analysis.entities?.state ||
    analysis.entities?.skill ||
    analysis.entities?.jobTitle ||
    analysis.entities?.companyName ||
    analysis.entities?.universityName ||
    analysis.filters?.email ||
    analysis.filters?.phoneNumber;

  const prevHasContent =
    Object.keys(session.contextMemory.lastFilters?.entities || {}).length > 0 ||
    Object.keys(session.contextMemory.lastFilters?.filters || {}).length > 0;

  // Check if the new query's strong entities CONFLICT with previous context
  // e.g. previous had jobTitle:"New Issue", new has skill:"Adobe XD" → different topic → reset
  const prevEntities = session.contextMemory.lastFilters?.entities || {};
  const prevFilters = session.contextMemory.lastFilters?.filters || {};

  const isTopicSwitch =
    prevHasContent &&
    // Strong entity conflicts
    ((analysis.entities?.jobTitle &&
      prevEntities.jobTitle &&
      analysis.entities.jobTitle.toLowerCase() !==
        prevEntities.jobTitle.toLowerCase()) ||
      (analysis.entities?.skill &&
        prevEntities.skill &&
        analysis.entities.skill.toLowerCase() !==
          prevEntities.skill.toLowerCase()) ||
      (analysis.entities?.city &&
        prevEntities.city &&
        analysis.entities.city.toLowerCase() !==
          prevEntities.city.toLowerCase()) ||
      (analysis.entities?.state &&
        prevEntities.state &&
        analysis.entities.state.toLowerCase() !==
          prevEntities.state.toLowerCase()) ||
      // New skill introduced where there was none before
      (analysis.entities?.skill && !prevEntities.skill) ||
      // New jobTitle introduced where there was none before + previous had interview filters
      (analysis.entities?.jobTitle &&
        !prevEntities.jobTitle &&
        prevFilters.hasInterview != null) ||
      // Filter-only topic switch: completely different filter domain
      // e.g. previous had hasInterview, new has hasAadhar — unrelated domains
      (!hasNewStrongEntity &&
        Object.keys(cleanFilters(analysis.filters)).length > 0 &&
        Object.keys(prevFilters).length > 0 &&
        (() => {
          const interviewFilters = new Set([
            "hasInterview",
            "interviewStatus",
            "interviewer",
            "isQualified",
            "hasFeedback",
            "marksObtained",
            "marksOperator",
            "interviewCount",
            "interviewCountOperator",
            "interviewAfter",
            "interviewBefore",
          ]);
          const documentFilters = new Set([
            "hasAadhar",
            "hasPanCard",
            "hasBankPassbook",
            "hasBankStatement",
            "hasSalarySlip",
            "hasExperienceLetter",
            "hasOfferLetter",
            "hasItr",
            "hasAllDocuments",
          ]);
          const profileFilters = new Set([
            "gender",
            "languageKnown",
            "hasLinkedin",
            "hasPortfolio",
            "isStudying",
            "birthYear",
            "birthYearFrom",
            "birthYearTo",
          ]);
          const educationFilters = new Set([
            "ugDegree",
            "pgDegree",
            "hasPostGraduation",
            "ugGraduationYear",
            "pgGraduationYear",
          ]);
          const experienceFilters = new Set([
            "hasExperience",
            "isCurrentlyWorking",
            "experienceYears",
            "experienceYearsFrom",
            "experienceYearsTo",
            "experienceRole",
          ]);

          const domainOf = (filterSet) => {
            const keys = Object.keys(filterSet);
            if (keys.some((k) => interviewFilters.has(k))) return "INTERVIEW";
            if (keys.some((k) => documentFilters.has(k))) return "DOCUMENT";
            if (keys.some((k) => profileFilters.has(k))) return "PROFILE";
            if (keys.some((k) => educationFilters.has(k))) return "EDUCATION";
            if (keys.some((k) => experienceFilters.has(k))) return "EXPERIENCE";
            return "OTHER";
          };

          const newDomain = domainOf(cleanFilters(analysis.filters));
          const prevDomain = domainOf(prevFilters);

          return (
            newDomain !== "OTHER" &&
            prevDomain !== "OTHER" &&
            newDomain !== prevDomain
          );
        })()));

  // New independent query or topic switch → use as-is, clear previous context
  if ((hasNewStrongEntity && !prevHasContent) || isTopicSwitch) {
    log.info(
      `[Context] ${isTopicSwitch ? "Topic switch" : "New independent"} query → reset, no merge`,
    );
    session.contextMemory.lastFilters = { entities: {}, filters: {} };
    return {
      entities: cleanObj(analysis.entities),
      filters: cleanFilters(analysis.filters),
    };
  }

  // ── FOLLOW-UP: merge new filters onto previous, new values always win ─────
  // Only merge when the new query is clearly refining the SAME topic
  // (e.g. "shortlisted ones" after "React candidates" — same domain, additive filter)
  if (
    queryType === "FILTER" &&
    prevHasContent &&
    session.contextMemory.mode !== "SINGLE" &&
    !hasNewStrongEntity // ← key guard: if there's a strong new entity, don't merge
  ) {
    const prevEntities = session.contextMemory.lastFilters.entities || {};
    const prevFilters = session.contextMemory.lastFilters.filters || {};

    const mergedEntities = { ...prevEntities };
    const mergedFilters = { ...prevFilters };

    for (const [k, v] of Object.entries(analysis.entities || {})) {
      if (v != null && v !== "") mergedEntities[k] = v;
    }
    for (const [k, v] of Object.entries(analysis.filters || {})) {
      if (v != null) mergedFilters[k] = v;
    }

    log.info(`[Context] Follow-up merge applied`);
    return {
      entities: mergedEntities,
      filters: mergedFilters,
    };
  }

  // ── Default: use current analysis as-is ───────────────────────────────────
  return {
    entities: cleanObj(analysis.entities),
    filters: cleanFilters(analysis.filters),
  };
}

function cleanObj(obj) {
  const out = {};
  for (const [k, v] of Object.entries(obj || {})) {
    if (v != null && v !== "") out[k] = v;
  }
  return out;
}

function cleanFilters(obj) {
  const out = {};
  for (const [k, v] of Object.entries(obj || {})) {
    if (v != null) out[k] = v;
  }
  return out;
}

function updateSessionContext(session, analysis, resolved) {
  session.lastTopic = null;
  const hasOnlyCandidate =
    resolved.entities?.candidateName &&
    Object.keys(resolved.entities).length === 1;

  const prevMode = session.contextMemory.mode;

  if (
    hasOnlyCandidate ||
    (resolved.entities?.candidateName &&
      Object.keys(resolved.filters || {}).length === 0)
  ) {
    session.contextMemory.mode = "SINGLE";
    session.contextMemory.candidateName = resolved.entities.candidateName;
    // Always clear lastFilters when entering SINGLE mode
    session.contextMemory.lastFilters = { entities: {}, filters: {} };
    if (prevMode === "LIST") {
      log.info(`[Context] LIST → SINGLE, filters cleared`);
    }
  } else if (resolved.entities?.candidateName) {
    // Has candidate + other entities/filters — still SINGLE mode
    session.contextMemory.mode = "SINGLE";
    session.contextMemory.candidateName = resolved.entities.candidateName;
  } else {
    session.contextMemory.mode = "LIST";
    session.contextMemory.candidateName = null;
    // Save current filters for potential follow-up merging
    session.contextMemory.lastFilters = {
      entities: resolved.entities || {},
      filters: resolved.filters || {},
    };
  }

  session.contextMemory.lastIntent = analysis.intent;
  session.contextMemory.lastSections = analysis.sectionsNeeded;
}
//  PUBLIC API

export async function warmUp() {
  if (warmUpPromise) return warmUpPromise;
  warmUpPromise = _doWarmUp().finally(() => {
    warmUpPromise = null;
  });
  return warmUpPromise;
}

async function _doWarmUp() {
  log.info("Candidate cache warm-up starting...");
  const MAX_ATTEMPTS = 3;
  for (let attempt = 1; attempt <= MAX_ATTEMPTS; attempt++) {
    try {
      await loadFromSp();
      scheduleAutoRefresh();
      lastRefreshError = null;
      log.info(
        `Candidate cache ready — ${candidateStore.candidates.length} candidates, cachedAt: ${candidateStore.cachedAt}`,
      );
      return;
    } catch (err) {
      log.error(
        `Warm-up attempt ${attempt}/${MAX_ATTEMPTS} failed: ${err.message}`,
      );
      lastRefreshError = err.message;
      if (attempt < MAX_ATTEMPTS) {
        const delay = attempt * 2000;
        log.info(`Retrying warm-up in ${delay}ms...`);
        await new Promise((r) => setTimeout(r, delay));
      } else {
        throw err;
      }
    }
  }
}

function handleGreetingsAndSmallTalk(question) {
  if (detectGreeting(question)) {
    return {
      success: true,
      answer: getGreetingResponse(),
      dataframe: [],
    };
  }

  const smallTalk = detectSmallTalk?.(question);
  if (smallTalk) {
    return {
      success: true,
      answer: smallTalk,
      dataframe: [],
    };
  }

  return null;
}

async function ensureCacheReady() {
  if (!candidateStore) {
    log.warn("Cache cold — triggering warm-up");
    await warmUp();
  }

  const ageMs = Date.now() - candidateStore.cachedAt.getTime();
  if (ageMs > CACHE_TTL_MS) {
    log.info("Cache stale — background refresh triggered");
    loadFromSp().catch((e) => {
      lastRefreshError = e.message;
      log.error(`Background refresh failed: ${e.message}`);
    });
  }
}

function sanitizeQuestion(question) {
  return String(question).trim().slice(0, 500).replace(/[`\\]/g, " ");
}

function handleContextReset(question, session) {
  const isHardReset =
    question.toLowerCase().includes("show all") ||
    question.toLowerCase().includes("reset") ||
    question.toLowerCase().includes("start over");

  if (isHardReset) {
    session.contextMemory.mode = "LIST";
    session.contextMemory.candidateName = null;
    session.lastTopic = null;
    session.conversationMemory = [];
    session.contextMemory.lastFilters = {};

    log.info("[ContextReset] Hard reset triggered");
  }
}

export async function answerFromCache(question, options = {}) {
  const sessionId = options?.sessionId ?? "default";
  const session = getSession(sessionId);

  // const { lastTopic, lastResolvedContext, contextMemory, conversationMemory } = session;

  // const analysis = await analyzeQuestion(question, candidateStore.candidates, session);
  addToMemory(session, "user", question);
  const onChunk =
    typeof options === "function" ? options : (options?.onChunk ?? null);

  question = sanitizeQuestion(question);
  await ensureCacheReady();

  const greetingResponse = handleGreetingsAndSmallTalk(question);
  if (greetingResponse) return greetingResponse;

  let analysis = await analyzeQuestion(
    question,
    candidateStore.candidates,
    session,
  );

  // ── FUZZY NAME RESOLUTION ────────────────────────────────────────────────
  if (analysis.entities?.candidateName) {
    const matches = fuzzyMatchName(
      analysis.entities.candidateName,
      candidateStore.candidates,
    );
    if (matches.length === 1) {
      const best = matches[0].candidate;
      log.info(
        `[FuzzyMatch] "${analysis.entities.candidateName}" → "${best.FullName}"`,
      );
      analysis.entities.candidateName = best.FullName;
    }
  } else if (matches.length > 1) {
    const names = matches.map((m) => m.candidate.FullName);
    const msg = `Did you mean:\n- ${names.join("\n- ")}`;
    // Don't save context — we're in limbo waiting for clarification
    // But DO clear lastFilters so the clarification answer doesn't inherit stale state
    session.contextMemory.lastFilters = { entities: {}, filters: {} };
    if (onChunk) await onChunk(msg);
    return { success: true, answer: onChunk ? null : msg, dataframe: [] };
  }

  // ── HARD RESET on explicit user request ──────────────────────────────────
  handleContextReset(question, session);

  // ── RESOLVE CONTEXT (single authoritative function) ──────────────────────
  const resolved = resolveAnalysis(analysis, session, question);
  analysis.entities = resolved.entities;
  analysis.filters = resolved.filters;

  log.info(
    `[Normalised] entities: ${JSON.stringify(analysis.entities)} | filters: ${JSON.stringify(analysis.filters)}`,
  );

  // ── PRONOUN with no context ───────────────────────────────────────────────
  const isPronounQuery = PRONOUN_RE.test(question);
  if (isPronounQuery && !analysis.entities?.candidateName) {
    log.warn(`[Context] Pronoun detected but no candidate in context`);
    return {
      success: true,
      answer: "Could you clarify which candidate you're referring to?",
      dataframe: [],
    };
  }

  // ── VAGUE QUERY HANDLING ──────────────────────────────────────────────────
  const hasAnyFilter = Object.values(analysis.filters || {}).some(
    (v) => v != null,
  );
  const hasAnyEntity = !!(
    analysis.entities?.candidateName ||
    analysis.entities?.skill ||
    analysis.entities?.city ||
    analysis.entities?.state ||
    analysis.entities?.jobTitle ||
    analysis.entities?.companyName ||
    analysis.entities?.universityName
  );
  const isVagueQuery = !hasAnyEntity && !hasAnyFilter;

  if (isVagueQuery) {
    if (session.contextMemory.candidateName) {
      analysis.entities.candidateName = session.contextMemory.candidateName;
      log.info(
        `[Context] Vague → filled from SINGLE context: "${session.contextMemory.candidateName}"`,
      );
    } else if (analysis.intent !== "COUNT") {
      const isListIntent =
        /\b(all|every|list|show|total|count|how many|candidates?|people)\b/i.test(
          question,
        );
      if (!isListIntent) {
        return {
          success: true,
          answer: "Please specify the candidate name.",
          dataframe: [],
        };
      }
      log.info(`[Context] Vague list query → returning all candidates`);
    }
  }

  // ── TOPICS & SECTIONS ─────────────────────────────────────────────────────
  const topics = detectTopics(question);
  if (topics.length > 0 && !analysis.entities?.candidateName) {
    session.lastTopic = topics;
    log.info(`[Context] Topic set: ${topics}`);
  }

  // ── CLARIFICATION ─────────────────────────────────────────────────────────
  if (analysis.askClarification) {
    const msg = analysis.clarificationMessage;
    if (onChunk) await onChunk(msg);
    return { success: true, answer: onChunk ? null : msg, dataframe: [] };
  }

  // ── UPDATE SESSION STATE ──────────────────────────────────────────────────
  updateSessionContext(session, analysis, resolved);
  fixMisclassifiedIntent(analysis, question);

  // Rebuild resolved to reflect any mutations from fixMisclassifiedIntent
  resolved.entities = cleanObj(analysis.entities);
  resolved.filters = cleanFilters(analysis.filters);

  log.info(`[Step 1] Analysis: ${JSON.stringify(analysis)}`);

  let finalSections = analysis.sectionsNeeded;

  // 🔥 APPLY LAST TOPIC (CRITICAL FIX)
  const isStandaloneNameQuery =
    analysis.entities?.candidateName &&
    Object.keys(analysis.entities).filter((k) => analysis.entities[k])
      .length === 1 &&
    question.trim().split(/\s+/).length <= 3;

  const currentTopics = detectTopics(question);
  if (
    currentTopics.length === 0 &&
    session.lastTopic &&
    analysis.entities?.candidateName &&
    !isStandaloneNameQuery
  ) {
    const mergedSections = new Set(["root"]);
    const topicsToApply = Array.isArray(session.lastTopic)
      ? session.lastTopic
      : [session.lastTopic];

    for (const t of topicsToApply) {
      // or topicsToApply
      if (t === "SKILLS") mergedSections.add("Skills");
      if (t === "EDUCATION") mergedSections.add("Education");
      if (t === "EXPERIENCE") mergedSections.add("Experience");
      if (t === "INTERVIEW") {
        mergedSections.add("Applications");
        mergedSections.add("Applications.Interviews");
      }
      if (t === "JOB") mergedSections.add("Applications");
      if (t === "PROFILE") mergedSections.add("Profile"); // ← NEW
      if (t === "DOCUMENTS") mergedSections.add("Documents"); // ← NEW
      if (t === "RESUMES") mergedSections.add("Resumes"); // ← NEW
    }
    finalSections = [...mergedSections];
    log.info(
      `[ContextFix] Applying last topics: ${topicsToApply.join(",")} → ${finalSections}`,
    );
    session.lastTopic = null;
  } else if (isStandaloneNameQuery) {
    session.lastTopic = null;
  }

  if (["PDF", "REPORT", "OOS"].includes(analysis.intent)) {
    return {
      success: false,
      forwardIntent: analysis.intent,
      answer: null,
      dataframe: null,
    };
  }

  // const vaguePatterns = ["interview date", "job details", "interview status"];

  // if (isVagueQuery) {
  //   if (session.contextMemory.candidateName) {
  //     analysis.entities.candidateName = session.contextMemory.candidateName;
  //     log.info(`[ContextResolve] Filled missing candidate`);
  //   } else if (analysis.intent === "COUNT") {
  //     // COUNT with no filters = total count, handled above — let it fall through
  //   } else {
  //     // Only block if the question is clearly about a specific person (short, no list keywords)
  //     const isListIntent =
  //       /\b(all|every|list|show|total|count|how many|candidates?|people)\b/i.test(
  //         question,
  //       );
  //     if (!isListIntent) {
  //       return {
  //         success: true,
  //         answer: "Please specify the candidate name.",
  //         dataframe: [],
  //       };
  //     }
  //     // For list queries with no filters, show all candidates
  //     log.info(
  //       `[VagueQuery] List intent detected with no filters → returning all candidates`,
  //     );
  //   }
  // }
  // 🔥 APPLY PREVIOUS TOPIC
  if (currentTopics.length > 0) {
    const mergedSections = new Set(
      Array.isArray(finalSections) ? finalSections : ["root"],
    );

    for (const t of currentTopics) {
      if (t === "SKILLS") mergedSections.add("Skills");
      if (t === "INTERVIEW") {
        mergedSections.add("Applications");
        mergedSections.add("Applications.Interviews");
      }
      if (t === "EDUCATION") mergedSections.add("Education");
      if (t === "EXPERIENCE") mergedSections.add("Experience");
      if (t === "JOB") mergedSections.add("Applications");
    }
    finalSections = [...mergedSections];
    log.info(
      `[Fix] Merged sections for topics [${currentTopics.join(",")}]: ${finalSections}`,
    );
    session.lastTopic = null;
  }

  // interviewRound not in SP
  if (analysis.filters?.interviewRound) {
    const msg =
      `The interview round type (e.g. "Coding Round", "HR Round") is not stored in the database — only interview **status** is tracked.\n\n` +
      `You can filter by status instead:\n` +
      `- **Completed** — *"candidates whose interview is completed"*\n` +
      `- **Scheduled** — *"candidates with scheduled interviews"*\n` +
      `- **In Progress** — *"candidates with interview in progress"*\n` +
      `- **Postponed** — *"candidates with postponed interviews"*\n` +
      `- **Cancelled** — *"candidates with cancelled interviews"*`;

    if (onChunk) await onChunk(msg);
    return {
      success: true,
      type: "CACHE",
      dataframe: [],
      cachedAt: candidateStore.cachedAt,
      answer: onChunk ? null : msg,
    };
  }

  // COUNT intent
  if (analysis.intent === "COUNT") {
    const isPerCandidateCount =
      /interview\s*(count|counts|number|total)/i.test(question) ||
      /number\s+of\s+interviews?/i.test(question) ||
      /(total|count)\s+.*interview/i.test(question) ||
      /interview.*per\s*(candidate|person)/i.test(question) ||
      /candidate.*with.*interview\s*count/i.test(question);

    if (isPerCandidateCount) {
      analysis.intent = "CANDIDATE";
      log.info(
        `[IntentOverride] COUNT → CANDIDATE (per-candidate interview count query)`,
      );
      // fall through to CANDIDATE handling below
    } else {
      const hasEntity = Object.values(analysis.entities || {}).some(
        (v) => v != null && v !== "",
      );
      const hasFilter = Object.values(analysis.filters || {}).some(
        (v) => v != null,
      );

      if (!hasEntity && !hasFilter) {
        const total = candidateStore.candidates.length;
        const msg = `There are currently **${total}** candidate${total !== 1 ? "s" : ""} registered in the system (as of ${candidateStore.cachedAt.toLocaleString()}).`;
        updateSessionContext(session, analysis, resolved);
        if (onChunk) await onChunk(msg);
        return {
          success: true,
          type: "CACHE_COUNT",
          dataframe: [],
          cachedAt: candidateStore.cachedAt,
          answer: onChunk ? null : msg,
        };
      }

      const countFiltered = filterCandidates(
        analysis.entities,
        analysis.filters || {},
      );
      log.info(`[Count] Filtered candidates: ${countFiltered.length}`);

      const label = buildFilterLabel(analysis.entities, analysis.filters || {});
      if (countFiltered.length === 0) {
        const msg = `No candidates found ${label}.`;
        if (onChunk) await onChunk(msg);
        return {
          success: true,
          type: "CACHE_COUNT",
          dataframe: [],
          cachedAt: candidateStore.cachedAt,
          answer: onChunk ? null : msg,
        };
      }

      const msg = `There are **${countFiltered.length}** candidate${countFiltered.length !== 1 ? "s" : ""} ${label}.`;
      if (onChunk) await onChunk(msg);
      return {
        success: true,
        type: "CACHE_COUNT",
        dataframe: [],
        cachedAt: candidateStore.cachedAt,
        answer: onChunk ? null : msg,
      };
    }
  }

  // CANDIDATE intent

  const q = question.toLowerCase();
  const isHighestMarks =
    (q.includes("highest") ||
      q.includes("top scorer") ||
      q.includes("most marks") ||
      q.includes("best marks")) &&
    (q.includes("mark") || q.includes("score") || q.includes("marks"));
  const isLowestMarks =
    (q.includes("lowest") ||
      q.includes("least marks") ||
      q.includes("minimum marks")) &&
    (q.includes("mark") || q.includes("score") || q.includes("marks"));

  const isHighestSkills =
    (q.includes("highest") || q.includes("most") || q.includes("top")) &&
    (q.includes("skill") || q.includes("skills")) &&
    !isHighestMarks; // guard: don't double-match "highest marks"

  const isLowestSkills =
    (q.includes("lowest") || q.includes("least") || q.includes("fewest")) &&
    (q.includes("skill") || q.includes("skills")) &&
    !isLowestMarks;
  // 🔥 MULTI-FILTER FOLLOW-UP SUPPORT

  let filtered = filterCandidates(analysis.entities, analysis.filters || {});
  const positionMatch = question
    .toLowerCase()
    .match(
      /\b(first|second|third|fourth|fifth|sixth|seventh|eighth|ninth|tenth|eleventh|twelfth|thirteenth|fourteenth|fifteenth|sixteenth|seventeenth|eighteenth|nineteenth|twentieth|\d+(st|nd|rd|th))\b/,
    );

  let positionIndex = null;

  if (positionMatch) {
    const map = {
      first: 0,
      "1st": 0,
      second: 1,
      "2nd": 1,
      third: 2,
      "3rd": 2,
      fourth: 3,
      "4th": 3,
      fifth: 4,
      "5th": 4,
      sixth: 5,
      "6th": 5,
      seventh: 6,
      "7th": 6,
      eighth: 7,
      "8th": 7,
      ninth: 8,
      "9th": 8,
      tenth: 9,
      "10th": 9,
      eleventh: 10,
      "11th": 10,
      twelfth: 11,
      "12th": 11,
      thirteenth: 12,
      "13th": 12,
      fourteenth: 13,
      "14th": 13,
      fifteenth: 14,
      "15th": 14,
      sixteenth: 15,
      "16th": 15,
      seventeenth: 16,
      "17th": 16,
      eighteenth: 17,
      "18th": 17,
      nineteenth: 18,
      "19th": 18,
      twentieth: 19,
      "20th": 19,
    };

    const word = positionMatch[1];

    if (map[word] !== undefined) {
      positionIndex = map[word];
    } else {
      positionIndex = parseInt(word) - 1;
    }
  }
  log.info(
    `[Step 2] Candidates after filter: ${filtered.length} | isHighestMarks: ${isHighestMarks} | isLowestMarks: ${isLowestMarks} | isHighestSkills: ${isHighestSkills} | isLowestSkills: ${isLowestSkills}`,
  );

  const userLimit =
    analysis.limit != null &&
    Number.isInteger(analysis.limit) &&
    analysis.limit > 0
      ? analysis.limit
      : null;

  const effectiveCap = userLimit
    ? Math.min(userLimit, MAX_CANDIDATES_TO_LLM)
    : MAX_CANDIDATES_TO_LLM;

  const wasFilterOnlyQuery =
    !analysis.entities?.candidateName &&
    (analysis.filters?.email || analysis.filters?.phoneNumber);

  if (wasFilterOnlyQuery && filtered.length === 1) {
    const resolvedName = filtered[0].FullName;
    session.contextMemory.candidateName = resolvedName;
    session.contextMemory.mode = "SINGLE";
    log.info(
      `[ContextUpdate] Email/phone query resolved to → "${resolvedName}"`,
    );
  }
  if (wasFilterOnlyQuery && filtered.length > 1) {
    session.contextMemory.candidateName = null;
    session.contextMemory.mode = "LIST";
  }

  if (
    !isHighestSkills &&
    !isLowestSkills &&
    !isHighestMarks &&
    !isLowestMarks
  ) {
    filtered = rankCandidates(filtered, analysis);
  }

  if (filtered.length === 0) {
    const label = buildFilterLabel(analysis.entities, analysis.filters || {});
    const msg = analysis.entities?.candidateName
      ? `I couldn't find any candidate named "${analysis.entities.candidateName}". Please check the name and try again.`
      : `No candidates found ${label}.`;

    if (onChunk) await onChunk(msg);
    return {
      success: true,
      answer: onChunk ? null : msg,
      dataframe: [],
      cachedAt: candidateStore.cachedAt,
    };
  }

  let sortedFiltered = filtered;

  if (isHighestMarks || isLowestMarks) {
    const getBestMarks = (candidate) => candidate._meta?.bestMarks ?? -1;
    sortedFiltered = [...filtered].sort((a, b) => {
      const marksA = getBestMarks(a);
      const marksB = getBestMarks(b);
      return isHighestMarks ? marksB - marksA : marksA - marksB;
    });
    sortedFiltered = sortedFiltered.filter((c) => getBestMarks(c) >= 0);
    log.info(
      `[Marks sort] ${isHighestMarks ? "Highest" : "Lowest"} — ${sortedFiltered.length} candidates`,
    );
  } else if (isHighestSkills || isLowestSkills) {
    const getSkillCount = (c) => (c.Skills || []).length;
    sortedFiltered = [...filtered].sort((a, b) =>
      isHighestSkills
        ? getSkillCount(b) - getSkillCount(a)
        : getSkillCount(a) - getSkillCount(b),
    );
    log.info(
      `[Skills sort] ${isHighestSkills ? "Highest" : "Lowest"} — top candidate has ${(sortedFiltered[0]?.Skills || []).length} skills`,
    );
  }
  // ── INTERVIEW COUNT FAST-PATH ─────────────────────────────────────────────
  const isInterviewCountQuery =
    /interview\s*(count|counts|number|total)/i.test(question) ||
    /number\s+of\s+interviews?/i.test(question) ||
    /(total|count)\s+.*interview/i.test(question) ||
    /interview.*per\s*(candidate|person)/i.test(question) ||
    /candidate.*with.*interview\s*count/i.test(question);

  if (isInterviewCountQuery) {
    const lines = sortedFiltered
      .map((c) => {
        const allInterviews = (c.Applications || []).reduce(
          (arr, app) =>
            arr.concat(Array.isArray(app.Interviews) ? app.Interviews : []),
          [],
        );
        const totalCount = allInterviews.length;
        const uniqueJobs = new Set(
          (c.Applications || [])
            .filter(
              (app) =>
                Array.isArray(app.Interviews) && app.Interviews.length > 0,
            )
            .map((app) => app.JobTitle)
            .filter(Boolean),
        );
        return { name: c.FullName, count: totalCount, jobs: uniqueJobs.size };
      })
      .sort((a, b) => b.count - a.count);

    const withInterviews = lines.filter((l) => l.count > 0);
    const noInterviews = lines.filter((l) => l.count === 0);

    let answer = `Here are all **${lines.length} candidates** with their interview counts:\n\n`;
    if (withInterviews.length > 0) {
      answer += withInterviews
        .map(
          (l, i) =>
            `${i + 1}. **${l.name}** — ${l.count} interview${l.count !== 1 ? "s" : ""}` +
            (l.jobs > 1 ? ` across ${l.jobs} jobs` : ""),
        )
        .join("\n");
    }
    if (noInterviews.length > 0) {
      answer += `\n\n**${noInterviews.length} candidate${noInterviews.length !== 1 ? "s" : ""}** have no interviews recorded.`;
    }
  }
  // ── SKILLS RANK FAST-PATH ─────────────────────────────────────────────────
  if (isHighestSkills || isLowestSkills) {
    const displayLimit = userLimit ?? 10;
    const top = sortedFiltered.slice(0, displayLimit);

    const lines = top.map((c, i) => {
      const skills = (c.Skills || []).map((s) => s.Skill).filter(Boolean);
      const skillCount = skills.length;
      const preview =
        skills.slice(0, 5).join(", ") +
        (skills.length > 5 ? ` +${skills.length - 5} more` : "");
      return `${i + 1}. **${c.FullName}** — ${skillCount} skill${skillCount !== 1 ? "s" : ""}${skillCount > 0 ? `: ${preview}` : ""}`;
    });

    const answer =
      `Top **${top.length}** candidates by skill count${isLowestSkills ? " (fewest first)" : ""}:\n\n` +
      lines.join("\n") +
      (sortedFiltered.length > displayLimit
        ? `\n\n*Showing ${top.length} of ${sortedFiltered.length} candidates.*`
        : "");

    updateSessionContext(session, analysis, resolved);
    if (onChunk) await onChunk(answer);
    return {
      success: true,
      type: "CACHE",
      answer: onChunk ? null : answer,
      dataframe: top,
      cachedAt: candidateStore.cachedAt,
      totalFound: sortedFiltered.length,
    };
  }
  // ── END SKILLS RANK FAST-PATH ─────────────────────────────────────────────
  // ── END SKILLS RANK FAST-PATH ─────────────────────────────────────────────

  // ── INTERVIEW + FEEDBACK FAST-PATH ───────────────────────────────────────
  const isInterviewFeedbackQuery =
    (q.includes("interview") && q.includes("feedback")) ||
    (q.includes("interview") && q.includes("detail")) ||
    (q.includes("interview") && q.includes("result")) ||
    (q.includes("interview") && q.includes("status") && q.includes("feedback"));
  // ── INTERVIEWER NAME FAST-PATH ────────────────────────────────────────────
  const isInterviewerNameQuery =
    /who\s+(is|are)\s+(taking|conducting|doing|handling)\s+(the\s+)?interview/i.test(
      question,
    ) ||
    /interview\s+(taken|conducted|done|handled)\s+by\s+whom/i.test(question) ||
    /who\s+(took|conducted|handled|did)\s+(the\s+)?interview/i.test(question) ||
    /interviewer\s+(name|names|list|who)/i.test(question) ||
    /who\s+interviewed/i.test(question);

  if (isInterviewerNameQuery) {
    const displayLimit = userLimit ?? 20;

    // Collect all unique interviewers, optionally filtered by jobTitle
    const interviewerMap = new Map(); // interviewer name → Set of job titles

    for (const c of sortedFiltered) {
      for (const app of c.Applications || []) {
        // If jobTitle filter is active, only include matching apps
        if (analysis.entities?.jobTitle) {
          const jobMatch = (app.JobTitle || "")
            .toLowerCase()
            .includes(analysis.entities.jobTitle.toLowerCase());
          if (!jobMatch) continue;
        }

        for (const iv of Array.isArray(app.Interviews) ? app.Interviews : []) {
          const interviewer = (iv.Interviewer || "").trim();
          if (!interviewer) continue;

          if (!interviewerMap.has(interviewer)) {
            interviewerMap.set(interviewer, new Set());
          }
          if (app.JobTitle) {
            interviewerMap.get(interviewer).add(app.JobTitle);
          }
        }
      }
    }

    if (interviewerMap.size === 0) {
      const jobLabel = analysis.entities?.jobTitle
        ? ` for the "${analysis.entities.jobTitle}" job`
        : "";
      const msg = `No interviewers found${jobLabel}.`;
      updateSessionContext(session, analysis, resolved);
      if (onChunk) await onChunk(msg);
      return {
        success: true,
        type: "CACHE",
        answer: onChunk ? null : msg,
        dataframe: [],
      };
    }

    const jobLabel = analysis.entities?.jobTitle
      ? ` for the **"${analysis.entities.jobTitle}"** job`
      : "";

    let answer = `Here are the **${interviewerMap.size} interviewer${interviewerMap.size !== 1 ? "s" : ""}** taking interviews${jobLabel}:\n\n`;

    let i = 1;
    for (const [name, jobs] of interviewerMap) {
      const jobList = [...jobs];
      const jobStr =
        jobList.length > 0
          ? ` — interviewing for: ${jobList.slice(0, 3).join(", ")}${jobList.length > 3 ? ` +${jobList.length - 3} more` : ""}`
          : "";
      answer += `${i}. **${name}**${jobStr}\n`;
      i++;
      if (i > displayLimit) break;
    }

    updateSessionContext(session, analysis, resolved);

    if (onChunk) await onChunk(answer);
    return {
      success: true,
      type: "CACHE",
      answer: onChunk ? null : answer,
      dataframe: sortedFiltered,
      cachedAt: candidateStore.cachedAt,
      totalFound: sortedFiltered.length,
    };
  }
  // ── END INTERVIEWER NAME FAST-PATH ────────────────────────────────────────
  if (isInterviewFeedbackQuery) {
    const displayLimit = userLimit ?? 5;
    const candidates = sortedFiltered.filter((c) =>
      (c.Applications || []).some(
        (app) => Array.isArray(app.Interviews) && app.Interviews.length > 0,
      ),
    );
    const top = candidates.slice(0, displayLimit);

    if (top.length === 0) {
      const msg = "No candidates with interview records found.";
      updateSessionContext(session, analysis, resolved);
      if (onChunk) await onChunk(msg);
      return {
        success: true,
        type: "CACHE",
        answer: onChunk ? null : msg,
        dataframe: [],
      };
    }

    const lines = top.map((c, idx) => {
      const interviews = (c.Applications || []).flatMap((app) =>
        (Array.isArray(app.Interviews) ? app.Interviews : []).map((iv) => ({
          ...iv,
          JobTitle: app.JobTitle,
        })),
      );

      // Deduplicate by feedback content to avoid showing 150 identical entries
      const seen = new Set();
      const uniqueInterviews = interviews.filter((iv) => {
        const key = `${iv.JobTitle}||${iv.InterviewStatus}||${iv.Interviewer}||${(iv.feedback || "").trim().slice(0, 60)}`;
        if (seen.has(key)) return false;
        seen.add(key);
        return true;
      });

      const ivLines = uniqueInterviews.slice(0, 5).map((iv) => {
        const parts = [];
        if (iv.JobTitle) parts.push(`Job: **${iv.JobTitle}**`);
        if (iv.InterviewStatus) parts.push(`Status: ${iv.InterviewStatus}`);
        if (iv.Interviewer) parts.push(`Interviewer: ${iv.Interviewer}`);
        if (iv.isQualified != null)
          parts.push(`Qualified: ${iv.isQualified ? "Yes" : "No"}`);
        if (iv.marksObtained != null)
          parts.push(`Marks: ${iv.marksObtained}/${iv.totalMarks ?? "?"}`);
        if (iv.feedback?.trim()) {
          // Trim long auto-generated feedback to one clean line
          const fb = iv.feedback.trim().replace(/\s+/g, " ");
          const short = fb.length > 120 ? fb.slice(0, 120) + "..." : fb;
          parts.push(`Feedback: "${short}"`);
        }
        return `  - ${parts.join(" | ")}`;
      });

      const more =
        uniqueInterviews.length > 5
          ? `\n  *(+${uniqueInterviews.length - 5} more interviews)*`
          : "";

      return `**${idx + 1}. ${c.FullName}**\n${ivLines.join("\n")}${more}`;
    });

    const header = `Here are **${top.length}** candidate${top.length !== 1 ? "s" : ""} with their interview details and feedback:\n\n`;
    const footer =
      candidates.length > displayLimit
        ? `\n\n*Showing ${top.length} of ${candidates.length} candidates with interviews.*`
        : "";

    const answer = header + lines.join("\n\n") + footer;

    updateSessionContext(session, analysis, resolved);
    if (onChunk) await onChunk(answer);
    return {
      success: true,
      type: "CACHE",
      answer: onChunk ? null : answer,
      dataframe: top,
      cachedAt: candidateStore.cachedAt,
      totalFound: candidates.length,
    };
  }
  // ── END INTERVIEW + FEEDBACK FAST-PATH ───────────────────────────────────
  // ── END INTERVIEW + FEEDBACK FAST-PATH ───────────────────────────────────
  // Step 3: project
  const isSingleCandidate = !!(
    analysis.entities?.candidateName &&
    analysis.entities.candidateName.trim() !== ""
  );

  const sections = isSingleCandidate
    ? finalSections === "ALL" || !Array.isArray(finalSections)
      ? ALL_SECTIONS
      : finalSections
    : getSummarySections(analysis.filters || {}, finalSections);

  // const effectiveCap = userLimit
  //   ? Math.min(userLimit, MAX_CANDIDATES_TO_LLM)
  //   : MAX_CANDIDATES_TO_LLM;

  let slicedForLLM;

  if (positionIndex !== null) {
    slicedForLLM = sortedFiltered.slice(positionIndex, positionIndex + 1);
  } else {
    slicedForLLM = sortedFiltered.slice(0, effectiveCap);
  }
  const projected = projectData(slicedForLLM, sections);

  // 🔥 set context if single result (like "show first one")
  if (projected.length === 1 && !analysis.entities?.candidateName) {
    const name = projected[0].FullName;

    session.contextMemory.candidateName = name;
    session.contextMemory.mode = "SINGLE";

    session.lastResolvedContext = {
      candidateName: name,
    };

    log.info(`[ContextSet] Single result → ${name}`);
  }
  function resolveResponseType({ question, projected, filtered, sections }) {
    const isSingleCandidateFinal = projected.length === 1;
    const totalFoundFinal = filtered.length;

    const hasSkills = sections.includes("Skills");
    const hasEducation = sections.includes("Education");
    const hasProfile = sections.includes("Profile");
    const hasExp = sections.includes("Experience");
    const hasDocuments = sections.includes("Documents");
    const hasResumes = sections.includes("Resumes");
    const hasApps = sections.includes("Applications");

    const contentSections = [
      hasSkills,
      hasEducation,
      hasProfile,
      hasExp,
      hasDocuments,
      hasResumes,
      hasApps,
    ].filter(Boolean).length;

    let responseType = detectResponseType(
      question,
      isSingleCandidateFinal,
      totalFoundFinal,
    );

    if (detectNameOnlyRequest(question)) {
      return "NAME_LIST";
    }
    if (contentSections >= 2) {
      responseType = "MULTI_SECTION";
    } else if (hasSkills) {
      responseType = "WITH_SKILLS";
    } else if (hasEducation) {
      responseType = "WITH_EDUCATION";
    } else if (hasProfile) {
      responseType = "WITH_PROFILE";
    } else if (hasExp) {
      responseType = "WITH_EXPERIENCE";
    }
    return refineResponseType({
      responseType,
      question,
      projected,
      contentSections,
    });
  }

  // 🔥 RESPONSE TYPE (CORRECT PLACE)
  const responseType = resolveResponseType({
    question,
    projected,
    filtered,
    sections,
  });

  const mappedResponse = handleResponseType(
    responseType,
    projected,
    question,
    analysis,
  );

  if (mappedResponse) {
    updateSessionContext(session, analysis, resolved);
    return {
      ...mappedResponse,
      type: "CACHE",
      isSingleCandidate: projected.length === 1,
      entities: analysis.entities,
    };
  }
  if (analysis.entities?.candidateName) {
    session.lastResolvedContext = {
      candidateName: analysis.entities.candidateName,
      jobTitle:
        analysis.entities.jobTitle ?? session.lastResolvedContext.jobTitle,
    };
  }

  log.info(`[ResponseType] ${responseType}`);

  // ✅ YES / NO (Studying)
  const answer = await generateNaturalAnswer(
    question,
    `${SP_NAME} (cached at ${candidateStore.cachedAt.toLocaleString()})`,
    projected,
    {
      userLimit,
      totalFound:
        isHighestMarks || isLowestMarks
          ? sortedFiltered.length
          : filtered.length,
      isSingleCandidate,
      sectionsNeeded: sections,
      responseType: responseType,
    },
    onChunk,
  );
  //   if (analysis.entities?.candidateName) {
  //   lastTopic = null; // reset after applying
  // }
  addToMemory(session, "assistant", answer);

  return {
    success: true,
    type: "CACHE",
    answer,
    dataframe: projected,
    cachedAt: candidateStore.cachedAt,
    totalFound:
      isHighestMarks || isLowestMarks ? sortedFiltered.length : filtered.length,
    shownToLLM: projected.length,
    userLimit: userLimit ?? null,
    sectionsUsed: sections,
    entities: analysis.entities,
    isSingleCandidate,
  };
}

export async function forceRefresh() {
  log.info("Manual cache refresh triggered");
  await loadFromSp();
  lastRefreshError = null;
  return getCacheStatus();
}

export function getCacheStatus() {
  if (!candidateStore) {
    return {
      status: "cold",
      message: "Cache not loaded",
      lastRefreshError,
    };
  }
  const ageMs = Date.now() - candidateStore.cachedAt.getTime();
  return {
    status: ageMs > CACHE_TTL_MS ? "stale" : "warm",
    cachedAt: candidateStore.cachedAt,
    ageMinutes: Math.floor(ageMs / 60000),
    totalCandidates: candidateStore.candidates.length,
    ttlMinutes: CACHE_TTL_MS / 60000,
    lastRefreshError,
  };
}

export function clearCache() {
  candidateStore = null;
  candidateIndex = null;
  lastResolvedContext = {};
  analysisCache.clear();
  sessions.clear();
  if (refreshTimer) {
    clearInterval(refreshTimer);
    refreshTimer = null;
  }
  log.info("Candidate cache cleared");
}

export function resetConversationContext(sessionId = "default") {
  sessions.delete(sessionId);
  log.info(`Conversation context reset for session: ${sessionId}`);
}

//  SP LOADER

async function loadFromSp() {
  const pool = await getPool();
  const result = await pool.request().execute(SP_NAME);
  const rows = result.recordset || [];

  if (!rows || rows.length === 0) {
    log.warn("SP returned 0 rows");
  }

  //  always define parsed properly
  const parsedCandidates = rows.map(parseRow);

  //  assign store
  candidateStore = {
    cachedAt: new Date(),
    candidates: parsedCandidates,
  };

  //  build index
  candidateIndex = buildIndex(parsedCandidates);
  log.info(`SP load complete — ${parsedCandidates.length} candidates`);
}

function detectGreeting(question) {
  if (!question) return false;

  const q = question.toLowerCase().trim();

  const greetings = [
    "hi",
    "hello",
    "hey",
    "hii",
    "hiii",
    "heyy",
    "heyyy",

    "yo",
    "sup",
    "wassup",
    "what's up",
    "whats up",
    "watsup",
    "sup bro",
    "hey bro",
    "hi bro",
    "hello bro",

    "good morning",
    "good afternoon",
    "good evening",
    "good day",
    "greetings",

    "how are you",
    "how are you doing",
    "how's it going",
    "how is it going",
    "how do you do",

    "namaste",
    "namaskar",
    "hello ji",
    "hi ji",
    "kaise ho",
    "kaise ho bhai",
    "kya haal hai",
    "kya haal",
    "ram ram",
    "pranam",

    "hi there",
    "hey there",
    "hello there",

    "hlo",
    "hlw",
    "helo",
    "helooo",

    "hey buddy",
    "hi buddy",
    "hello buddy",
    "hey dude",
    "hi dude",
  ];

  return greetings.some((g) => {
    if (g.split(" ").length > 1) {
      return q.includes(g); // multi-word phrases are fine
    }
    const regex = new RegExp(`\\b${g}\\b`);
    return regex.test(q); // single word must match as whole word
  });
}

function detectNameOnlyRequest(question) {
  const q = question.toLowerCase().trim();

  return (
    /only\s+name/.test(q) ||
    /names?\s+only/.test(q) ||
    /just\s+(the\s+)?names?/.test(q) ||
    /give\s+(me\s+)?(only\s+)?names?/.test(q) ||
    /list\s+(of\s+)?names?/.test(q) ||
    /candidate\s+names?/.test(q) ||
    /names?\s+of\s+candidates?/.test(q) ||
    /name\s+of\s+candidate/.test(q) ||
    /who\s+are\s+the\s+candidates/.test(q)
  );
}
function getGreetingResponse() {
  const responses = [
    "Hi 👋 How can I help you today?",
    "Hello! 😊 What would you like to know?",
    "Hey there! How can I assist you?",
    "Hi! Ask me anything about candidates, jobs, or interviews.",
  ];

  return responses[Math.floor(Math.random() * responses.length)];
}

function detectSmallTalk(q) {
  const text = q.toLowerCase();

  if (text.includes("thank")) return "You're welcome 😊";
  if (text.includes("bye")) return "Goodbye! 👋";
  if (text === "ok") return "👍";

  return null;
}

function calculateExperienceYears(experience = []) {
  let totalMonths = 0;

  for (const exp of experience) {
    if (!exp.startDate) continue;

    const start = new Date(exp.startDate);
    const end = exp.isCurrentCompany
      ? new Date()
      : exp.endDate
        ? new Date(exp.endDate)
        : null;

    if (!end) continue;

    const months =
      (end.getFullYear() - start.getFullYear()) * 12 +
      (end.getMonth() - start.getMonth());

    if (!isNaN(months) && months > 0) totalMonths += months;
  }

  return +(totalMonths / 12).toFixed(1);
}

function matchApplicationWithInterview(candidate, filters, entities) {
  const apps = candidate.Applications || [];

  return apps.some((app) => {
    // Match job title
    if (entities.jobTitle) {
      if (
        !app.JobTitle ||
        app.JobTitle.toLowerCase() !== entities.jobTitle.toLowerCase()
      ) {
        return false;
      }
    }

    // Match interview filters INSIDE SAME APP
    if (filters.marksObtained != null) {
      if (!Array.isArray(app.Interviews)) return false;

      const matchInterview = app.Interviews.some((i) => {
        const marks = parseFloat(i.marksObtained);
        if (isNaN(marks)) return false;

        switch (filters.marksOperator) {
          case "gt":
            return marks > filters.marksObtained;
          case "gte":
            return marks >= filters.marksObtained;
          case "lt":
            return marks < filters.marksObtained;
          case "lte":
            return marks <= filters.marksObtained;
          case "eq":
            return marks === filters.marksObtained;
          default:
            return false;
        }
      });

      if (!matchInterview) return false;
    }

    return true;
  });
}

function getBestMarks(applications = []) {
  let best = -1;

  for (const app of applications) {
    if (!Array.isArray(app.Interviews)) continue;

    for (const i of app.Interviews) {
      const m = parseFloat(i.marksObtained);
      if (!isNaN(m) && m > best) best = m;
    }
  }

  return best >= 0 ? best : null;
}

function hasAnyInterview(applications = []) {
  return applications.some(
    (app) => Array.isArray(app.Interviews) && app.Interviews.length > 0,
  );
}

function parseRow(row) {
  const parsed = {
    CandidateId: row.CandidateId,
    FullName: row.FullName,
    email: row.email,
    phoneNumber: row.phoneNumber,
    isVerified: row.isVerified,
    isProfileComplete: row.isProfileComplete,
    RegisteredOn: row.RegisteredOn,
    Profile: safeParseJson(row.Profile, {}),
    Skills: safeParseJson(row.Skills, []),
    Education: safeParseJson(row.Education, []),
    Experience: safeParseJson(row.Experience, []),
    Applications: safeParseJson(row.Applications, []),
    Documents: safeParseJson(row.Documents, {}),
    Resumes: safeParseJson(row.Resumes, []),
  };

  parsed._meta = {
    experienceYears: calculateExperienceYears(parsed.Experience),
    bestMarks: getBestMarks(parsed.Applications),
    hasInterview: hasAnyInterview(parsed.Applications),
  };

  return parsed;
}

function normalizeSkill(str) {
  return str.toLowerCase().replace(/[.\s]/g, "");
}

function buildIndex(candidates) {
  const index = {
    byName: new Map(),
    byCity: new Map(),
    byState: new Map(),
    bySkill: new Map(),
    byJobTitle: new Map(),
  };

  for (const c of candidates) {
    // NAME
    const name = (c.FullName || "").toLowerCase().trim();
    if (name) {
      index.byName.set(name, c);
    }

    // CITY
    const city = (c.Profile?.city || "").toLowerCase().trim();
    if (city) {
      if (!index.byCity.has(city)) index.byCity.set(city, []);
      index.byCity.get(city).push(c);
    }

    // STATE
    const state = (c.Profile?.state || "").toLowerCase().trim();
    if (state) {
      if (!index.byState.has(state)) index.byState.set(state, []);
      index.byState.get(state).push(c);
    }

    // SKILLS
    for (const s of c.Skills || []) {
      const skill = normalizeSkill(s.Skill);
      if (!skill) continue;

      if (!index.bySkill.has(skill)) index.bySkill.set(skill, []);
      index.bySkill.get(skill).push(c);
    }

    // JOB TITLE
    for (const app of c.Applications || []) {
      const job = (app.JobTitle || "").toLowerCase().trim();
      if (!job) continue;

      if (!index.byJobTitle.has(job)) index.byJobTitle.set(job, []);
      index.byJobTitle.get(job).push(c);
    }
  }
  console.log("Index built. Total skills:", index.bySkill.size);
  return index;
}

function fuzzyMatchName(input, candidates) {
  if (!input) return [];

  const query = input.toLowerCase().trim();
  const matches = [];

  for (const c of candidates) {
    const name = (c.FullName || "").toLowerCase();

    let score = 0;

    // exact match (highest priority)
    if (name === query) {
      return [{ candidate: c, score: 1 }];
    }

    // partial match
    if (name.includes(query)) {
      score = query.length / name.length;
    }

    // word match
    const words = name.split(" ");
    for (const w of words) {
      if (w.startsWith(query)) {
        score = Math.max(score, query.length / w.length);
      }
    }

    if (score > 0.3) {
      matches.push({ candidate: c, score });
    }
  }

  // sort best first
  matches.sort((a, b) => b.score - a.score);

  return matches.slice(0, 5); // top 5 matches
}

function rankCandidates(candidates, analysis) {
  return candidates
    .map((c) => {
      let score = 0;

      // 🔥 SKILL MATCH (high weight)
      if (analysis.entities?.skill && c.Skills) {
        const skill = analysis.entities.skill.toLowerCase();
        const hasSkill = c.Skills.some((s) =>
          s.Skill?.toLowerCase().includes(skill),
        );
        if (hasSkill) score += 50;
      }

      // 🔥 EXPERIENCE MATCH
      if (analysis.filters?.experienceYears != null) {
        const exp = c._meta?.experienceYears || 0;
        const target = analysis.filters.experienceYears;

        if (exp >= target) score += 30;
      }

      // 🔥 LOCATION MATCH
      if (analysis.entities?.city) {
        const city = c.Profile?.city?.toLowerCase();
        if (city === analysis.entities.city.toLowerCase()) {
          score += 20;
        }
      }

      // 🔥 INTERVIEW MARKS
      if (c._meta?.bestMarks != null) {
        score += c._meta.bestMarks / 10; // normalize
      }

      // 🔥 VERIFIED BONUS
      if (c.isVerified) {
        score += 5;
      }

      return { candidate: c, score };
    })
    .sort((a, b) => b.score - a.score)
    .map((x) => x.candidate);
}

function safeParseJson(value, fallback) {
  if (!value) return fallback;
  if (typeof value === "object") return value;
  try {
    return JSON.parse(value);
  } catch {
    return fallback;
  }
}

function scheduleAutoRefresh() {
  if (refreshTimer) clearInterval(refreshTimer);
  refreshTimer = setInterval(async () => {
    log.info("Auto-refresh — reloading candidate data...");
    try {
      await loadFromSp();
      lastRefreshError = null;
      log.info("Auto-refresh complete");
    } catch (err) {
      lastRefreshError = err.message;
      log.error(`Auto-refresh failed: ${err.message}`);
    }
  }, CACHE_TTL_MS);
}

function fixMisclassifiedIntent(analysis, question) {
  const q = question.toLowerCase();

  const birthdatePatterns = [
    /birth\s*date/i,
    /date\s*of\s*birth/i,
    /dob/i,
    /born\s*(in|between|after|before|from)/i,
    /age\s*(between|from|above|below|under|over)/i,
    /born\s*\d{4}/i,
  ];
  const hasBirthdatePattern = birthdatePatterns.some((r) => r.test(question));
  const yearRangeMatch = question.match(
    /(?:between|from)\s+(\d{4})\s+(?:to|and|-)\s+(\d{4})/i,
  );

  if (hasBirthdatePattern || (yearRangeMatch && q.includes("birth"))) {
    if (analysis.intent === "OOS" || analysis.intent === "PDF") {
      analysis.intent = "CANDIDATE";
      log.info(`[IntentFix] Birthdate query overridden → CANDIDATE`);
    }
    if (
      !Array.isArray(analysis.sectionsNeeded) ||
      !analysis.sectionsNeeded.includes("Profile")
    ) {
      analysis.sectionsNeeded = ["root", "Profile"];
    }
    if (yearRangeMatch) {
      const y1 = parseInt(yearRangeMatch[1], 10);
      const y2 = parseInt(yearRangeMatch[2], 10);
      analysis.filters.birthYearFrom = Math.min(y1, y2);
      analysis.filters.birthYearTo = Math.max(y1, y2);
      analysis.filters.birthYear = null;
      log.info(
        `[IntentFix] birthYearFrom: ${analysis.filters.birthYearFrom}, birthYearTo: ${analysis.filters.birthYearTo}`,
      );
    } else {
      const allYears = [...question.matchAll(/(19|20)\d{2}/g)].map((m) =>
        parseInt(m[0], 10),
      );
      if (allYears.length === 1 && !analysis.filters.birthYear) {
        analysis.filters.birthYear = allYears[0];
        analysis.filters.birthYearFrom = null;
        analysis.filters.birthYearTo = null;
        log.info(`[IntentFix] birthYear: ${analysis.filters.birthYear}`);
      }
    }
  }

  //  Graduation year
  const gradPatterns = [
    /complet\w*\s+(?:their\s+)?study/i,
    /graduat\w*\s+in\s+\d{4}/i,
    /passed\s+out\s+in\s+\d{4}/i,
    /finish\w*\s+(?:their\s+)?(?:study|education|degree)\s+in\s+\d{4}/i,
  ];
  if (gradPatterns.some((r) => r.test(question))) {
    if (analysis.intent === "OOS" || analysis.intent === "PDF") {
      analysis.intent = "CANDIDATE";
      analysis.sectionsNeeded = ["root", "Profile", "Education"];
      log.info(`[IntentFix] Graduation year query overridden → CANDIDATE`);
    }
    const yearMatch = question.match(/(19|20)\d{2}/);
    if (yearMatch && !analysis.filters.ugGraduationYear) {
      analysis.filters.ugGraduationYear = parseInt(yearMatch[0], 10);
      analysis.filters.isStudying = null;
      log.info(
        `[IntentFix] ugGraduationYear: ${analysis.filters.ugGraduationYear}`,
      );
    }
  }

  //  PDF
  const pdfKeywords = [
    "recruitment process",
    "recruitment management",
    "recruitment policy",
    "hiring process",
    "hiring policy",
    "hiring procedure",
    "interview process",
    "interview policy",
    "interview procedure",
    "onboarding process",
    "onboarding policy",
    "pre-boarding",
    "pre boarding",
    "selection process",
    "selection criteria",
    "selection policy",
    "online communication training",
    "communication training",
    "hr policy",
    "hr process",
    "hr procedure",
    "hr guideline",
    "what is recruitment",
    "how does recruitment",
    "explain recruitment",
    "what is hiring",
    "how does hiring",
    "explain hiring",
    "what is interview",
    "how does interview",
    "explain interview",
    "what is onboarding",
    "what is pre-boarding",
  ];
  const qLower = question.toLowerCase();
  const isPdfQuestion = pdfKeywords.some((kw) => qLower.includes(kw));
  if (analysis.intent === "OOS" && isPdfQuestion) {
    analysis.intent = "PDF";
    log.info(`[IntentFix] PDF policy question overridden from OOS → PDF`);
  }

  const jobTitlePattern = question.match(
    /(?:^|\bin\b|\bfor\b|\bapplied\s+(?:in|for)\b)\s+([A-Za-z0-9 ]+?)\s+\bjob\b/i,
  );
  if (jobTitlePattern) {
    const extractedJobTitle = jobTitlePattern[1].trim();

    const skipWords = new Set(["a", "the", "this", "that", "any", "some"]);
    if (
      extractedJobTitle.length > 0 &&
      !skipWords.has(extractedJobTitle.toLowerCase())
    ) {
      const cityVal = (analysis.entities.city || "").toLowerCase();
      const stateVal = (analysis.entities.state || "").toLowerCase();
      const extracted = extractedJobTitle.toLowerCase();

      if (cityVal === extracted || stateVal === extracted) {
        log.info(
          `[IntentFix] "in/for X job" — moving "${extractedJobTitle}" from city/state → jobTitle`,
        );
        if (cityVal === extracted) analysis.entities.city = null;
        if (stateVal === extracted) analysis.entities.state = null;
      }

      if (
        !analysis.entities.jobTitle ||
        analysis.entities.jobTitle.toLowerCase() === "job" ||
        analysis.entities.jobTitle.toLowerCase() === extracted
      ) {
        analysis.entities.jobTitle = extractedJobTitle;
        log.info(
          `[IntentFix] jobTitle set to "${extractedJobTitle}" from "in/for X job" pattern`,
        );
      }
      if (Array.isArray(analysis.sectionsNeeded)) {
        if (!analysis.sectionsNeeded.includes("Applications")) {
          analysis.sectionsNeeded.push("Applications");
        }
      }
    }
  }

  const jobPrefixPattern = question.match(
    /^([A-Za-z0-9 ]+?)\s+\bjob\b(?:\s+(?:candidates?|wale|applicants?|people))?/i,
  );
  if (jobPrefixPattern && !jobTitlePattern) {
    const extracted = jobPrefixPattern[1].trim();
    const skipStarters = new Set([
      "candidate",
      "candidates",
      "show",
      "list",
      "find",
      "get",
      "all",
      "the",
      "a",
    ]);
    if (extracted.length > 0 && !skipStarters.has(extracted.toLowerCase())) {
      const cityVal = (analysis.entities.city || "").toLowerCase();
      if (cityVal === extracted.toLowerCase()) {
        analysis.entities.city = null;
        log.info(
          `[IntentFix] "X job" prefix — clearing wrong city "${extracted}"`,
        );
      }
      if (
        !analysis.entities.jobTitle ||
        analysis.entities.jobTitle.toLowerCase() === "job"
      ) {
        analysis.entities.jobTitle = extracted;
        log.info(`[IntentFix] "X job" prefix — jobTitle set to "${extracted}"`);
      }
    }
  }
  //  Interviewer
  const interviewerMatch = question.match(
    /(?:interview\w*\s+(?:taken\s+)?by|interviewed\s+by|interviewer\s+(?:is\s+)?)\s+(.+?)(?:\s*$|\s*\?)/i,
  );
  if (interviewerMatch) {
    if (analysis.intent === "OOS" || analysis.intent === "PDF") {
      analysis.intent = "CANDIDATE";
      analysis.sectionsNeeded = [
        "root",
        "Applications",
        "Applications.Interviews",
      ];
      log.info(`[IntentFix] Interviewer query overridden → CANDIDATE`);
    }
    if (!analysis.filters.interviewer) {
      analysis.filters.interviewer = interviewerMatch[1].trim();
      analysis.filters.hasInterview = true;
      log.info(`[IntentFix] interviewer: "${analysis.filters.interviewer}"`);
    }
  }

  if (
    /interview\s*(count|counts|number|total)/i.test(question) ||
    /number\s+of\s+interviews?/i.test(question) ||
    /(total|count)\s+.*interview/i.test(question)
  ) {
    // Do NOT set hasInterview:true here — user wants counts for ALL candidates, including 0
    // Just ensure the right sections are loaded
    analysis.filters.hasInterview = null;
    if (
      !Array.isArray(analysis.sectionsNeeded) ||
      !analysis.sectionsNeeded.includes("Applications.Interviews")
    ) {
      analysis.sectionsNeeded = [
        "root",
        "Applications",
        "Applications.Interviews",
      ];
    }
    log.info(
      `[IntentFix] interview count pattern → sections set, hasInterview cleared`,
    );
  }
  if (
    /who\s+(is|are)\s+(taking|conducting|doing|handling)\s+(the\s+)?interview/i.test(
      question,
    ) ||
    /who\s+(took|conducted|handled|did)\s+(the\s+)?interview/i.test(question) ||
    /interviewer\s+(name|names|list|who)/i.test(question) ||
    /who\s+interviewed/i.test(question)
  ) {
    if (
      !Array.isArray(analysis.sectionsNeeded) ||
      !analysis.sectionsNeeded.includes("Applications.Interviews")
    ) {
      analysis.sectionsNeeded = [
        "root",
        "Applications",
        "Applications.Interviews",
      ];
    }
    log.info(`[IntentFix] Interviewer name query → sections set`);
  }
}

// analyzeQuestion

async function analyzeQuestion(question, candidates, session) {
  const cacheKey = `${question.toLowerCase().trim()}::${session.conversationMemory.length}::${session.contextMemory.mode ?? "none"}::${session.contextMemory.candidateName ?? "none"}`;
  const cached = analysisCache.get(cacheKey);
  if (cached && Date.now() - cached.ts < ANALYSIS_CACHE_TTL) {
    log.info(`[AnalysisCache] HIT for: "${cacheKey.slice(0, 60)}"`);
    return cached.result;
  }

  const client = getOpenAIClient();

  const likelyNameQuery =
    /\b[A-Z][a-z]+\b/.test(question) ||
    /\b(who is|about|tell me about|details of)\b/i.test(question);
  const nameList = likelyNameQuery
    ? candidates
        .map((c) => c.FullName)
        .filter(Boolean)
        .reduce((acc, name) => {
          const next = acc ? `${acc}, ${name}` : name;
          return next.length > NAME_LIST_CHAR_CAP ? acc : next;
        }, "")
    : "(omitted — not a name-based query)";

  const prompt = `
You are the routing and analysis brain of a recruitment chatbot.

The candidate database is built from this SP structure:
  root                    → CandidateId, FullName, email, phoneNumber, isVerified, isProfileComplete, RegisteredOn
  Profile                 → dateOfBirth, Gender, state, city, languageKnown, linkedinProfileUrl, portfolioGithubWebsiteUrl, isStudying
  Skills                  → Skill (array of skill names)
  Education               → UnderGraduationDegree, underGraduationUniversityName, underGraduationStartYear, underGraduationEndYear, PostGraduationDegree, postGraduationUniversityName, postGraduationStartYear, postGraduationEndYear
  Experience              → companyName, role, startDate, endDate, isCurrentCompany
  Applications            → ApplicationId, JobTitle, Status, isAccepted, isShortlisted, AppliedOn
  Applications.Interviews → InterviewId, interviewAt, completedAt, InterviewStatus, Interviewer, feedback, isQualified, marksObtained, totalMarks
  Documents               → adharPath, pancardPath, bankpassbook, bankStatement, salarySlip, expierenceLetter, offerLetter, itr
  Resumes                 → id, resume, createdAt

Candidate names in the database:
${nameList}

Return ONLY valid JSON — no markdown, no explanation.

{
  "intent": "CANDIDATE" | "COUNT" | "PDF" | "REPORT" | "OOS",

  "limit": number or null,

  "entities": {
    "candidateName":  string or null,
    "email":          string or null,
    "city":           string or null,
    "state":          string or null,
    "skill":          string or null,
    "jobTitle":       string or null,
    "companyName":    string or null,
    "universityName": string or null
  },

  NOTE: gender, language, isStudying, isVerified and all boolean candidate
  properties belong in "filters" NOT in "entities". Never put them in entities.

  "filters": {
    "isVerified":          boolean or null,
    "isProfileComplete":   boolean or null,
    "isStudying":          boolean or null,
    "gender":              string or null,
    "languageKnown":       string or null,
    "hasLinkedin":         boolean or null,
    "hasPortfolio":        boolean or null,
    "birthYearFrom":       number or null,
    "birthYearTo":         number or null,
    "birthYear":           number or null,
    "ugDegree":            string or null,
    "pgDegree":            string or null,
    "hasPostGraduation":   boolean or null,
    "ugGraduationYear":    number or null,
    "pgGraduationYear":    number or null,
    "isCurrentlyWorking":  boolean or null,
    "hasExperience":       boolean or null,
    "isShortlisted":       boolean or null,
    "isAccepted":          boolean or null,
    "hasApplied":          boolean or null,
    "applicationStatus":   string or null,
    "isQualified":         boolean or null,
    "hasInterview":        boolean or null,
    "hasFeedback":         boolean or null,
    "interviewStatus":     string or null,
    "interviewer":         string or null,
    "interviewRound":      string or null,
    "marksObtained":       number or null,
    "marksOperator":       "eq"|"gt"|"lt"|"gte"|"lte" or null,
    "hasAadhar":           boolean or null,
    "hasPanCard":          boolean or null,
    "hasBankPassbook":     boolean or null,
    "hasBankStatement":    boolean or null,
    "hasSalarySlip":       boolean or null,
    "hasExperienceLetter": boolean or null,
    "hasOfferLetter":      boolean or null,
    "hasItr":              boolean or null,
    "hasResume":           boolean or null,
    "resumeCount":         number or null,
    "resumeCountOperator": "eq"|"gt"|"lt"|"gte"|"lte" or null,
    "registeredAfter":     string (ISO date) or null,
    "registeredBefore":    string (ISO date) or null
  },

  "sectionsNeeded": "ALL" or string[]
}

────────────────────────────────────────
INTENT RULES  ← READ CAREFULLY:

COUNT  — ONLY when the user explicitly asks for a NUMBER/QUANTITY.
         Trigger words: "how many", "count", "total number", "total count",
         "kitne", "count karo", "total candidates".
         Examples:
           "how many candidates have React skill" → COUNT
           "total verified candidates"            → COUNT
           "count freshers"                       → COUNT

CANDIDATE — everything else that returns a list or profile.
         A NUMBER at the START of a question means the user wants
         that many records returned — it is NOT a COUNT query.
         Examples:
           "5 candidates with interview status"   → CANDIDATE, limit: 5
           "show me 10 shortlisted candidates"    → CANDIDATE, limit: 10
           "top 3 candidates from Ahmedabad"      → CANDIDATE, limit: 3
           "list all verified candidates"         → CANDIDATE, limit: null
           "candidates with Python skill"         → CANDIDATE, limit: null
           "tell me about Avinash"                → CANDIDATE, limit: null
           "candidates born between 2000 and 2005" → CANDIDATE
           "candidates with birthdate in 2001"    → CANDIDATE
           "candidates born after 1995"           → CANDIDATE

         IMPORTANT: Any question about candidate birthdate, date of birth,
         or age range is CANDIDATE intent — never OOS.

CRITICAL: If the question starts with or contains a number followed by
"candidate(s)", "applicant(s)", "person(s)", "result(s)", or any noun
that implies a list → intent is CANDIDATE, put that number in "limit".
Never set intent to COUNT just because a number appears in the question.

CRITICAL — pronoun-only queries like "his skills", "her education", "their experience"
are ALWAYS intent: CANDIDATE with the relevant section.
These are NEVER OOS — they are follow-up questions about the previously mentioned candidate.
  "his skills"      → intent: CANDIDATE, sectionsNeeded: ["root","Skills"]
  "her education"   → intent: CANDIDATE, sectionsNeeded: ["root","Education"]  
  "his experience"  → intent: CANDIDATE, sectionsNeeded: ["root","Experience"]
  "their interview" → intent: CANDIDATE, sectionsNeeded: ["root","Applications","Applications.Interviews"]

PDF    — questions about POLICIES, PROCESSES, GUIDELINES, or HR concepts.
         These do NOT ask for specific data records.
         Examples:
           "what is the recruitment process?"
           "explain the interview policy"
           "how does hiring work?"
           "what is pre-boarding?"
           "what is online communication training?"
           "what is Recruitment Management?"
           "explain the onboarding process"
           "what are the selection criteria?"
           Any "what is", "how does", "explain", "describe" about HR/recruitment concepts.

REPORT — user explicitly wants to download/generate a report file.
         Examples: "generate a report", "export as PDF", "create a report of interviews"

OOS    — COMPLETELY unrelated to recruitment, HR, candidates, or jobs.
         Only use OOS for: weather, cooking, general knowledge, math, jokes.
         Examples: "what is 2+2", "write a poem", "what is Python programming"
         NOT OOS: anything about recruitment process, HR policies, candidates,
         interviews, onboarding, job descriptions — these are PDF or CANDIDATE.

────────────────────────────────────────
LIMIT RULES:
- Extract "limit" when the user says "top N", "first N", "show N",
  "give me N", or starts the question with a bare number like "5 candidates".
- Set limit to the integer N (e.g. 5, 10, 3).
- Set limit to null when no specific count is requested.
- limit is ONLY valid with intent CANDIDATE. For COUNT, always set limit: null.

────────────────────────────────────────
ENTITY RULES:
- CRITICAL — never carry forward entities from conversation history into the current question.
- If the user says "skills" alone, skill: null — do NOT assume they mean a previously mentioned skill.
- Only extract entities explicitly stated in the CURRENT question.
- candidateName: match ANY name resembling one in the list (partial/lowercase/typo OK)
- Extract city, state, skill, jobTitle, companyName when clearly mentioned
- universityName: extract when user mentions a college, university, or institute name
- NEVER put gender, language, isStudying, or any boolean property in entities.
  Gender ("male","female") → filters.gender
  Language ("hindi","english") → filters.languageKnown
  "studying"/"working" → filters.isStudying
  "verified" → filters.isVerified
    "users from Silver Oak"           → universityName: "Silver Oak"
    "candidates from GTU"             → universityName: "GTU"
    "students of MIT college"         → universityName: "MIT"
    "who studied at Nirma university" → universityName: "Nirma"

ENTITY EXTRACTIONS that go into FILTERS (not entities):
  phone number / mobile number    → filters.phoneNumber: "<number>"
  email address / mail id         → filters.email: "<email>"

CRITICAL — jobTitle vs city disambiguation:
The word "in" before a job name does NOT mean location. These patterns ALL mean jobTitle:
  "candidate in X job"       → jobTitle: "X",     city: null
  "candidates in X job"      → jobTitle: "X",     city: null
  "candidate for X job"      → jobTitle: "X",     city: null
  "applied in X job"         → jobTitle: "X",     city: null
  "applied for X job"        → jobTitle: "X",     city: null
  "X job candidates"         → jobTitle: "X",     city: null
  "candidate in GHU job"     → jobTitle: "GHU",   city: null  ← NOT city:"GHU"
  "candidate in New Issue job" → jobTitle: "New Issue", city: null
  "male candidate in GHU job"  → jobTitle: "GHU", city: null, gender: "Male"

RULE: If the question contains "in X job" or "for X job" or "X job" where X is
a word/phrase before the word "job", extract X as jobTitle — NEVER as city or state.
Only extract city/state when the question says "from X", "in X city", "located in X",
or X is a well-known city/state name used WITHOUT the word "job" after it.
Examples of CORRECT city extraction:
  "candidates from Surat"       → city: "Surat"
  "candidates in Ahmedabad"     → city: "Ahmedabad"   (no "job" after it)
  "male candidates from Gujarat" → state: "Gujarat"

────────────────────────────────────────
FILTER RULES — populate "filters" based on what the user asks:

ROOT:
  "verified candidates"           → isVerified: true
  "unverified candidates"         → isVerified: false
  "complete profile"              → isProfileComplete: true
  "incomplete profile"            → isProfileComplete: false

ENTITY EXTRACTIONS (go into entities, not filters):
  phone number / mobile number    → phoneNumber: "<number>"
  email address / mail id         → email: "<email>"

REGISTRATION DATE (go into filters):
  "registered after Jan 2024"     → registeredAfter: "2024-01-01"
  "registered before 2023"        → registeredBefore: "2023-01-01"
  "joined this month"             → registeredAfter: "<first day of current month>"

PROFILE:
  "studying" / "students"         → isStudying: true
  "not studying" / "working"      → isStudying: false
  "male candidates"               → gender: "Male"
  "female candidates"             → gender: "Female"
  "speaks Hindi" / "Hindi"        → languageKnown: "Hindi"
  "has linkedin"                  → hasLinkedin: true
  "no linkedin"                   → hasLinkedin: false
  "has portfolio/github"          → hasPortfolio: true

BIRTHDATE / AGE:
  "born in 2000"                  → birthYear: 2000
  "birthdate between 2000 and 2005" → birthYearFrom: 2000, birthYearTo: 2005
  "born between 2010 to 2020"     → birthYearFrom: 2010, birthYearTo: 2020
  "candidates born after 2000"    → birthYearFrom: 2000
  "candidates born before 1990"   → birthYearTo: 1990
  "age between X and Y" → convert to birth years: birthYearFrom = currentYear-Y, birthYearTo = currentYear-X
  IMPORTANT: birthdate queries are CANDIDATE intent, NOT OOS.

EDUCATION:
  "B.Tech candidates"             → ugDegree: "B.Tech"
  "MBA candidates"                → pgDegree: "MBA"
  "M.Tech candidates"             → pgDegree: "MTech"
  "master degree" / "masters"     → pgDegree: "Masters"  (NOT hasPostGraduation)
  "has PG" / "has masters/PG"     → hasPostGraduation: true
  "no PG degree"                  → hasPostGraduation: false
  "B.Tech candidates"             → ugDegree: "BTech"
  "bachelor degree" / "bachelors" → ugDegree: "Bachelors"
  "graduated in 2023" / "completed study in 2023" / "passed out in 2023"
                                  → ugGraduationYear: 2023, isStudying: null (NOT false)
  "completed PG in 2023"          → pgGraduationYear: 2023, isStudying: null
  Do NOT set isStudying when a graduation year is mentioned.

  IMPORTANT: When user mentions a specific degree type like "master degree",
  "masters", "MBA", "M.Tech" → always use pgDegree, not hasPostGraduation.
  hasPostGraduation is only for "has PG" / "has masters" without specifying type.

EXPERIENCE:
  "currently working"             → isCurrentlyWorking: true
  "freshers" / "no experience"    → hasExperience: false
  "has experience"                → hasExperience: true
  "candidates who worked as X" / "role as X"  → experienceRole: "X"
"candidates with X role"                     → experienceRole: "X"

APPLICATIONS:
  "shortlisted"                   → isShortlisted: true
  "not shortlisted"               → isShortlisted: false
  "accepted"                      → isAccepted: true
  "rejected"                      → applicationStatus: "Rejected"
  "selected"                      → applicationStatus: "Selected"
  "applied"                       → hasApplied: true
  "applied this month"     → appliedAfter: "<first day of current month ISO>"
"applied after Jan 2024" → appliedAfter: "2024-01-01"
"applied before 2023"    → appliedBefore: "2023-01-01"

INTERVIEWS:
  "with interview status" / "has interview" / "interview wale" → hasInterview: true
  "interview taken by X" / "interviewed by X" / "interviewer is X" → interviewer: "X"
  "system admin interview" / "taken by system admin" → interviewer: "system admin"
  "passed interview" / "qualified"→ isQualified: true
  "failed interview"              → isQualified: false
  "has feedback"                  → hasFeedback: true
  "completed interview"           → interviewStatus: "Completed"
  "scheduled interview"           → interviewStatus: "Scheduled"
  "in progress interview"         → interviewStatus: "In Progress"
  "postponed"                     → interviewStatus: "Postponed"
  "cancelled interview"           → interviewStatus: "Cancelled"
  "coding round" / "HR round"     → interviewRound: "<round name>" (not in SP)
  marks comparisons               → marksObtained + marksOperator
  "interviews this week"        → interviewAfter: "<Monday ISO date>"
"interviews scheduled today"  → interviewAfter: "<today ISO>", interviewBefore: "<today ISO>"
"interviews after March 2026" → interviewAfter: "2026-03-01"
"more than 3 interviews"   → interviewCount: 3, interviewCountOperator: "gt"
"exactly 2 interviews"     → interviewCount: 2, interviewCountOperator: "eq"
"at least 1 interview"     → interviewCount: 1, interviewCountOperator: "gte"

  IMPORTANT: "with interview status" means the candidate HAS an interview.
  Always set hasInterview: true for this phrase. Do NOT leave all filters null.

DOCUMENTS:
  "uploaded aadhar"               → hasAadhar: true
  "no aadhar" / "missing aadhar"  → hasAadhar: false
  "has pan card"                  → hasPanCard: true
  "has salary slip"               → hasSalarySlip: true
  "has experience letter"         → hasExperienceLetter: true
  "has offer letter"              → hasOfferLetter: true
  "has ITR"                       → hasItr: true
  "has bank passbook"             → hasBankPassbook: true
  "has bank statement"            → hasBankStatement: true
  "all documents uploaded"        → set all document booleans to true
  "missing documents"             → set relevant document boolean(s) to false
  "all documents uploaded" / "complete documents" → hasAllDocuments: true
"missing any document"   / "incomplete documents" → hasAllDocuments: false

RESUMES:
  "has resume" / "uploaded resume"   → hasResume: true
  "no resume" / "missing resume"     → hasResume: false
  "uploaded 2 resumes"               → resumeCount: 2,  resumeCountOperator: "eq"
  "more than 1 resume"               → resumeCount: 1,  resumeCountOperator: "gt"
  "at least 3 resumes"               → resumeCount: 3,  resumeCountOperator: "gte"
  "less than 2 resumes"              → resumeCount: 2,  resumeCountOperator: "lt"
  "exactly 1 resume"                 → resumeCount: 1,  resumeCountOperator: "eq"

  "experienceYears":         number or null,
"experienceYearsOperator": "eq"|"gt"|"lt"|"gte"|"lte" or null,
EXPERIENCE YEARS:
  "2 years of experience"         → experienceYears: 2, experienceYearsOperator: "eq"
  "more than 3 years experience"  → experienceYears: 3, experienceYearsOperator: "gte"
  "at least 2 years experience"   → experienceYears: 2, experienceYearsOperator: "gte"
  "less than 1 year experience"   → experienceYears: 1, experienceYearsOperator: "lt"
  "under 5 years experience"      → experienceYears: 5, experienceYearsOperator: "lt"
  "between 2 and 4 years"         → use experienceYearsFrom + experienceYearsTo
  "freshers / no experience"      → hasExperience: false  (NOT experienceYears)
  "has experience"                → hasExperience: true   (no year filter)
  "experienceYearsFrom": number or null,
"experienceYearsTo":   number or null,

  IMPORTANT: When the user mentions a specific NUMBER with "resume" (e.g.
  "upload 2 resume", "2 resumes", "uploaded 3 resumes"), ALWAYS set
  resumeCount to that number and resumeCountOperator to "eq".
  Also set hasResume: true whenever resumeCount is set.

────────────────────────────────────────
SECTIONS NEEDED RULES:
Return "ALL" for broad questions: "details", "full profile", "everything about", "tell me about", "batao", "dikhao"
Return specific array for targeted questions:
  isVerified/isProfileComplete/phone/email/name → ["root"]
  isStudying/gender/city/language/linkedin       → ["root", "Profile"]
  skills/technology                              → ["root", "Skills"]
  education/degree/university/college/institute  → ["root", "Education"]
  experience/company/working                     → ["root", "Experience"]
  job/application/shortlisted/accepted/status    → ["root", "Applications"]
  interview/marks/feedback/qualified/interview status → ["root", "Applications", "Applications.Interviews"]
  documents/aadhar/pan/resume                    → ["root", "Documents", "Resumes"]
  combined filters across sections               → include all relevant sections
When a filter is present with no candidate name → use sections matching that filter, NOT "ALL"
Always include "root" in every specific array.
When unsure → "ALL"

Recent conversation (for pronoun/follow-up resolution ONLY — do NOT inherit entities like skill/city/jobTitle from previous turns):
${buildConversationContext(session)}

Current user question:
"${question.replace(/"/g, '\\"')}"JSON:`.trim();

  // LLM call with single retry on JSON parse failure
  let lastError = null;
  for (let attempt = 1; attempt <= 2; attempt++) {
    try {
      const resp = await client.chat.completions.create({
        model: "gpt-4o-mini",
        messages: [{ role: "user", content: prompt }],
        max_tokens: 500,
        temperature: 0,
      });
      let raw = (resp.choices[0].message.content || "")
        .trim()
        .replace(/^```[a-z]*\n?/, "")
        .replace(/\n?```$/, "");

      const parsed = JSON.parse(raw);
      // 🔥 FIX: prevent wrong OOS for data-related queries
      if (parsed.intent === "OOS") {
        const q = question.toLowerCase();

        const dataKeywords = [
          "interview",
          "interview date",
          "interview status",
          "education",
          "skills",
          "experience",
          "job",
          "application",
          "marks",
          "candidate",
        ];

        const isDataQuery = dataKeywords.some((k) => q.includes(k));

        if (isDataQuery) {
          log.info("[IntentFix] OOS → CANDIDATE (data keyword detected)");

          parsed.intent = "CANDIDATE";
          parsed.sectionsNeeded = [
            "root",
            "Applications",
            "Applications.Interviews",
          ];
        }
      }
      // Schema validation
      if (!validateAnalysis(parsed)) {
        throw new Error(
          `Invalid analysis schema: ${JSON.stringify(parsed).slice(0, 100)}`,
        );
      }

      const rawLimit = parsed.limit;
      const safeLimit =
        rawLimit != null &&
        Number.isFinite(Number(rawLimit)) &&
        Number(rawLimit) > 0
          ? Math.floor(Number(rawLimit))
          : null;

      const rawEntities = parsed.entities || {};
      const rawFilters = parsed.filters || {};

      //  Normalise misplaced fields
      const normEntities = { ...rawEntities };
      const normFilters = {
        ...rawFilters,
        interviewRound: rawFilters.interviewRound || null,
      };

      if (normEntities.gender != null && normEntities.gender !== "") {
        normFilters.gender = normFilters.gender || normEntities.gender;
        delete normEntities.gender;
      }
      if (normEntities.language != null && normEntities.language !== "") {
        normFilters.languageKnown =
          normFilters.languageKnown || normEntities.language;
        delete normEntities.language;
      }
      if (
        normEntities.languageKnown != null &&
        normEntities.languageKnown !== ""
      ) {
        normFilters.languageKnown =
          normFilters.languageKnown || normEntities.languageKnown;
        delete normEntities.languageKnown;
      }
      [
        "isStudying",
        "isVerified",
        "isProfileComplete",
        "hasExperience",
        "isCurrentlyWorking",
        "isShortlisted",
        "isAccepted",
        "hasInterview",
        "hasResume",
      ].forEach((key) => {
        if (normEntities[key] != null) {
          normFilters[key] = normFilters[key] ?? normEntities[key];
          delete normEntities[key];
        }
      });
      if (normEntities.interviewer != null && normEntities.interviewer !== "") {
        normFilters.interviewer =
          normFilters.interviewer || normEntities.interviewer;
        delete normEntities.interviewer;
      }

      const FILTER_KEYS = [
        "interviewer",
        "interviewStatus",
        "applicationStatus",
        "isQualified",
        "hasFeedback",
        "marksObtained",
        "marksOperator",
        "hasAadhar",
        "hasPanCard",
        "hasBankPassbook",
        "hasBankStatement",
        "hasSalarySlip",
        "hasExperienceLetter",
        "hasOfferLetter",
        "hasItr",
        "resumeCount",
        "resumeCountOperator",
        "hasLinkedin",
        "hasPortfolio",
        "ugDegree",
        "pgDegree",
        "hasPostGraduation",
        "ugGraduationYear",
        "pgGraduationYear",
        "hasApplied",
        "birthYear",
        "birthYearFrom",
        "birthYearTo",
        "experienceYears",
        "experienceYearsOperator",
        "experienceYearsFrom",
        "experienceYearsTo",
        "phoneNumber",
        "email",
        "registeredAfter",
        "registeredBefore",
      ];
      FILTER_KEYS.forEach((key) => {
        if (normEntities[key] != null) {
          normFilters[key] = normFilters[key] ?? normEntities[key];
          delete normEntities[key];
        }
      });

      log.info(
        `[Normalised] entities: ${JSON.stringify(normEntities)} | filters: ${JSON.stringify(normFilters)}`,
      );

      const result = {
        intent: parsed.intent || "CANDIDATE",
        limit: safeLimit,
        entities: normEntities,
        filters: normFilters,
        sectionsNeeded: parsed.sectionsNeeded || "ALL",
      };
      // const pronounPattern = /\b(he|she|they|his|her|their|them|this person|the candidate|this candidate)\b/i;

      // if (pronounPattern.test(question) && session.contextMemory.candidateName) {
      //   const expanded = `${session.contextMemory.candidateName} ${question
      //     .replace(/\b(his|her|their)\b/gi, "")
      //     .replace(/\b(he|she|they|them|this person|the candidate|this candidate)\b/gi, "")
      //     .trim()}`;

      //   log.info(`[PronounExpand] "${question}" → "${expanded}"`);
      //   question = expanded;
      // }
      // STEP 3 — HANDLE “WHOSE?” (ADD HERE)
      if (
        result.intent === "CANDIDATE" &&
        !result.entities?.candidateName &&
        !session.contextMemory?.candidateName
      ) {
        const isListQuery =
          result.entities?.skill ||
          result.entities?.city ||
          result.entities?.state ||
          result.entities?.jobTitle ||
          result.entities?.jobTitle ||
          result.entities?.companyName ||
          result.entities?.universityName ||
          result.filters?.email || // ← check filters not entities
          result.filters?.phoneNumber;
        Object.values(result.filters || {}).some((v) => v != null);

        // Only ask clarification if it's a vague single word AND
        // no pronoun expansion already happened (i.e. question was not expanded)
        const isVagueFollowUp =
          question.trim().split(/\s+/).length <= 2 &&
          /^(interview|education|skills?|experience|job)$/i.test(
            question.trim(),
          );

        const pronounAlreadyExpanded =
          PRONOUN_RE.test(question) === false &&
          session.contextMemory?.candidateName != null;

        if (isVagueFollowUp && !isListQuery && !pronounAlreadyExpanded) {
          return {
            intent: "CANDIDATE",
            askClarification: true,
            clarificationMessage: "Please specify the candidate name.",
          };
        }
      }

      // ── Store in LRU cache ────────────────────────────────────────────────
      analysisCache.set(cacheKey, { result, ts: Date.now() });

      if (analysisCache.size > ANALYSIS_CACHE_MAX) {
        analysisCache.delete(analysisCache.keys().next().value);
      }

      return result;
    } catch (err) {
      lastError = err;
      log.error(`analyzeQuestion attempt ${attempt}/2 failed: ${err.message}`);
      if (attempt < 2) {
        log.info("Retrying analyzeQuestion...");
        await new Promise((r) => setTimeout(r, 300));
      }
    }
  }

  log.error(
    `analyzeQuestion failed after retry — defaulting to CANDIDATE/ALL. Last error: ${lastError?.message}`,
  );
  return {
    intent: "CANDIDATE",
    limit: null,
    entities: {},
    filters: {},
    sectionsNeeded: "ALL",
  };
}

// Schema validator

function validateAnalysis(parsed) {
  const validIntents = ["CANDIDATE", "COUNT", "PDF", "REPORT", "OOS"];
  if (!parsed || typeof parsed !== "object" || Array.isArray(parsed))
    return false;
  if (!validIntents.includes(parsed.intent)) return false;
  if (
    parsed.entities !== null &&
    parsed.entities !== undefined &&
    typeof parsed.entities !== "object"
  )
    return false;
  if (
    parsed.filters !== null &&
    parsed.filters !== undefined &&
    typeof parsed.filters !== "object"
  )
    return false;
  return true;
}

// FILTER CANDIDATES

function filterCandidates(entities, filters = {}) {
  if (!candidateStore || !candidateIndex) {
    console.warn("Cache or index not ready");
    return [];
  }

  let pools = [];

  // ─────────────────────────────
  // STEP 1: INDEX FILTERING
  // ─────────────────────────────

  if (entities.skill) {
    const skillKey = normalizeSkill(entities.skill);
    const skillSet = candidateIndex.bySkill.get(skillKey) || [];
    pools.push(skillSet);
  }

  if (entities.state) {
    const stateSet =
      candidateIndex.byState.get(entities.state.toLowerCase()) || [];
    pools.push(stateSet);
  }

  if (entities.jobTitle) {
    const jobSet =
      candidateIndex.byJobTitle.get(entities.jobTitle.toLowerCase()) || [];
    pools.push(jobSet);
  }

  // ─────────────────────────────
  // STEP 2: INTERSECTION
  // ─────────────────────────────

  let pool;

  if (pools.length === 0) {
    pool = candidateStore.candidates;
  } else {
    pools.sort((a, b) => a.length - b.length);
    pool = pools[0];

    for (let i = 1; i < pools.length; i++) {
      const set = new Set(pools[i]);
      pool = pool.filter((c) => set.has(c));
    }
  }

  // ─────────────────────────────
  // STEP 3: FINAL FILTERING
  // ─────────────────────────────

  pool = pool.filter((candidate) => {
    if (entities.candidateName) {
      const search = entities.candidateName.toLowerCase().trim();
      const fullName = (candidate.FullName || "").toLowerCase();

      const words = search.split(/\s+/).filter((w) => w.length > 0);

      const match =
        fullName.includes(search) ||
        (words.length >= 2 && words.every((w) => fullName.includes(w))) ||
        (words.length === 1 && fullName.includes(words[0]));

      if (!match) return false;
    }

    if (entities.city) {
      const city = (candidate.Profile?.city || "").toLowerCase();
      if (!city.includes(entities.city.toLowerCase())) return false;
    }
    // email filter
    if (filters.email) {
      const email = (candidate.email || "").toLowerCase();
      if (!email.includes(filters.email.toLowerCase())) return false;
    }

    // phone filter
    if (filters.phoneNumber) {
      const phone = (candidate.phoneNumber || "").toString();
      if (!phone.includes(filters.phoneNumber.toString())) return false;
    }

    // company filter
    if (entities.companyName) {
      const match = (candidate.Experience || []).some((e) =>
        (e.companyName || "")
          .toLowerCase()
          .includes(entities.companyName.toLowerCase()),
      );
      if (!match) return false;
    }

    // birth year
    if (filters.birthYear != null) {
      const dob = candidate.Profile?.dateOfBirth;
      if (!dob) return false;

      const year = new Date(dob).getFullYear();
      if (year !== filters.birthYear) return false;
    }

    // 🟢 ✅ NESTED JOIN LOGIC (THIS IS THE STEP 2 YOU ASKED)
    if (entities.jobTitle || filters.marksObtained != null) {
      if (!matchApplicationWithInterview(candidate, filters, entities)) {
        return false;
      }
    }

    // other filters
    return matchesFilters(candidate, filters);
  });

  return pool;
}

function detectResponseType(question, isSingleCandidate, totalFound) {
  const q = question.toLowerCase();

  // YES / NO
  if (/^(is|are|was|were|does|do|did|can|has|have)\b/.test(q)) {
    return "YES_NO";
  }

  // COUNT
  if (q.includes("how many") || q.includes("count")) {
    return "COUNT";
  }

  // SINGLE candidate
  if (isSingleCandidate) {
    return "DETAIL";
  }

  // LIST
  if (totalFound > 1) {
    return "LIST";
  }

  return "DEFAULT";
}

// ENTITY MATCHING

// function matchesEntity(candidate, entities) {
//   const checks = [];

//   if (entities.candidateName != null && entities.candidateName !== "") {
//     const search = entities.candidateName.toLowerCase().trim();
//     const fullName = (candidate.FullName || "").toLowerCase();
//     const words = search.split(/\s+/).filter((w) => w.length > 0);
//     const directMatch = fullName.includes(search);
//     const meaningfulWords = words.filter((w) => w.length > 1);
//     const multiWordMatch = meaningfulWords.length >= 2
//       && meaningfulWords.every((w) => fullName.includes(w));
//     const singleWordMatch = meaningfulWords.length === 1
//       && fullName.includes(meaningfulWords[0]);
//     const nameMatch = meaningfulWords.length >= 2
//       ? (directMatch || multiWordMatch)
//       : (directMatch || singleWordMatch);
//     checks.push(nameMatch);
//   }

//   if (entities.email != null && entities.email !== "") {
//     checks.push(
//       (candidate.email || "").toLowerCase() === entities.email.toLowerCase(),
//     );
//   }

//   // ── City: exact match or "starts with word boundary" guard ───────────────
//   if (entities.city != null && entities.city !== "") {
//     const cityVal   = (candidate.Profile?.city || "").toLowerCase().trim();
//     const cityQuery = entities.city.toLowerCase().trim();

//     const cityMatch =
//       cityVal === cityQuery ||
//       cityVal.startsWith(cityQuery + " ") ||
//       cityVal.includes(cityQuery);
//     checks.push(cityMatch);
//   }

//   // ── State: same guard as city ─────────────────────────────────────────────
//   if (entities.state != null && entities.state !== "") {
//     const stateVal   = (candidate.Profile?.state || "").toLowerCase().trim();
//     const stateQuery = entities.state.toLowerCase().trim();
//     const stateMatch =
//       stateVal === stateQuery ||
//       stateVal.startsWith(stateQuery + " ") ||
//       stateVal.includes(stateQuery);
//     checks.push(stateMatch);
//   }

//   if (entities.skill != null && entities.skill !== "") {
//     const skills = (candidate.Skills || []).map((s) =>
//       (s.Skill || "").toLowerCase(),
//     );
//     checks.push(skills.some((sk) => sk.includes(entities.skill.toLowerCase())));
//   }

//   if (entities.jobTitle != null && entities.jobTitle !== "") {
//     const jobs = (candidate.Applications || []).map((a) =>
//       (a.JobTitle || "").toLowerCase(),
//     );
//     const titleQuery = entities.jobTitle.toLowerCase();
//     const titleWords = titleQuery.split(/\s+/).filter((w) => w.length > 1);
//     const jobMatch =
//       jobs.some((j) => j.includes(titleQuery)) ||
//       (titleWords.length > 0 &&
//         jobs.some((j) =>
//           titleWords.length <= 2
//             ? titleWords.some((w) => j.includes(w))
//             : titleWords.every((w) => j.includes(w)),
//         ));
//     checks.push(jobMatch);
//   }

//   if (entities.companyName != null && entities.companyName !== "") {
//     const companies = (candidate.Experience || []).map((e) =>
//       (e.companyName || "").toLowerCase(),
//     );
//     const companyQuery = entities.companyName.toLowerCase();
//     const companyWords = companyQuery.split(/\s+/).filter((w) => w.length > 2);
//     const companyMatch =
//       companies.some((co) => co.includes(companyQuery)) ||
//       (companyWords.length > 0 &&
//         companies.some((co) => companyWords.every((w) => co.includes(w))));
//     checks.push(companyMatch);
//   }

//   if (entities.universityName != null && entities.universityName !== "") {
//     const uniQuery = entities.universityName.toLowerCase().trim();
//     const edu = candidate.Education || [];
//     const allUnis = edu
//       .flatMap((e) => [
//         (e.underGraduationUniversityName || "").toLowerCase(),
//         (e.postGraduationUniversityName || "").toLowerCase(),
//       ])
//       .filter(Boolean);
//     const uniWords = uniQuery.split(/\s+/).filter((w) => w.length > 2);
//     const uniMatch =
//       allUnis.some((u) => u.includes(uniQuery)) ||
//       (uniWords.length > 0 &&
//         allUnis.some((u) =>
//           uniWords.length <= 2
//             ? uniWords.some((w) => u.includes(w))
//             : uniWords.every((w) => u.includes(w)),
//         ));
//     checks.push(uniMatch);
//   }

//   return checks.length === 0 ? false : checks.every(Boolean);
// }

// const filterModules = [
//   filterRoot,
//   filterProfile,
//   filterEducation,
//   filterExperience,
//   filterApplications,
//   filterInterviews,
//   filterDocuments,
//   filterResumes,
// ];

function matchesFilters(candidate, filters) {
  // ROOT
  if (
    filters.isVerified != null ||
    filters.isProfileComplete != null ||
    filters.registeredAfter ||
    filters.registeredBefore ||
    filters.phoneNumber ||
    filters.email
  ) {
    if (!filterRoot(candidate, filters)) return false;
  }

  // PROFILE
  if (
    filters.gender ||
    filters.languageKnown ||
    filters.hasLinkedin != null ||
    filters.hasPortfolio != null ||
    filters.isStudying != null ||
    filters.birthYear != null ||
    filters.birthYearFrom != null ||
    filters.birthYearTo != null
  ) {
    if (!filterProfile(candidate, filters)) return false;
  }

  // EDUCATION
  if (
    filters.ugDegree ||
    filters.pgDegree ||
    filters.hasPostGraduation != null ||
    filters.ugGraduationYear != null ||
    filters.pgGraduationYear != null ||
    filters.universityName
  ) {
    if (!filterEducation(candidate, filters)) return false;
  }

  // EXPERIENCE
  if (
    filters.hasExperience != null ||
    filters.isCurrentlyWorking != null ||
    filters.experienceYears != null ||
    filters.experienceYearsFrom != null ||
    filters.experienceYearsTo != null ||
    filters.experienceRole
  ) {
    if (!filterExperience(candidate, filters)) return false;
  }

  // APPLICATIONS
  if (
    filters.isShortlisted != null ||
    filters.isAccepted != null ||
    filters.hasApplied != null ||
    filters.applicationStatus ||
    filters.appliedAfter ||
    filters.appliedBefore
  ) {
    if (!filterApplications(candidate, filters)) return false;
  }

  // INTERVIEWS
  if (
    filters.hasInterview != null ||
    filters.interviewer ||
    filters.interviewStatus ||
    filters.isQualified != null ||
    filters.hasFeedback != null ||
    filters.marksObtained != null ||
    filters.interviewCount != null ||
    filters.interviewAfter ||
    filters.interviewBefore ||
    filters.interviewRound
  ) {
    if (!filterInterviews(candidate, filters)) return false;
  }

  // DOCUMENTS
  if (
    filters.hasAllDocuments != null ||
    filters.hasAadhar != null ||
    filters.hasPanCard != null ||
    filters.hasBankPassbook != null ||
    filters.hasBankStatement != null ||
    filters.hasSalarySlip != null ||
    filters.hasExperienceLetter != null ||
    filters.hasOfferLetter != null ||
    filters.hasItr != null
  ) {
    if (!filterDocuments(candidate, filters)) return false;
  }

  // RESUMES
  if (filters.hasResume != null || filters.resumeCount != null) {
    if (!filterResumes(candidate, filters)) return false;
  }

  // SKILLS COUNT
  if (filters.skillCount != null) {
    if (!filterSkills(candidate, filters)) return false;
  }

  return true;
}
function filterRoot(candidate, filters) {
  if (filters.isVerified != null) {
    if (Boolean(candidate.isVerified) !== Boolean(filters.isVerified))
      return false;
  }
  if (filters.isProfileComplete != null) {
    if (
      Boolean(candidate.isProfileComplete) !==
      Boolean(filters.isProfileComplete)
    )
      return false;
  }
  if (filters.phoneNumber) {
    const phone = (candidate.phoneNumber || "").toLowerCase();
    if (!phone.includes(filters.phoneNumber.toLowerCase())) return false;
  }
  if (filters.email) {
    const email = (candidate.email || "").toLowerCase();
    if (!email.includes(filters.email.toLowerCase())) return false;
  }
  if (filters.registeredAfter) {
    const reg = new Date(candidate.RegisteredOn);
    if (reg < new Date(filters.registeredAfter)) return false;
  }

  if (filters.registeredBefore) {
    const reg = new Date(candidate.RegisteredOn);
    if (reg > new Date(filters.registeredBefore)) return false;
  }
  return true;
}

function filterProfile(candidate, filters) {
  if (filters.isStudying != null) {
    const val = candidate.Profile?.isStudying;
    if (val != null && Boolean(val) !== Boolean(filters.isStudying))
      return false;
  }

  if (filters.gender) {
    const g = (candidate.Profile?.Gender || "").toLowerCase();
    if (!g.includes(filters.gender.toLowerCase())) return false;
  }

  if (filters.languageKnown) {
    const lang = (candidate.Profile?.languageKnown || "").toLowerCase();
    if (!lang.includes(filters.languageKnown.toLowerCase())) return false;
  }

  if (filters.hasLinkedin != null) {
    const hasIt = !!candidate.Profile?.linkedinProfileUrl;
    if (hasIt !== Boolean(filters.hasLinkedin)) return false;
  }

  if (filters.hasPortfolio != null) {
    const hasIt = !!candidate.Profile?.portfolioGithubWebsiteUrl;
    if (hasIt !== Boolean(filters.hasPortfolio)) return false;
  }

  // birthYear filters
  if (
    filters.birthYear != null ||
    filters.birthYearFrom != null ||
    filters.birthYearTo != null
  ) {
    const dob = candidate.Profile?.dateOfBirth;
    if (!dob) return false;
    const year = new Date(dob).getFullYear();
    if (isNaN(year)) return false;
    if (filters.birthYear != null && year !== filters.birthYear) return false;
    if (filters.birthYearFrom != null && year < filters.birthYearFrom)
      return false;
    if (filters.birthYearTo != null && year > filters.birthYearTo) return false;
  }

  return true;
}

function filterEducation(candidate, filters) {
  const edu = candidate.Education || [];
  if (filters.universityName) {
    const query = filters.universityName.toLowerCase();

    const match = (candidate.Education || []).some(
      (e) =>
        (e.underGraduationUniversityName || "").toLowerCase().includes(query) ||
        (e.postGraduationUniversityName || "").toLowerCase().includes(query),
    );

    if (!match) return false;
  }
  if (filters.ugDegree) {
    const ugQuery = filters.ugDegree.toLowerCase().trim();

    const match = edu.some((e) =>
      (e.UnderGraduationDegree || "").toLowerCase().includes(ugQuery),
    );

    if (!match) return false;
  }

  if (filters.pgDegree) {
    const pgQuery = filters.pgDegree.toLowerCase().trim();

    const match = edu.some((e) =>
      (e.PostGraduationDegree || "").toLowerCase().includes(pgQuery),
    );

    if (!match) return false;
  }

  if (filters.hasPostGraduation != null) {
    const hasPg = edu.some(
      (e) => e.PostGraduationDegree && e.PostGraduationDegree.trim() !== "",
    );

    if (hasPg !== Boolean(filters.hasPostGraduation)) return false;
  }

  return true;
}

function filterExperience(candidate, filters) {
  if (filters.hasExperience === true) {
    if ((candidate._meta.experienceYears || 0) <= 0) return false;
  }
  if (filters.isCurrentlyWorking != null) {
    const isWorking = (candidate.Experience || []).some(
      (e) => e.isCurrentCompany,
    );
    if (isWorking !== Boolean(filters.isCurrentlyWorking)) return false;
  }

  if (filters.experienceRole) {
    const query = filters.experienceRole.toLowerCase();
    const match = (candidate.Experience || []).some((e) =>
      (e.role || "").toLowerCase().includes(query),
    );
    if (!match) return false;
  }
  if (filters.hasExperience === false) {
    if ((candidate._meta.experienceYears || 0) > 0) return false;
  }
  if (
    filters.experienceYears != null ||
    filters.experienceYearsFrom != null ||
    filters.experienceYearsTo != null
  ) {
    const totalYears = candidate._meta.experienceYears || 0;

    if (filters.experienceYears != null) {
      const target = Number(filters.experienceYears);
      const op = filters.experienceYearsOperator || "eq";

      if (op === "gt" && !(totalYears > target)) return false;
      if (op === "lt" && !(totalYears < target)) return false;
      if (op === "gte" && !(totalYears >= target)) return false;
      if (op === "lte" && !(totalYears <= target)) return false;
      if (op === "eq" && !(Math.abs(totalYears - target) <= 0.5)) return false;
    }
    if (
      filters.experienceYearsFrom != null ||
      filters.experienceYearsTo != null
    ) {
      const totalYears = candidate._meta.experienceYears || 0;
      if (
        filters.experienceYearsFrom != null &&
        totalYears < filters.experienceYearsFrom
      )
        return false;
      if (
        filters.experienceYearsTo != null &&
        totalYears > filters.experienceYearsTo
      )
        return false;
    }
  }

  return true;
}

function filterApplications(candidate, filters) {
  const apps = candidate.Applications || [];

  if (filters.isShortlisted != null) {
    const match = apps.some(
      (a) => Boolean(a.isShortlisted) === Boolean(filters.isShortlisted),
    );
    if (!match) return false;
  }

  if (filters.isAccepted != null) {
    const match = apps.some(
      (a) => Boolean(a.isAccepted) === Boolean(filters.isAccepted),
    );
    if (!match) return false;
  }

  if (filters.hasApplied != null) {
    const hasApp = apps.length > 0;
    if (hasApp !== Boolean(filters.hasApplied)) return false;
  }

  if (filters.applicationStatus) {
    const target = filters.applicationStatus.toLowerCase();
    const match = apps.some((a) =>
      (a.Status || "").toLowerCase().includes(target),
    );
    if (!match) return false;
  }
  if (filters.appliedAfter || filters.appliedBefore) {
    const match = (candidate.Applications || []).some((app) => {
      if (!app.AppliedOn) return false;
      const d = new Date(app.AppliedOn);
      if (filters.appliedAfter && d < new Date(filters.appliedAfter))
        return false;
      if (filters.appliedBefore && d > new Date(filters.appliedBefore))
        return false;
      return true;
    });
    if (!match) return false;
  }

  return true;
}

function filterInterviews(candidate, filters) {
  // 🟢 SMART interviewer matching (FINAL FIX)
  if (filters.interviewer) {
    const queryWords = filters.interviewer
      .toLowerCase()
      .split(/\s+/)
      .filter((w) => w.length > 0);

    const apps = candidate.Applications || [];

    let found = false;

    for (const app of apps) {
      if (!Array.isArray(app.Interviews)) continue;

      for (const i of app.Interviews) {
        const name = (i.Interviewer || "").toLowerCase();

        const match = queryWords.every((word) => name.includes(word));

        if (match) {
          found = true;
          break;
        }
      }

      if (found) break;
    }

    if (!found) return false;
  }
  if (filters.hasInterview != null) {
    if (candidate._meta.hasInterview !== filters.hasInterview) {
      return false;
    }
  }
  if (filters.interviewCount != null) {
    const count = (candidate.Applications || []).reduce(
      (sum, app) =>
        sum + (Array.isArray(app.Interviews) ? app.Interviews.length : 0),
      0,
    );
    const target = Number(filters.interviewCount);
    const op = filters.interviewCountOperator || "eq";
    if (op === "eq" && count !== target) return false;
    if (op === "gt" && count <= target) return false;
    if (op === "lt" && count >= target) return false;
    if (op === "gte" && count < target) return false;
    if (op === "lte" && count > target) return false;
  }
  if (filters.interviewAfter || filters.interviewBefore) {
    const apps = candidate.Applications || [];
    const match = apps.some((app) =>
      (Array.isArray(app.Interviews) ? app.Interviews : []).some((iv) => {
        const d = new Date(iv.interviewAt || iv.completedAt);
        if (isNaN(d)) return false;
        if (filters.interviewAfter && d < new Date(filters.interviewAfter))
          return false;
        if (filters.interviewBefore && d > new Date(filters.interviewBefore))
          return false;
        return true;
      }),
    );
    if (!match) return false;
  }
  //   if (filters.interviewer) {
  //   const match = (candidate.Applications || []).some(app =>
  //     (app.Interviews || []).some(i =>
  //       (i.Interviewer || "").toLowerCase().includes(filters.interviewer.toLowerCase())
  //     )
  //   );
  //   if (!match) return false;
  // }

  if (filters.marksObtained != null) {
    const marks = candidate._meta.bestMarks;
    if (marks == null) return false;

    const target = Number(filters.marksObtained);
    const op = filters.marksOperator || "eq";

    if (op === "gt" && !(marks > target)) return false;
    if (op === "lt" && !(marks < target)) return false;
    if (op === "gte" && !(marks >= target)) return false;
    if (op === "lte" && !(marks <= target)) return false;
    if (op === "eq" && !(Math.abs(marks - target) < 0.01)) return false;
  }

  if (filters.interviewStatus) {
    const target = filters.interviewStatus.toLowerCase();
    const apps = candidate.Applications || [];
    const match = apps.some((app) =>
      (Array.isArray(app.Interviews) ? app.Interviews : []).some((iv) =>
        (iv.InterviewStatus || "").toLowerCase().includes(target),
      ),
    );
    if (!match) return false;
  }

  if (filters.isQualified != null) {
    const apps = candidate.Applications || [];
    const match = apps.some((app) =>
      (Array.isArray(app.Interviews) ? app.Interviews : []).some(
        (iv) => Boolean(iv.isQualified) === Boolean(filters.isQualified),
      ),
    );
    if (!match) return false;
  }

  if (filters.hasFeedback != null) {
    const apps = candidate.Applications || [];
    const hasFb = apps.some((app) =>
      (Array.isArray(app.Interviews) ? app.Interviews : []).some(
        (iv) => iv.feedback && iv.feedback.trim() !== "",
      ),
    );
    if (hasFb !== Boolean(filters.hasFeedback)) return false;
  }

  return true;
}

function filterDocuments(candidate, filters) {
  const doc = candidate.Documents || {};
  if (filters.hasAllDocuments === true) {
    const allPresent =
      !!doc.adharPath &&
      !!doc.pancardPath &&
      !!doc.bankpassbook &&
      !!doc.bankStatement &&
      !!doc.salarySlip &&
      !!doc.expierenceLetter &&
      !!doc.offerLetter &&
      !!doc.itr;
    if (!allPresent) return false;
  }
  if (filters.hasAllDocuments === false) {
    const allPresent =
      !!doc.adharPath &&
      !!doc.pancardPath &&
      !!doc.bankpassbook &&
      !!doc.bankStatement &&
      !!doc.salarySlip &&
      !!doc.expierenceLetter &&
      !!doc.offerLetter &&
      !!doc.itr;
    if (allPresent) return false;
  }

  if (filters.hasAadhar != null) {
    if (!!doc.adharPath !== Boolean(filters.hasAadhar)) return false;
  }
  if (filters.hasPanCard != null) {
    if (!!doc.pancardPath !== Boolean(filters.hasPanCard)) return false;
  }
  if (filters.hasBankPassbook != null) {
    if (!!doc.bankpassbook !== Boolean(filters.hasBankPassbook)) return false;
  }
  if (filters.hasBankStatement != null) {
    if (!!doc.bankStatement !== Boolean(filters.hasBankStatement)) return false;
  }
  if (filters.hasSalarySlip != null) {
    if (!!doc.salarySlip !== Boolean(filters.hasSalarySlip)) return false;
  }
  if (filters.hasExperienceLetter != null) {
    if (!!doc.expierenceLetter !== Boolean(filters.hasExperienceLetter))
      return false;
  }
  if (filters.hasOfferLetter != null) {
    if (!!doc.offerLetter !== Boolean(filters.hasOfferLetter)) return false;
  }
  if (filters.hasItr != null) {
    if (!!doc.itr !== Boolean(filters.hasItr)) return false;
  }

  return true;
}

function filterResumes(candidate, filters) {
  const resumes = candidate.Resumes || [];

  if (filters.hasResume != null) {
    const hasIt = resumes.some((r) => r.resume && r.resume.trim() !== "");
    if (hasIt !== Boolean(filters.hasResume)) return false;
  }

  if (filters.resumeCount != null) {
    const count = resumes.filter(
      (r) => r.resume && r.resume.trim() !== "",
    ).length;

    const target = Number(filters.resumeCount);
    const op = filters.resumeCountOperator || "eq";

    if (op === "eq" && count !== target) return false;
    if (op === "gt" && count <= target) return false;
    if (op === "lt" && count >= target) return false;
    if (op === "gte" && count < target) return false;
    if (op === "lte" && count > target) return false;
  }

  return true;
}
function filterSkills(candidate, filters) {
  if (filters.skillCount != null) {
    const count = (candidate.Skills || []).length;
    const target = Number(filters.skillCount);
    const op = filters.skillCountOperator || "eq";
    if (op === "eq" && count !== target) return false;
    if (op === "gt" && count <= target) return false;
    if (op === "lt" && count >= target) return false;
    if (op === "gte" && count < target) return false;
    if (op === "lte" && count > target) return false;
  }
  return true;
}

function getSummarySections(filters, llmSectionsNeeded) {
  const llmIsRootOnly =
    Array.isArray(llmSectionsNeeded) &&
    llmSectionsNeeded.length === 1 &&
    llmSectionsNeeded[0] === "root";

  if (llmIsRootOnly) {
    const needsEducation =
      filters.ugDegree ||
      filters.pgDegree ||
      filters.hasPostGraduation != null ||
      filters.ugGraduationYear != null ||
      filters.pgGraduationYear != null ||
      filters.universityName;
    const needsExperience =
      filters.hasExperience != null ||
      filters.isCurrentlyWorking != null ||
      filters.experienceYears != null;
    const needsApplications =
      filters.isShortlisted != null ||
      filters.isAccepted != null ||
      filters.hasApplied != null ||
      filters.applicationStatus ||
      filters.hasInterview != null ||
      filters.interviewStatus ||
      filters.interviewer ||
      filters.isQualified != null ||
      filters.hasFeedback != null ||
      filters.marksObtained != null;
    const needsDocuments =
      filters.hasAadhar != null ||
      filters.hasPanCard != null ||
      filters.hasBankPassbook != null ||
      filters.hasBankStatement != null ||
      filters.hasSalarySlip != null ||
      filters.hasExperienceLetter != null ||
      filters.hasOfferLetter != null ||
      filters.hasItr != null;
    const needsResumes =
      filters.hasResume != null || filters.resumeCount != null;
    const needsProfile =
      filters.gender ||
      filters.languageKnown ||
      filters.hasLinkedin != null ||
      filters.isStudying != null ||
      filters.birthYear != null ||
      filters.birthYearFrom != null ||
      filters.birthYearTo != null;

    const sections = new Set(["root"]);
    if (needsProfile) sections.add("Profile");
    if (needsEducation) sections.add("Education");
    if (needsExperience) sections.add("Experience");
    if (needsApplications) {
      sections.add("Applications");
      sections.add("Applications.Interviews");
    }
    if (needsDocuments) sections.add("Documents");
    if (needsResumes) sections.add("Resumes");
    return [...sections];
  }

  const sections = new Set(["root"]);

  // Add Profile only when explicitly needed
  const needsProfile =
    Array.isArray(llmSectionsNeeded) && llmSectionsNeeded.includes("Profile");
  if (needsProfile) sections.add("Profile");

  if (
    filters.birthYear != null ||
    filters.birthYearFrom != null ||
    filters.birthYearTo != null
  ) {
    sections.add("Profile");
  }
  if (
    filters.ugDegree ||
    filters.pgDegree ||
    filters.hasPostGraduation != null ||
    filters.ugGraduationYear != null ||
    filters.pgGraduationYear != null
  ) {
    sections.add("Education");
  }
  if (filters.hasExperience != null || filters.isCurrentlyWorking != null) {
    sections.add("Experience");
  }
  if (
    filters.isShortlisted != null ||
    filters.isAccepted != null ||
    filters.hasApplied != null ||
    filters.applicationStatus ||
    filters.hasInterview != null ||
    filters.interviewStatus ||
    filters.interviewer ||
    filters.isQualified != null ||
    filters.hasFeedback != null ||
    filters.marksObtained != null
  ) {
    sections.add("Applications");
    sections.add("Applications.Interviews");
  }

  // ADD THIS inside getSummarySections, before the final return
  if (filters.hasResume != null || filters.resumeCount != null) {
    sections.add("Resumes");
    // Remove Skills if it was added — resume queries don't need it
    sections.delete("Skills");
  }

  const allowed = new Set([
    "root",
    "Profile",
    "Skills",
    "Education",
    "Experience",
    "Applications",
    "Applications.Interviews",
  ]);
  if (Array.isArray(llmSectionsNeeded)) {
    llmSectionsNeeded.forEach((s) => {
      if (allowed.has(s)) sections.add(s);
    });
  }

  return [...sections];
}

// PROJECT DATA

function projectData(candidates, sectionsNeeded) {
  const needed = new Set(sectionsNeeded);
  if (needed.has("Applications.Interviews")) needed.add("Applications");
  log.info(`Projecting sections: ${[...needed].join(", ")}`);

  return candidates.map((c) => {
    const out = {
      CandidateId: c.CandidateId,
      FullName: c.FullName,
      email: c.email,
    };
    if (needed.has("root")) {
      out.phoneNumber = c.phoneNumber;
      out.isVerified = c.isVerified;
      out.isProfileComplete = c.isProfileComplete;
      out.RegisteredOn = c.RegisteredOn;
    }
    if (needed.has("Profile")) out.Profile = c.Profile;
    if (needed.has("Skills")) out.Skills = c.Skills;
    if (needed.has("Education")) out.Education = c.Education;
    if (needed.has("Experience")) out.Experience = c.Experience;
    if (needed.has("Applications")) {
      out.Applications = needed.has("Applications.Interviews")
        ? c.Applications
        : (c.Applications || []).map(({ Interviews, ...rest }) => rest);
    }
    if (needed.has("Documents")) out.Documents = c.Documents;
    if (needed.has("Resumes")) out.Resumes = c.Resumes;
    return out;
  });
}

// HELPERS

function buildFilterLabel(entities, filters) {
  const parts = [];

  if (entities?.candidateName) return `named "${entities.candidateName}"`;
  if (entities?.email) parts.push(`with email "${entities.email}"`);
  if (entities?.phoneNumber)
    parts.push(`with phone number "${entities.phoneNumber}"`);
  if (entities?.jobTitle) parts.push(`applied for "${entities.jobTitle}"`);
  if (entities?.skill) parts.push(`with skill "${entities.skill}"`);
  if (entities?.city) parts.push(`from "${entities.city}"`);
  if (entities?.state) parts.push(`from "${entities.state}"`);
  if (entities?.companyName)
    parts.push(`who worked at "${entities.companyName}"`);
  if (entities?.universityName)
    parts.push(`from "${entities.universityName}" university/college`);

  if (filters.isStudying === true) parts.push("currently studying");
  if (filters.isStudying === false) parts.push("not currently studying");
  if (filters.isShortlisted === true) parts.push("shortlisted");
  if (filters.isVerified === true) parts.push("verified");
  if (filters.isVerified === false) parts.push("unverified");
  if (filters.isProfileComplete === true) parts.push("with complete profile");
  if (filters.isProfileComplete === false)
    parts.push("with incomplete profile");
  if (filters.hasExperience === false)
    parts.push("with no work experience (freshers)");
  if (filters.hasExperience === true) parts.push("with work experience");
  if (filters.isCurrentlyWorking === true) parts.push("currently working");
  if (filters.gender) parts.push(`who are ${filters.gender}`);
  if (filters.languageKnown) parts.push(`who speak ${filters.languageKnown}`);
  if (filters.ugDegree) parts.push(`with ${filters.ugDegree} degree`);
  if (filters.pgDegree) parts.push(`with ${filters.pgDegree} degree`);
  if (filters.hasPostGraduation === true)
    parts.push("with post-graduation degree");
  if (filters.hasPostGraduation === false)
    parts.push("without post-graduation degree");
  if (filters.applicationStatus)
    parts.push(`with application status "${filters.applicationStatus}"`);
  if (filters.interviewStatus)
    parts.push(`with interview status "${filters.interviewStatus}"`);
  if (filters.isQualified === true) parts.push("who qualified the interview");
  if (filters.isQualified === false)
    parts.push("who did not qualify the interview");
  if (filters.marksObtained != null) {
    const opLabel = {
      eq: "exactly",
      gt: "more than",
      lt: "less than",
      gte: "at least",
      lte: "at most",
    }[filters.marksOperator || "eq"];
    parts.push(`who scored ${opLabel} ${filters.marksObtained} marks`);
  }
  if (filters.hasResume === true) parts.push("with uploaded resume");
  if (filters.hasResume === false) parts.push("with no resume uploaded");
  if (filters.hasAadhar === false) parts.push("with missing Aadhar card");
  if (filters.hasPanCard === false) parts.push("with missing PAN card");
  if (filters.experienceYears != null) {
    const opLabel = {
      eq: "exactly",
      gt: "more than",
      lt: "less than",
      gte: "at least",
      lte: "at most",
    }[filters.experienceYearsOperator || "eq"];
    parts.push(
      `with ${opLabel} ${filters.experienceYears} year(s) of experience`,
    );
  }
  if (
    filters.experienceYearsFrom != null ||
    filters.experienceYearsTo != null
  ) {
    const from = filters.experienceYearsFrom ?? "?";
    const to = filters.experienceYearsTo ?? "?";
    parts.push(`with ${from}–${to} years of experience`);
  }

  return parts.length ? parts.join(", ") : "matching your query";
}
