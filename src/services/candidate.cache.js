// ============================================================
// candidate.cache.js

import { getPool } from "../db/connection.js";
import { getOpenAIClient, generateNaturalAnswer } from "./ai.service.js";
import { log } from "../utils/logger.js";

const SP_NAME = "dbo.sp_GetAllCandidateDetails_Full";
const CACHE_TTL_MS = 30 * 60 * 1000;
const MAX_CANDIDATES_TO_LLM = 50;
const NAME_LIST_CHAR_CAP = 3000;

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

// ─────────────────────────────────────────────────────────────────────────────
// PUBLIC API
// ─────────────────────────────────────────────────────────────────────────────

export async function warmUp() {
  log.info("Candidate cache warm-up starting...");
  try {
    await loadFromSp();
    scheduleAutoRefresh();
    log.info(
      `Candidate cache ready — ${candidateStore.candidates.length} candidates, cachedAt: ${candidateStore.cachedAt}`,
    );
  } catch (err) {
    log.error(`Candidate cache warm-up failed: ${err.message}`);
    throw err;
  }
}

// ─────────────────────────────────────────────────────────────────────────────
// answerFromCache
// ─────────────────────────────────────────────────────────────────────────────
//
// onChunk — optional async (text: string) => void streaming callback.
//           When provided the answer is streamed token-by-token directly to
//           the caller (e.g. the HTTP response).  The `answer` field in the
//           returned object will be null in this mode — the caller should NOT
//           try to send it again.
//           When omitted the full answer string is returned in result.answer
//           (backward-compatible non-streaming behaviour).
//
export async function answerFromCache(question, options = {}) {
  // Support both calling conventions:
  //   answerFromCache(q, { onChunk })  ← chatbot.service.js
  //   answerFromCache(q, fn)           ← legacy direct callback
  const onChunk = typeof options === "function"
    ? options
    : (options?.onChunk ?? null);
  if (!candidateStore) {
    log.warn("Cache cold — triggering warm-up");
    await warmUp();
  }

  const ageMs = Date.now() - candidateStore.cachedAt.getTime();
  if (ageMs > CACHE_TTL_MS) {
    log.info("Cache stale — background refresh triggered");
    loadFromSp().catch((e) =>
      log.error(`Background refresh failed: ${e.message}`),
    );
  }

  // Step 1: single LLM call — routing + entities + filters + sections
  const analysis = await analyzeQuestion(question, candidateStore.candidates);

  // Post-analysis fix: override OOS intent for known patterns the LLM
  // misclassifies. These are all clearly candidate data queries.
  fixMisclassifiedIntent(analysis, question);

  log.info(`[Step 1] Analysis: ${JSON.stringify(analysis)}`);

  // Non-candidate → signal router
  if (["PDF", "REPORT", "OOS"].includes(analysis.intent)) {
    return {
      success: false,
      forwardIntent: analysis.intent,
      answer: null,
      dataframe: null,
    };
  }

  // interviewRound not in SP — give clear message
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

  // ── COUNT intent ────────────────────────────────────────────────────────
  if (analysis.intent === "COUNT") {
    const hasEntity = Object.values(analysis.entities || {}).some(
      (v) => v != null && v !== "",
    );
    const hasFilter = Object.values(analysis.filters || {}).some(
      (v) => v != null,
    );

    if (!hasEntity && !hasFilter) {
      const total = candidateStore.candidates.length;
      const msg = `There are currently **${total}** candidate${total !== 1 ? "s" : ""} registered in the system (as of ${candidateStore.cachedAt.toLocaleString()}).`;
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

  // ── CANDIDATE intent ─────────────────────────────────────────────────────

  // Detect "highest marks" / "lowest marks" / "top scorer" intent.
  // The LLM returns marksObtained: null for these because "highest" is not a
  // numeric value — we handle it here with a sort instead of a filter.
  const q = question.toLowerCase();
  const isHighestMarks =
    (q.includes("highest") || q.includes("top scorer") || q.includes("most marks") || q.includes("best marks")) &&
    (q.includes("mark") || q.includes("score") || q.includes("marks"));
  const isLowestMarks =
    (q.includes("lowest") || q.includes("least marks") || q.includes("minimum marks")) &&
    (q.includes("mark") || q.includes("score") || q.includes("marks"));

  // Step 2: filter — returns full matched pool (no slice)
  const filtered = filterCandidates(analysis.entities, analysis.filters || {});
  log.info(`[Step 2] Candidates after filter: ${filtered.length} | isHighestMarks: ${isHighestMarks} | isLowestMarks: ${isLowestMarks}`);

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

  // ── Highest / Lowest marks: sort the full pool by best marks, take top N ──
  // This runs before slicing so we always show the genuinely top-scoring
  // candidates, not just whoever happened to be first in the DB.
  let sortedFiltered = filtered;
  if (isHighestMarks || isLowestMarks) {
    // Compute each candidate's best (max) marks across all interviews
    const getBestMarks = (candidate) => {
      const allMarks = (candidate.Applications || []).flatMap((app) =>
        (app.Interviews || [])
          .map((i) => i.marksObtained)
          .filter((m) => m != null)
          .map(Number),
      );
      return allMarks.length > 0 ? Math.max(...allMarks) : -1;
    };

    sortedFiltered = [...filtered].sort((a, b) => {
      const marksA = getBestMarks(a);
      const marksB = getBestMarks(b);
      return isHighestMarks ? marksB - marksA : marksA - marksB;
    });

    // Remove candidates with no marks at all for highest/lowest queries
    sortedFiltered = sortedFiltered.filter((c) => getBestMarks(c) >= 0);

    log.info(`[Marks sort] ${isHighestMarks ? "Highest" : "Lowest"} marks sort applied — ${sortedFiltered.length} candidates with marks`);
  }

  // Step 3: project
  // isSingleCandidate = true when the user asked about a specific person by
  // name. Controls column set (summary vs full) and interview detail level.
  const isSingleCandidate = !!(
    analysis.entities?.candidateName &&
    analysis.entities.candidateName.trim() !== ""
  );

  // Sections to project:
  // • Single candidate + LLM gave specific sections (e.g. ["root","Skills"])
  //   → use exactly those sections — "tell me skills of X" only needs Skills
  // • Single candidate + LLM said "ALL" or gave nothing
  //   → use ALL_SECTIONS (full profile view)
  // • List query → use getSummarySections() (minimal relevant sections)
  const llmSections = analysis.sectionsNeeded;
  const sections = isSingleCandidate
    ? (llmSections === "ALL" || !Array.isArray(llmSections)
        ? ALL_SECTIONS
        : llmSections)
    : getSummarySections(analysis.filters || {}, llmSections);

  const userLimit =
    analysis.limit != null &&
    Number.isInteger(analysis.limit) &&
    analysis.limit > 0
      ? analysis.limit
      : null;

  const effectiveCap = userLimit
    ? Math.min(userLimit, MAX_CANDIDATES_TO_LLM)
    : MAX_CANDIDATES_TO_LLM;

  const slicedForLLM = sortedFiltered.slice(0, effectiveCap);
  const projected = projectData(slicedForLLM, sections);
  log.info(
    `[Step 3] isSingleCandidate: ${isSingleCandidate} | Sections: ${JSON.stringify(sections)} — userLimit: ${userLimit ?? "none"} — sending ${projected.length} of ${filtered.length} candidates to LLM`,
  );

  // Step 4: answer
  // Pass isSingleCandidate so generateNaturalAnswer picks the right column set.
  const answer = await generateNaturalAnswer(
    question,
    `${SP_NAME} (cached at ${candidateStore.cachedAt.toLocaleString()})`,
    projected,
    {
      userLimit,
      totalFound: (isHighestMarks || isLowestMarks) ? sortedFiltered.length : filtered.length,
      isSingleCandidate,
      sectionsNeeded: sections, // tells ai.service.js which fields to show
    },
    onChunk,
  );

  return {
    success: true,
    type: "CACHE",
    answer,
    dataframe: projected,
    cachedAt: candidateStore.cachedAt,
    totalFound: (isHighestMarks || isLowestMarks) ? sortedFiltered.length : filtered.length,
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
  return getCacheStatus();
}

export function getCacheStatus() {
  if (!candidateStore) return { status: "cold", message: "Cache not loaded" };
  const ageMs = Date.now() - candidateStore.cachedAt.getTime();
  return {
    status: ageMs > CACHE_TTL_MS ? "stale" : "warm",
    cachedAt: candidateStore.cachedAt,
    ageMinutes: Math.floor(ageMs / 60000),
    totalCandidates: candidateStore.candidates.length,
    ttlMinutes: CACHE_TTL_MS / 60000,
  };
}

export function clearCache() {
  candidateStore = null;
  if (refreshTimer) {
    clearInterval(refreshTimer);
    refreshTimer = null;
  }
  log.info("Candidate cache cleared");
}

// ─────────────────────────────────────────────────────────────────────────────
// SP LOADER
// ─────────────────────────────────────────────────────────────────────────────

async function loadFromSp() {
  const pool = await getPool();
  const result = await pool.request().execute(SP_NAME);
  const rows = result.recordset || [];
  if (rows.length === 0) log.warn("SP returned 0 rows");
  candidateStore = { cachedAt: new Date(), candidates: rows.map(parseRow) };
  log.info(`SP load complete — ${candidateStore.candidates.length} candidates`);
}

function parseRow(row) {
  return {
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
      log.info("Auto-refresh complete");
    } catch (err) {
      log.error(`Auto-refresh failed: ${err.message}`);
    }
  }, CACHE_TTL_MS);
}

// ─────────────────────────────────────────────────────────────────────────────
// ─────────────────────────────────────────────────────────────────────────────
// fixMisclassifiedIntent
// ─────────────────────────────────────────────────────────────────────────────
// Fixes cases where analyzeQuestion returns OOS/PDF for queries that are
// clearly about candidate data. Mutates the analysis object in place.
//
// These patterns are reliably misclassified by gpt-4o-mini:
//   • birthdate / age range queries → CANDIDATE + birthYear filters
//   • graduation year queries        → CANDIDATE + ugGraduationYear filter
//   • interviewer queries            → CANDIDATE + interviewer filter
//
function fixMisclassifiedIntent(analysis, question) {
  const q = question.toLowerCase();

  // ── Birthdate / age range ─────────────────────────────────────────────────
  const birthdatePatterns = [
    /birth\s*date/i, /date\s*of\s*birth/i, /dob/i,
    /born\s*(in|between|after|before|from)/i,
    /age\s*(between|from|above|below|under|over)/i,
    /born\s*\d{4}/i,
  ];
  const hasBirthdatePattern = birthdatePatterns.some((r) => r.test(question));

  // Also detect "between YEAR to/and YEAR" or "from YEAR to YEAR"
  const yearRangeMatch = question.match(
    /(?:between|from)\s+(\d{4})\s+(?:to|and|-)\s+(\d{4})/i
  );

  if (hasBirthdatePattern || (yearRangeMatch && q.includes("birth"))) {
    if (analysis.intent === "OOS" || analysis.intent === "PDF") {
      analysis.intent = "CANDIDATE";
      log.info(`[IntentFix] Birthdate query overridden → CANDIDATE`);
    }
    // Always set Profile section for birthdate queries
    if (!Array.isArray(analysis.sectionsNeeded) || !analysis.sectionsNeeded.includes("Profile")) {
      analysis.sectionsNeeded = ["root", "Profile"];
    }

    // ALWAYS overwrite birthYear filters from question text — never trust
    // the LLM values which can be swapped or wrong for range queries.
    if (yearRangeMatch) {
      const y1 = parseInt(yearRangeMatch[1], 10);
      const y2 = parseInt(yearRangeMatch[2], 10);
      // Always assign smaller year to From, larger to To regardless of order
      analysis.filters.birthYearFrom = Math.min(y1, y2);
      analysis.filters.birthYearTo   = Math.max(y1, y2);
      // Clear conflicting single year
      analysis.filters.birthYear = null;
      log.info(`[IntentFix] birthYearFrom: ${analysis.filters.birthYearFrom}, birthYearTo: ${analysis.filters.birthYearTo}`);
    } else {
      // Single year — extract from question, e.g. "born in 2001", "birthdate 2001"
      const allYears = [...question.matchAll(/(19|20)\d{2}/g)].map((m) => parseInt(m[0], 10));
      if (allYears.length === 1 && !analysis.filters.birthYear) {
        analysis.filters.birthYear = allYears[0];
        analysis.filters.birthYearFrom = null;
        analysis.filters.birthYearTo   = null;
        log.info(`[IntentFix] birthYear: ${analysis.filters.birthYear}`);
      }
    }
  }

  // ── Graduation year — "completed study in YEAR" / "graduated in YEAR" ─────
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
    // Extract year if not already set
    const yearMatch = question.match(/(19|20)\d{2}/);
    if (yearMatch && !analysis.filters.ugGraduationYear) {
      analysis.filters.ugGraduationYear = parseInt(yearMatch[0], 10);
      analysis.filters.isStudying = null; // never set isStudying for grad year queries
      log.info(`[IntentFix] ugGraduationYear: ${analysis.filters.ugGraduationYear}`);
    }
  }

  // ── PDF / policy questions misclassified as OOS ──────────────────────────
  // Detect HR policy/process questions that should go to PDF route
  // HR/policy question keywords — any question containing these about HR topics
  // should be PDF, never OOS
  const pdfKeywords = [
    "recruitment process", "recruitment management", "recruitment policy",
    "hiring process", "hiring policy", "hiring procedure",
    "interview process", "interview policy", "interview procedure",
    "onboarding process", "onboarding policy", "pre-boarding", "pre boarding",
    "selection process", "selection criteria", "selection policy",
    "online communication training", "communication training",
    "hr policy", "hr process", "hr procedure", "hr guideline",
    "what is recruitment", "how does recruitment", "explain recruitment",
    "what is hiring", "how does hiring", "explain hiring",
    "what is interview", "how does interview", "explain interview",
    "what is onboarding", "what is pre-boarding",
  ];
  const qLower = question.toLowerCase();
  const isPdfQuestion = pdfKeywords.some((kw) => qLower.includes(kw));

  if (analysis.intent === "OOS" && isPdfQuestion) {
    analysis.intent = "PDF";
    log.info(`[IntentFix] PDF policy question overridden from OOS → PDF`);
  }

  // ── Interviewer — "interview taken by X" / "interviewed by X" ─────────────
  const interviewerMatch = question.match(
    /(?:interview\w*\s+(?:taken\s+)?by|interviewed\s+by|interviewer\s+(?:is\s+)?)\s+(.+?)(?:\s*$|\s*\?)/i
  );
  if (interviewerMatch) {
    if (analysis.intent === "OOS" || analysis.intent === "PDF") {
      analysis.intent = "CANDIDATE";
      analysis.sectionsNeeded = ["root", "Applications", "Applications.Interviews"];
      log.info(`[IntentFix] Interviewer query overridden → CANDIDATE`);
    }
    if (!analysis.filters.interviewer) {
      analysis.filters.interviewer = interviewerMatch[1].trim();
      analysis.filters.hasInterview = true;
      log.info(`[IntentFix] interviewer: "${analysis.filters.interviewer}"`);
    }
  }
}

// analyzeQuestion — THE SINGLE LLM CALL
// ─────────────────────────────────────────────────────────────────────────────

async function analyzeQuestion(question, candidates) {
  const client = getOpenAIClient();

  const nameList = candidates
    .map((c) => c.FullName)
    .filter(Boolean)
    .reduce((acc, name) => {
      const next = acc ? `${acc}, ${name}` : name;
      return next.length > NAME_LIST_CHAR_CAP ? acc : next;
    }, "");

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
    "resumeCountOperator": "eq"|"gt"|"lt"|"gte"|"lte" or null
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

────────────────────────────────────────
FILTER RULES — populate "filters" based on what the user asks:

ROOT:
  "verified candidates"           → isVerified: true
  "unverified candidates"         → isVerified: false
  "complete profile"              → isProfileComplete: true
  "incomplete profile"            → isProfileComplete: false

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

APPLICATIONS:
  "shortlisted"                   → isShortlisted: true
  "not shortlisted"               → isShortlisted: false
  "accepted"                      → isAccepted: true
  "rejected"                      → applicationStatus: "Rejected"
  "selected"                      → applicationStatus: "Selected"
  "applied"                       → hasApplied: true

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

RESUMES:
  "has resume" / "uploaded resume"   → hasResume: true
  "no resume" / "missing resume"     → hasResume: false
  "uploaded 2 resumes"               → resumeCount: 2,  resumeCountOperator: "eq"
  "more than 1 resume"               → resumeCount: 1,  resumeCountOperator: "gt"
  "at least 3 resumes"               → resumeCount: 3,  resumeCountOperator: "gte"
  "less than 2 resumes"              → resumeCount: 2,  resumeCountOperator: "lt"
  "exactly 1 resume"                 → resumeCount: 1,  resumeCountOperator: "eq"

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

User question: "${question.replace(/"/g, '\\"')}"
JSON:`.trim();

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

    const rawLimit = parsed.limit;
    const safeLimit =
      rawLimit != null &&
      Number.isFinite(Number(rawLimit)) &&
      Number(rawLimit) > 0
        ? Math.floor(Number(rawLimit))
        : null;

    const rawEntities = parsed.entities || {};
    const rawFilters  = parsed.filters  || {};

    // ── Normalise misplaced fields ──────────────────────────────────────────
    // The LLM sometimes puts filter-type values (gender, city, state, skill,
    // language) inside "entities" instead of "filters".  Move them to the
    // correct bucket so filterCandidates() picks them up.
    const normEntities = { ...rawEntities };
    const normFilters  = { ...rawFilters, interviewRound: rawFilters.interviewRound || null };

    // gender → filters.gender
    if (normEntities.gender != null && normEntities.gender !== "") {
      normFilters.gender = normFilters.gender || normEntities.gender;
      delete normEntities.gender;
    }
    // language → filters.languageKnown
    if (normEntities.language != null && normEntities.language !== "") {
      normFilters.languageKnown = normFilters.languageKnown || normEntities.language;
      delete normEntities.language;
    }
    if (normEntities.languageKnown != null && normEntities.languageKnown !== "") {
      normFilters.languageKnown = normFilters.languageKnown || normEntities.languageKnown;
      delete normEntities.languageKnown;
    }
    // isStudying / isVerified / isProfileComplete → filters
    ["isStudying","isVerified","isProfileComplete","hasExperience","isCurrentlyWorking",
     "isShortlisted","isAccepted","hasInterview","hasResume"].forEach((key) => {
      if (normEntities[key] != null) {
        normFilters[key] = normFilters[key] ?? normEntities[key];
        delete normEntities[key];
      }
    });

    // interviewer → filters.interviewer
    if (normEntities.interviewer != null && normEntities.interviewer !== "") {
      normFilters.interviewer = normFilters.interviewer || normEntities.interviewer;
      delete normEntities.interviewer;
    }

    // Also move any remaining filter-type keys that landed in entities by mistake.
    // These are all the known filter keys — if any appear in entities, move them.
    const FILTER_KEYS = [
      "interviewer", "interviewStatus", "applicationStatus", "isQualified",
      "hasFeedback", "marksObtained", "marksOperator", "hasAadhar", "hasPanCard",
      "hasBankPassbook", "hasBankStatement", "hasSalarySlip", "hasExperienceLetter",
      "hasOfferLetter", "hasItr", "resumeCount", "resumeCountOperator",
      "hasLinkedin", "hasPortfolio", "ugDegree", "pgDegree", "hasPostGraduation",
      "ugGraduationYear", "pgGraduationYear", "hasApplied",
      "birthYear", "birthYearFrom", "birthYearTo",
    ];
    FILTER_KEYS.forEach((key) => {
      if (normEntities[key] != null) {
        normFilters[key] = normFilters[key] ?? normEntities[key];
        delete normEntities[key];
      }
    });

    // skill stays in entities (used by matchesEntity for skill matching)
    // city / state stay in entities (used by matchesEntity for location matching)
    // candidateName / email / jobTitle / companyName / universityName stay in entities

    log.info(`[Normalised] entities: ${JSON.stringify(normEntities)} | filters: ${JSON.stringify(normFilters)}`);

    return {
      intent: parsed.intent || "CANDIDATE",
      limit: safeLimit,
      entities: normEntities,
      filters: normFilters,
      sectionsNeeded: parsed.sectionsNeeded || "ALL",
    };
  } catch (err) {
    log.error(
      `analyzeQuestion failed: ${err.message} — defaulting to CANDIDATE/ALL`,
    );
    return {
      intent: "CANDIDATE",
      limit: null,
      entities: {},
      filters: {},
      sectionsNeeded: "ALL",
    };
  }
}

// ─────────────────────────────────────────────────────────────────────────────
// FILTER CANDIDATES
// ─────────────────────────────────────────────────────────────────────────────

function filterCandidates(entities, filters = {}) {
  if (!candidateStore) return [];
  const candidates = candidateStore.candidates;
  const hasEntity = Object.values(entities).some((v) => v != null && v !== "");
  const hasFilter = Object.values(filters).some((v) => v != null);

  let pool = hasEntity
    ? candidates.filter((c) => matchesEntity(c, entities))
    : candidates;
  if (hasFilter) pool = pool.filter((c) => matchesFilters(c, filters));
  return pool;
}

// ─────────────────────────────────────────────────────────────────────────────
// ENTITY MATCHING  (AND logic — all supplied entities must match)
// ─────────────────────────────────────────────────────────────────────────────

function matchesEntity(candidate, entities) {
  const checks = [];

  if (entities.candidateName != null && entities.candidateName !== "") {
    const search = entities.candidateName.toLowerCase().trim();
    const fullName = (candidate.FullName || "").toLowerCase();

    // Split search into words, keep all words (even short ones like "P.")
    const words = search.split(/\s+/).filter((w) => w.length > 0);

    // 1. Direct substring match — handles "naresh prajapati" → "Naresh Prajapati"
    const directMatch = fullName.includes(search);

    // 2. Multi-word AND match — ALL words must appear in the full name.
    //    "naresh prajapati" → both "naresh" AND "prajapati" must be in the name.
    //    This is the critical fix: prevents "Kalash Prajapati" from matching
    //    when the user searched for "naresh prajapati".
    //    Only meaningful words (length > 1) are required to all match.
    const meaningfulWords = words.filter((w) => w.length > 1);
    const multiWordMatch = meaningfulWords.length >= 2
      && meaningfulWords.every((w) => fullName.includes(w));

    // 3. Single-word search → partial match (e.g. "avinash" matches "Avinash Patel")
    //    Only used when the user typed a single name token.
    const singleWordMatch = meaningfulWords.length === 1
      && fullName.includes(meaningfulWords[0]);

    // Strict priority: direct match first, then multi-word AND, then single word.
    // Never fall back to single-word if a multi-word search was given —
    // that is what caused "Kalash Prajapati" to appear for "naresh prajapati".
    const nameMatch = meaningfulWords.length >= 2
      ? (directMatch || multiWordMatch)   // multi-word: require BOTH words
      : (directMatch || singleWordMatch); // single-word: partial is fine

    checks.push(nameMatch);
  }

  if (entities.email != null && entities.email !== "") {
    checks.push(
      (candidate.email || "").toLowerCase() === entities.email.toLowerCase(),
    );
  }

  if (entities.city != null && entities.city !== "") {
    checks.push(
      (candidate.Profile?.city || "")
        .toLowerCase()
        .includes(entities.city.toLowerCase()),
    );
  }

  if (entities.state != null && entities.state !== "") {
    checks.push(
      (candidate.Profile?.state || "")
        .toLowerCase()
        .includes(entities.state.toLowerCase()),
    );
  }

  if (entities.skill != null && entities.skill !== "") {
    const skills = (candidate.Skills || []).map((s) =>
      (s.Skill || "").toLowerCase(),
    );
    checks.push(skills.some((sk) => sk.includes(entities.skill.toLowerCase())));
  }

  if (entities.jobTitle != null && entities.jobTitle !== "") {
    const jobs = (candidate.Applications || []).map((a) =>
      (a.JobTitle || "").toLowerCase(),
    );
    const titleQuery = entities.jobTitle.toLowerCase();
    const titleWords = titleQuery.split(/\s+/).filter((w) => w.length > 1);
    const jobMatch =
      jobs.some((j) => j.includes(titleQuery)) ||
      (titleWords.length > 0 &&
        jobs.some((j) =>
          titleWords.length <= 2
            ? titleWords.some((w) => j.includes(w))
            : titleWords.every((w) => j.includes(w)),
        ));
    checks.push(jobMatch);
  }

  if (entities.companyName != null && entities.companyName !== "") {
    const companies = (candidate.Experience || []).map((e) =>
      (e.companyName || "").toLowerCase(),
    );
    const companyQuery = entities.companyName.toLowerCase();
    const companyWords = companyQuery.split(/\s+/).filter((w) => w.length > 2);
    const companyMatch =
      companies.some((co) => co.includes(companyQuery)) ||
      (companyWords.length > 0 &&
        companies.some((co) => companyWords.every((w) => co.includes(w))));
    checks.push(companyMatch);
  }

  if (entities.universityName != null && entities.universityName !== "") {
    const uniQuery = entities.universityName.toLowerCase().trim();
    const edu = candidate.Education || [];
    const allUnis = edu
      .flatMap((e) => [
        (e.underGraduationUniversityName || "").toLowerCase(),
        (e.postGraduationUniversityName || "").toLowerCase(),
      ])
      .filter(Boolean);
    const uniWords = uniQuery.split(/\s+/).filter((w) => w.length > 2);
    const uniMatch =
      allUnis.some((u) => u.includes(uniQuery)) ||
      (uniWords.length > 0 &&
        allUnis.some((u) =>
          uniWords.length <= 2
            ? uniWords.some((w) => u.includes(w))
            : uniWords.every((w) => u.includes(w)),
        ));
    checks.push(uniMatch);
  }

  return checks.length === 0 ? false : checks.every(Boolean);
}

function matchesFilters(candidate, filters) {
  // ── ROOT ──────────────────────────────────────────────────────────────────
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

  // ── PROFILE ───────────────────────────────────────────────────────────────
  if (filters.isStudying != null) {
    // Skip isStudying filter when a graduation year is also set.
    // "completed study in 2019" → LLM sets isStudying: false, but many
    // candidates have isStudying = null (not filled in). The graduation
    // year filter is more accurate for this intent.
    const hasGradYearFilter =
      filters.ugGraduationYear != null || filters.pgGraduationYear != null;
    if (!hasGradYearFilter) {
      // Only apply isStudying when the profile field is explicitly set (not null).
      // Candidates who never filled in isStudying should not be excluded.
      const val = candidate.Profile?.isStudying;
      if (val != null && Boolean(val) !== Boolean(filters.isStudying))
        return false;
    }
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

  // ── BIRTHDATE / AGE ───────────────────────────────────────────────────────
  // dateOfBirth is stored as ISO string e.g. "2000-05-15T18:30:00Z"
  // We extract the birth year and compare against the filter.
  if (filters.birthYear != null || filters.birthYearFrom != null || filters.birthYearTo != null) {
    const dob = candidate.Profile?.dateOfBirth;
    // If no DOB recorded, exclude — we can't verify the birthdate range
    if (!dob || dob === "" || dob === null) return false;
    const birthYear = parseInt(String(dob).split("-")[0], 10);
    if (isNaN(birthYear) || birthYear < 1900 || birthYear > 2100) return false;

    if (filters.birthYear != null && birthYear !== Number(filters.birthYear)) return false;
    if (filters.birthYearFrom != null && birthYear < Number(filters.birthYearFrom)) return false;
    if (filters.birthYearTo != null && birthYear > Number(filters.birthYearTo)) return false;
  }

  // ── EDUCATION ─────────────────────────────────────────────────────────────
  if (filters.ugDegree) {
    const edu = candidate.Education || [];
    const ugQuery = filters.ugDegree.toLowerCase().trim();

    const UG_ALIASES = {
      "btech":    ["bachelor of technology", "b.tech", "b.e."],
      "be":       ["bachelor of engineering", "b.e."],
      "bca":      ["bachelor of computer", "bca"],
      "bsc":      ["bachelor of science", "b.sc"],
      "ba":       ["bachelor of arts", "b.a."],
      "bcom":     ["bachelor of commerce", "b.com"],
      "bba":      ["bachelor of business", "bba", "bbm"],
      "bbm":      ["bachelor of business", "bbm"],
      "barch":    ["bachelor of architecture", "b.arch"],
      "bachelors":["bachelor"],
      "bachelor": ["bachelor"],
      "degree":   ["bachelor", "master"],
      "graduation":["bachelor"],
      "undergrad":["bachelor"],
    };

    const keywords = UG_ALIASES[ugQuery] || [ugQuery];
    const match = edu.some((e) => {
      const deg = (e.UnderGraduationDegree || "").toLowerCase();
      return keywords.some((kw) => deg.includes(kw));
    });
    if (!match) return false;
  }
  if (filters.pgDegree) {
    const edu = candidate.Education || [];
    const pgQuery = filters.pgDegree.toLowerCase().trim();

    // Expand generic degree terms to keywords that actually appear in DB values.
    // e.g. "Masters" → matches "Master of Technology", "Master of Arts", "M.Tech" etc.
    const PG_ALIASES = {
      "masters":  ["master"],
      "master":   ["master"],
      "master degree": ["master"],
      "masters degree": ["master"],
      "pg":       ["master", "m."],
      "mtech":    ["master of technology", "m.tech", "m.e."],
      "mba":      ["master of business", "mba"],
      "mca":      ["master of computer", "mca"],
      "msc":      ["master of science", "m.sc"],
      "ma":       ["master of arts", "m.a."],
      "mcom":     ["master of commerce", "m.com"],
      "me":       ["master of engineering", "m.e."],
      "march":    ["master of architecture", "m.arch"],
    };

    const keywords = PG_ALIASES[pgQuery] || [pgQuery];
    const match = edu.some((e) => {
      const deg = (e.PostGraduationDegree || "").toLowerCase();
      return keywords.some((kw) => deg.includes(kw));
    });
    if (!match) return false;
  }
  if (filters.hasPostGraduation != null) {
    // Skip this check if pgDegree is already set — pgDegree implies hasPostGraduation.
    // This prevents double-filtering where pgDegree passes but hasPostGraduation
    // runs a stricter check and rejects the same candidate.
    if (!filters.pgDegree) {
      const edu = candidate.Education || [];
      const hasPg = edu.some(
        (e) => e.PostGraduationDegree && e.PostGraduationDegree.trim() !== "",
      );
      if (hasPg !== Boolean(filters.hasPostGraduation)) return false;
    }
  }
  if (filters.ugGraduationYear != null) {
    const edu = candidate.Education || [];
    const target = Number(filters.ugGraduationYear);

    const match = edu.some((e) => {
      const ugRaw = e.underGraduationEndYear;
      const pgRaw = e.postGraduationEndYear;

      // Handle all possible storage formats:
      // number: 2019, float: 2019.0, string: "2019", "2019-01-01", null, ""
      const parseYear = (val) => {
        if (val == null || val === "" || val === "null") return null;
        // If it looks like a date string "2019-01-01", extract the year part
        if (typeof val === "string" && val.includes("-")) {
          const y = parseInt(val.split("-")[0], 10);
          return isNaN(y) ? null : y;
        }
        const n = parseInt(String(val), 10);
        return isNaN(n) ? null : n;
      };

      const ugYear = parseYear(ugRaw);
      const pgYear = parseYear(pgRaw);

      return ugYear === target ||
        (filters.pgGraduationYear == null && pgYear === target);
    });
    if (!match) return false;
  }
  if (filters.pgGraduationYear != null) {
    const edu = candidate.Education || [];
    const target = Number(filters.pgGraduationYear);
    const match = edu.some((e) => {
      const raw = e.postGraduationEndYear;
      if (raw == null || raw === "" || raw === "null") return false;
      if (typeof raw === "string" && raw.includes("-")) {
        return parseInt(raw.split("-")[0], 10) === target;
      }
      return parseInt(String(raw), 10) === target;
    });
    if (!match) return false;
  }

  // ── EXPERIENCE ────────────────────────────────────────────────────────────
  if (filters.isCurrentlyWorking != null) {
    const realExp = (candidate.Experience || []).filter(
      (e) => e.companyName && e.companyName.trim() !== "",
    );
    const working = realExp.some((e) => Boolean(e.isCurrentCompany));
    if (working !== Boolean(filters.isCurrentlyWorking)) return false;
  }
  if (filters.hasExperience != null) {
    const hasExp = (candidate.Experience || []).some(
      (e) => e.companyName && e.companyName.trim() !== "",
    );
    if (hasExp !== Boolean(filters.hasExperience)) return false;
  }

  // ── APPLICATIONS ──────────────────────────────────────────────────────────
  if (filters.isShortlisted != null) {
    const match = (candidate.Applications || []).some(
      (a) => Boolean(a.isShortlisted) === Boolean(filters.isShortlisted),
    );
    if (!match) return false;
  }
  if (filters.isAccepted != null) {
    const match = (candidate.Applications || []).some(
      (a) => Boolean(a.isAccepted) === Boolean(filters.isAccepted),
    );
    if (!match) return false;
  }
  if (filters.hasApplied != null) {
    const hasApp = (candidate.Applications || []).length > 0;
    if (hasApp !== Boolean(filters.hasApplied)) return false;
  }
  if (filters.applicationStatus) {
    const target = filters.applicationStatus.toLowerCase();
    const match = (candidate.Applications || []).some((a) =>
      (a.Status || "").toLowerCase().includes(target),
    );
    if (!match) return false;
  }

  // ── INTERVIEWS ────────────────────────────────────────────────────────────
  if (filters.isQualified != null) {
    const match = (candidate.Applications || []).some((app) =>
      (app.Interviews || []).some(
        (i) => Boolean(i.isQualified) === Boolean(filters.isQualified),
      ),
    );
    if (!match) return false;
  }
  if (filters.hasInterview != null) {
    const allInterviews = (candidate.Applications || []).flatMap(
      (a) => a.Interviews || [],
    );
    const hasIt = allInterviews.length > 0;
    if (hasIt !== Boolean(filters.hasInterview)) return false;
  }
  if (filters.hasFeedback != null) {
    const allInterviews = (candidate.Applications || []).flatMap(
      (a) => a.Interviews || [],
    );
    const hasIt = allInterviews.some(
      (i) => i.feedback && i.feedback.trim() !== "",
    );
    if (hasIt !== Boolean(filters.hasFeedback)) return false;
  }
  if (filters.interviewer) {
    const target = filters.interviewer.toLowerCase().trim();
    const match = (candidate.Applications || []).some((app) =>
      (app.Interviews || []).some((i) =>
        (i.Interviewer || "").toLowerCase().includes(target),
      ),
    );
    if (!match) return false;
  }
  if (filters.interviewStatus) {
    const target = filters.interviewStatus.toLowerCase();
    const match = (candidate.Applications || []).some((app) =>
      (app.Interviews || []).some((i) =>
        (i.InterviewStatus || "").toLowerCase().includes(target),
      ),
    );
    if (!match) return false;
  }
  if (filters.marksObtained != null) {
    const target = Number(filters.marksObtained);
    const op = filters.marksOperator || "eq";
    const allMarks = (candidate.Applications || []).flatMap((app) =>
      (app.Interviews || [])
        .map((i) => i.marksObtained)
        .filter((m) => m != null)
        .map(Number),
    );
    if (allMarks.length === 0) return false;
    const passes = allMarks.some((m) => {
      // Use Math.round to handle float storage (e.g. 30.0 === 30)
      const mRounded = Math.round(m * 100) / 100;
      if (op === "eq") return Math.abs(mRounded - target) < 0.01;
      if (op === "gt") return mRounded > target;
      if (op === "lt") return mRounded < target;
      if (op === "gte") return mRounded >= target;
      if (op === "lte") return mRounded <= target;
      return Math.abs(mRounded - target) < 0.01;
    });
    if (!passes) return false;
  }

  // ── DOCUMENTS ─────────────────────────────────────────────────────────────
  const doc = candidate.Documents || {};
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

  // ── RESUMES ───────────────────────────────────────────────────────────────
  if (filters.hasResume != null) {
    const hasIt = (candidate.Resumes || []).some(
      (r) => r.resume && r.resume.trim() !== "",
    );
    if (hasIt !== Boolean(filters.hasResume)) return false;
  }
  if (filters.resumeCount != null) {
    const count = (candidate.Resumes || []).filter(
      (r) => r.resume && r.resume.trim() !== "",
    ).length;
    const target = Number(filters.resumeCount);
    const op = filters.resumeCountOperator || "eq";
    const passes =
      op === "eq"
        ? count === target
        : op === "gt"
          ? count > target
          : op === "lt"
            ? count < target
            : op === "gte"
              ? count >= target
              : op === "lte"
                ? count <= target
                : count === target;
    if (!passes) return false;
  }

  return true;
}

// ─────────────────────────────────────────────────────────────────────────────
// getSummarySections
// ─────────────────────────────────────────────────────────────────────────────
// Returns the minimal set of DB sections needed for a LIST query.
// For a single-candidate detail query, ALL_SECTIONS is used instead.
//
// Rules:
//   • Always include root + Profile (name, city, state are always useful).
//   • Include Skills always (useful in every list).
//   • Include Education only when an education filter was applied.
//   • Include Experience only when an experience filter was applied.
//   • Include Applications (+Interviews) when any application/interview
//     filter was applied, OR when the LLM sectionsNeeded said to include it.
//   • Never include Documents or Resumes for list views — not useful in a table.

function getSummarySections(filters, llmSectionsNeeded) {
  const sections = new Set(["root", "Profile", "Skills"]);

  // Birthdate filters — always include Profile section
  if (filters.birthYear != null || filters.birthYearFrom != null || filters.birthYearTo != null) {
    sections.add("Profile");
  }

  // Education filters — always include Education section when any edu filter is set
  if (
    filters.ugDegree ||
    filters.pgDegree ||
    filters.hasPostGraduation != null ||
    filters.ugGraduationYear != null ||
    filters.pgGraduationYear != null
  ) {
    sections.add("Education");
  }

  // Also add Education when sectionsNeeded from LLM is missing it but
  // we have education-related filters — prevents "0 results" from missing data
  if ((filters.pgDegree || filters.ugDegree || filters.hasPostGraduation != null)
      && Array.isArray(llmSectionsNeeded)
      && !llmSectionsNeeded.includes("Education")) {
    sections.add("Education");
  }

  // Experience filters
  if (filters.hasExperience != null || filters.isCurrentlyWorking != null) {
    sections.add("Experience");
  }

  // Application / interview filters
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

  // Also respect specific section hints from the LLM (but never Documents/Resumes)
  if (Array.isArray(llmSectionsNeeded)) {
    const allowed = new Set(["root", "Profile", "Skills", "Education", "Experience", "Applications", "Applications.Interviews"]);
    llmSectionsNeeded.forEach((s) => { if (allowed.has(s)) sections.add(s); });
  }

  // Always include Applications for shortlisted/accepted display
  if (!sections.has("Applications")) {
    sections.add("Applications");
  }

  return [...sections];
}

// ─────────────────────────────────────────────────────────────────────────────
// PROJECT DATA
// ─────────────────────────────────────────────────────────────────────────────

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

// ─────────────────────────────────────────────────────────────────────────────
// HELPERS
// ─────────────────────────────────────────────────────────────────────────────

function buildFilterLabel(entities, filters) {
  if (entities?.candidateName) return `named "${entities.candidateName}"`;
  if (entities?.jobTitle) return `applied for "${entities.jobTitle}"`;
  if (entities?.skill) return `with skill "${entities.skill}"`;
  if (entities?.city) return `from "${entities.city}"`;
  if (entities?.companyName) return `who worked at "${entities.companyName}"`;
  if (entities?.universityName)
    return `from "${entities.universityName}" university/college`;
  if (filters.isStudying === true) return "currently studying";
  if (filters.isStudying === false) return "not currently studying";
  if (filters.isShortlisted === true) return "shortlisted";
  if (filters.isVerified === true) return "verified";
  if (filters.isVerified === false) return "unverified";
  if (filters.isProfileComplete === true) return "with complete profile";
  if (filters.isProfileComplete === false) return "with incomplete profile";
  if (filters.hasExperience === false)
    return "with no work experience (freshers)";
  if (filters.hasExperience === true) return "with work experience";
  if (filters.isCurrentlyWorking === true) return "currently working";
  if (filters.gender) return `who are ${filters.gender}`;
  if (filters.languageKnown) return `who speak ${filters.languageKnown}`;
  if (filters.ugDegree) return `with ${filters.ugDegree} degree`;
  if (filters.pgDegree) return `with ${filters.pgDegree} degree`;
  if (filters.hasPostGraduation === true) return "with post-graduation degree";
  if (filters.hasPostGraduation === false)
    return "without post-graduation degree";
  if (filters.applicationStatus)
    return `with application status "${filters.applicationStatus}"`;
  if (filters.interviewStatus)
    return `with interview status "${filters.interviewStatus}"`;
  if (filters.isQualified === true) return "who qualified the interview";
  if (filters.isQualified === false) return "who did not qualify the interview";
  if (filters.marksObtained != null) {
    const opLabel = {
      eq: "exactly",
      gt: "more than",
      lt: "less than",
      gte: "at least",
      lte: "at most",
    }[filters.marksOperator || "eq"];
    return `who scored ${opLabel} ${filters.marksObtained} marks`;
  }
  if (filters.hasResume === true) return "with uploaded resume";
  if (filters.hasResume === false) return "with no resume uploaded";
  if (filters.hasAadhar === false) return "with missing Aadhar card";
  if (filters.hasPanCard === false) return "with missing PAN card";
  return "matching your query";
}