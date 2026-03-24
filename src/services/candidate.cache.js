// candidate.cache.js

import { getPool } from "../db/connection.js";
import { getOpenAIClient, generateNaturalAnswer } from "./ai.service.js";
import { log } from "../utils/logger.js";

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

// ─────────────────────────────────────────────────────────────────────────────
// MODULE STATE
// ─────────────────────────────────────────────────────────────────────────────

let candidateStore = null;         
let refreshTimer = null;
let warmUpPromise = null;          
let lastRefreshError = null;       
let candidateIndex = null;

const analysisCache = new Map();


let lastResolvedContext = {};      

// ─────────────────────────────────────────────────────────────────────────────
// PUBLIC API
// ─────────────────────────────────────────────────────────────────────────────

export async function warmUp() {
 
  if (warmUpPromise) return warmUpPromise;
  warmUpPromise = _doWarmUp().finally(() => { warmUpPromise = null; });
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
      log.error(`Warm-up attempt ${attempt}/${MAX_ATTEMPTS} failed: ${err.message}`);
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


export async function answerFromCache(question, options = {}) {
 
  
  const onChunk = typeof options === "function"
    ? options
    : (options?.onChunk ?? null);

  
  question = String(question).trim().slice(0, 500);
 
  question = question.replace(/[`\\]/g, " ");

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

  
  const analysis = await analyzeQuestion(question, candidateStore.candidates);

  

  if (!analysis.entities.candidateName && lastResolvedContext.candidateName) {
    const pronounPattern = /\b(he|she|they|his|her|their|them|this person|the candidate|this candidate)\b/i;
    if (pronounPattern.test(question)) {
      analysis.entities.candidateName = lastResolvedContext.candidateName;
      log.info(`[ContextInherit] Carried forward candidateName: "${lastResolvedContext.candidateName}"`);
    }
  }

  
  fixMisclassifiedIntent(analysis, question);

  log.info(`[Step 1] Analysis: ${JSON.stringify(analysis)}`);

  
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

  const q = question.toLowerCase();
  const isHighestMarks =
    (q.includes("highest") || q.includes("top scorer") || q.includes("most marks") || q.includes("best marks")) &&
    (q.includes("mark") || q.includes("score") || q.includes("marks"));
  const isLowestMarks =
    (q.includes("lowest") || q.includes("least marks") || q.includes("minimum marks")) &&
    (q.includes("mark") || q.includes("score") || q.includes("marks"));

  
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

  
  let sortedFiltered = filtered;
  if (isHighestMarks || isLowestMarks) {
    const getBestMarks = (candidate) => {
      const allMarks = (candidate.Applications || []).flatMap((app) =>
        (Array.isArray(app.Interviews) ? app.Interviews : [])
          .map((i) => parseFloat(i.marksObtained))
          .filter((m) => !isNaN(m)),
      );
      return allMarks.length > 0 ? Math.max(...allMarks) : -1;
    };

    sortedFiltered = [...filtered].sort((a, b) => {
      const marksA = getBestMarks(a);
      const marksB = getBestMarks(b);
      return isHighestMarks ? marksB - marksA : marksA - marksB;
    });

    sortedFiltered = sortedFiltered.filter((c) => getBestMarks(c) >= 0);
    log.info(`[Marks sort] ${isHighestMarks ? "Highest" : "Lowest"} marks sort — ${sortedFiltered.length} candidates with marks`);
  }

  // Step 3: project
  const isSingleCandidate = !!(
    analysis.entities?.candidateName &&
    analysis.entities.candidateName.trim() !== ""
  );

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

  if (analysis.entities?.candidateName) {
    lastResolvedContext = {
      candidateName: analysis.entities.candidateName,
      jobTitle: analysis.entities.jobTitle ?? lastResolvedContext.jobTitle,
    };
  }

  // Step 4: answer
  const responseType = detectResponseType(question);
  const answer = await generateNaturalAnswer(
    question,
    `${SP_NAME} (cached at ${candidateStore.cachedAt.toLocaleString()})`,
    projected,
    {
      userLimit,
      totalFound: (isHighestMarks || isLowestMarks) ? sortedFiltered.length : filtered.length,
      isSingleCandidate,
      sectionsNeeded: sections,
      responseType:responseType,
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
  lastResolvedContext = {};
  analysisCache.clear();
  if (refreshTimer) {
    clearInterval(refreshTimer);
    refreshTimer = null;
  }
  log.info("Candidate cache cleared");
}

export function resetConversationContext() {
  lastResolvedContext = {};
  log.info("Conversation context reset");
}

// ─────────────────────────────────────────────────────────────────────────────
// SP LOADER
// ─────────────────────────────────────────────────────────────────────────────

async function loadFromSp() {
  const pool = await getPool();
  const result = await pool.request().execute(SP_NAME);
  const rows = result.recordset || [];

  if (!rows || rows.length === 0) {
    log.warn("SP returned 0 rows");
  }

  // ✅ FIX: always define parsed properly
  const parsedCandidates = rows.map(parseRow);

  // ✅ assign store
  candidateStore = {
    cachedAt: new Date(),
    candidates: parsedCandidates,
  };

  // ✅ build index
  candidateIndex = buildIndex(parsedCandidates);

  log.info(`SP load complete — ${parsedCandidates.length} candidates`);
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
      const skill = (s.Skill || "").toLowerCase().trim();
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

  return index;
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
    /birth\s*date/i, /date\s*of\s*birth/i, /dob/i,
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
    if (!Array.isArray(analysis.sectionsNeeded) || !analysis.sectionsNeeded.includes("Profile")) {
      analysis.sectionsNeeded = ["root", "Profile"];
    }
    if (yearRangeMatch) {
      const y1 = parseInt(yearRangeMatch[1], 10);
      const y2 = parseInt(yearRangeMatch[2], 10);
      analysis.filters.birthYearFrom = Math.min(y1, y2);
      analysis.filters.birthYearTo   = Math.max(y1, y2);
      analysis.filters.birthYear = null;
      log.info(`[IntentFix] birthYearFrom: ${analysis.filters.birthYearFrom}, birthYearTo: ${analysis.filters.birthYearTo}`);
    } else {
      const allYears = [...question.matchAll(/(19|20)\d{2}/g)].map((m) => parseInt(m[0], 10));
      if (allYears.length === 1 && !analysis.filters.birthYear) {
        analysis.filters.birthYear = allYears[0];
        analysis.filters.birthYearFrom = null;
        analysis.filters.birthYearTo   = null;
        log.info(`[IntentFix] birthYear: ${analysis.filters.birthYear}`);
      }
    }
  }

  // ── Graduation year ───────────────────────────────────────────────────────
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
      log.info(`[IntentFix] ugGraduationYear: ${analysis.filters.ugGraduationYear}`);
    }
  }

  // ── PDF / policy keywords misclassified as OOS ────────────────────────────
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

  const jobTitlePattern = question.match(
    /(?:^|\bin\b|\bfor\b|\bapplied\s+(?:in|for)\b)\s+([A-Za-z0-9 ]+?)\s+\bjob\b/i,
  );
  if (jobTitlePattern) {
    const extractedJobTitle = jobTitlePattern[1].trim();

    const skipWords = new Set(["a", "the", "this", "that", "any", "some"]);
    if (extractedJobTitle.length > 0 && !skipWords.has(extractedJobTitle.toLowerCase())) {
      const cityVal  = (analysis.entities.city  || "").toLowerCase();
      const stateVal = (analysis.entities.state || "").toLowerCase();
      const extracted = extractedJobTitle.toLowerCase();

      if (cityVal === extracted || stateVal === extracted) {
        log.info(`[IntentFix] "in/for X job" — moving "${extractedJobTitle}" from city/state → jobTitle`);
        if (cityVal === extracted)  analysis.entities.city  = null;
        if (stateVal === extracted) analysis.entities.state = null;
      }

     
      if (!analysis.entities.jobTitle ||
          analysis.entities.jobTitle.toLowerCase() === "job" ||
          analysis.entities.jobTitle.toLowerCase() === extracted) {
        analysis.entities.jobTitle = extractedJobTitle;
        log.info(`[IntentFix] jobTitle set to "${extractedJobTitle}" from "in/for X job" pattern`);
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
    const skipStarters = new Set(["candidate", "candidates", "show", "list", "find", "get", "all", "the", "a"]);
    if (extracted.length > 0 && !skipStarters.has(extracted.toLowerCase())) {
      const cityVal = (analysis.entities.city || "").toLowerCase();
      if (cityVal === extracted.toLowerCase()) {
        analysis.entities.city = null;
        log.info(`[IntentFix] "X job" prefix — clearing wrong city "${extracted}"`);
      }
      if (!analysis.entities.jobTitle || analysis.entities.jobTitle.toLowerCase() === "job") {
        analysis.entities.jobTitle = extracted;
        log.info(`[IntentFix] "X job" prefix — jobTitle set to "${extracted}"`);
      }
    }
  }
  // ── Interviewer ───────────────────────────────────────────────────────────
  const interviewerMatch = question.match(
    /(?:interview\w*\s+(?:taken\s+)?by|interviewed\s+by|interviewer\s+(?:is\s+)?)\s+(.+?)(?:\s*$|\s*\?)/i,
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

// ─────────────────────────────────────────────────────────────────────────────
// analyzeQuestion — THE SINGLE LLM CALL (with LRU cache + retry)
// ─────────────────────────────────────────────────────────────────────────────

async function analyzeQuestion(question, candidates) {
 
  const cacheKey = question.toLowerCase().trim();
  const cached = analysisCache.get(cacheKey);
  if (cached && Date.now() - cached.ts < ANALYSIS_CACHE_TTL) {
    log.info(`[AnalysisCache] HIT for: "${cacheKey.slice(0, 60)}"`);
    return cached.result;
  }

  const client = getOpenAIClient();

 
  const likelyNameQuery = /\b[A-Z][a-z]+\b/.test(question) || /\b(who is|about|tell me about|details of)\b/i.test(question);
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

  "experienceYears":         number or null,
"experienceYearsOperator": "eq"|"gt"|"lt"|"gte"|"lte" or null,
EXPERIENCE YEARS:
  "2 years of experience"         → experienceYears: 2, experienceYearsOperator: "eq"
  "more than 3 years experience"  → experienceYears: 3, experienceYearsOperator: "gt"
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

User question: "${question.replace(/"/g, '\\"')}"
JSON:`.trim();

  // ── LLM call with single retry on JSON parse failure ──────────────────────
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

      // ── Schema validation ─────────────────────────────────────────────────
      if (!validateAnalysis(parsed)) {
        throw new Error(`Invalid analysis schema: ${JSON.stringify(parsed).slice(0, 100)}`);
      }

      const rawLimit = parsed.limit;
      const safeLimit =
        rawLimit != null &&
        Number.isFinite(Number(rawLimit)) &&
        Number(rawLimit) > 0
          ? Math.floor(Number(rawLimit))
          : null;

      const rawEntities = parsed.entities || {};
      const rawFilters  = parsed.filters  || {};

      // ── Normalise misplaced fields ────────────────────────────────────────
      const normEntities = { ...rawEntities };
      const normFilters  = { ...rawFilters, interviewRound: rawFilters.interviewRound || null };

      if (normEntities.gender != null && normEntities.gender !== "") {
        normFilters.gender = normFilters.gender || normEntities.gender;
        delete normEntities.gender;
      }
      if (normEntities.language != null && normEntities.language !== "") {
        normFilters.languageKnown = normFilters.languageKnown || normEntities.language;
        delete normEntities.language;
      }
      if (normEntities.languageKnown != null && normEntities.languageKnown !== "") {
        normFilters.languageKnown = normFilters.languageKnown || normEntities.languageKnown;
        delete normEntities.languageKnown;
      }
      ["isStudying","isVerified","isProfileComplete","hasExperience","isCurrentlyWorking",
       "isShortlisted","isAccepted","hasInterview","hasResume"].forEach((key) => {
        if (normEntities[key] != null) {
          normFilters[key] = normFilters[key] ?? normEntities[key];
          delete normEntities[key];
        }
      });
      if (normEntities.interviewer != null && normEntities.interviewer !== "") {
        normFilters.interviewer = normFilters.interviewer || normEntities.interviewer;
        delete normEntities.interviewer;
      }

      const FILTER_KEYS = [
        "interviewer", "interviewStatus", "applicationStatus", "isQualified",
        "hasFeedback", "marksObtained", "marksOperator", "hasAadhar", "hasPanCard",
        "hasBankPassbook", "hasBankStatement", "hasSalarySlip", "hasExperienceLetter",
        "hasOfferLetter", "hasItr", "resumeCount", "resumeCountOperator",
        "hasLinkedin", "hasPortfolio", "ugDegree", "pgDegree", "hasPostGraduation",
        "ugGraduationYear", "pgGraduationYear", "hasApplied",
        "birthYear", "birthYearFrom", "birthYearTo","experienceYears", "experienceYearsOperator",
  "experienceYearsFrom", "experienceYearsTo",
      ];
      FILTER_KEYS.forEach((key) => {
        if (normEntities[key] != null) {
          normFilters[key] = normFilters[key] ?? normEntities[key];
          delete normEntities[key];
        }
      });

      log.info(`[Normalised] entities: ${JSON.stringify(normEntities)} | filters: ${JSON.stringify(normFilters)}`);

      const result = {
        intent: parsed.intent || "CANDIDATE",
        limit: safeLimit,
        entities: normEntities,
        filters: normFilters,
        sectionsNeeded: parsed.sectionsNeeded || "ALL",
      };

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

  log.error(`analyzeQuestion failed after retry — defaulting to CANDIDATE/ALL. Last error: ${lastError?.message}`);
  return {
    intent: "CANDIDATE",
    limit: null,
    entities: {},
    filters: {},
    sectionsNeeded: "ALL",
  };
}

// ── Schema validator ──────────────────────────────────────────────────────────

function validateAnalysis(parsed) {
  const validIntents = ["CANDIDATE", "COUNT", "PDF", "REPORT", "OOS"];
  if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) return false;
  if (!validIntents.includes(parsed.intent)) return false;
  if (parsed.entities !== null && parsed.entities !== undefined && typeof parsed.entities !== "object") return false;
  if (parsed.filters  !== null && parsed.filters  !== undefined && typeof parsed.filters  !== "object") return false;
  return true;
}

// ─────────────────────────────────────────────────────────────────────────────
// FILTER CANDIDATES
// ─────────────────────────────────────────────────────────────────────────────

function filterCandidates(entities, filters = {}) {
  if (!candidateStore) return [];

  let pools = [];

  // ─────────────────────────────
  // STEP 1: COLLECT INDEX SETS
  // ─────────────────────────────

  if (entities.skill) {
    const skillSet = candidateIndex.bySkill.get(entities.skill.toLowerCase()) || [];
    pools.push(skillSet);
  }

  if (entities.city) {
    const citySet = candidateIndex.byCity.get(entities.city.toLowerCase()) || [];
    pools.push(citySet);
  }

  if (entities.state) {
    const stateSet = candidateIndex.byState.get(entities.state.toLowerCase()) || [];
    pools.push(stateSet);
  }

  if (entities.jobTitle) {
    const jobSet = candidateIndex.byJobTitle.get(entities.jobTitle.toLowerCase()) || [];
    pools.push(jobSet);
  }

  // ─────────────────────────────
  // STEP 2: INTERSECTION (KEY FIX)
  // ─────────────────────────────

  let pool;

  if (pools.length === 0) {
    pool = candidateStore.candidates;
  } else {
    // take smallest set first (performance)
    pools.sort((a, b) => a.length - b.length);

    pool = pools[0];

    for (let i = 1; i < pools.length; i++) {
      const set = new Set(pools[i]);
      pool = pool.filter(c => set.has(c));
    }
  }

  // ─────────────────────────────
  // STEP 3: APPLY REMAINING FILTERS
  // ─────────────────────────────

  pool = pool.filter(candidate => {

    // company filter
    if (entities.companyName) {
      const match = (candidate.Experience || []).some(e =>
        (e.companyName || "").toLowerCase().includes(entities.companyName.toLowerCase())
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

    return matchesFilters(candidate, filters);
  });

  return pool;
}


function detectResponseType(question) {
  const q = question.toLowerCase();

  if (q.includes("education")) return "WITH_EDUCATION";
  if (q.includes("skill")) return "WITH_SKILLS";
  if (q.includes("resume")) return "WITH_RESUME";
  if (q.includes("experience")) return "WITH_EXPERIENCE";

  return "DEFAULT";
}

// ─────────────────────────────────────────────────────────────────────────────
// ENTITY MATCHING
// ─────────────────────────────────────────────────────────────────────────────

function matchesEntity(candidate, entities) {
  const checks = [];

  if (entities.candidateName != null && entities.candidateName !== "") {
    const search = entities.candidateName.toLowerCase().trim();
    const fullName = (candidate.FullName || "").toLowerCase();
    const words = search.split(/\s+/).filter((w) => w.length > 0);
    const directMatch = fullName.includes(search);
    const meaningfulWords = words.filter((w) => w.length > 1);
    const multiWordMatch = meaningfulWords.length >= 2
      && meaningfulWords.every((w) => fullName.includes(w));
    const singleWordMatch = meaningfulWords.length === 1
      && fullName.includes(meaningfulWords[0]);
    const nameMatch = meaningfulWords.length >= 2
      ? (directMatch || multiWordMatch)
      : (directMatch || singleWordMatch);
    checks.push(nameMatch);
  }

  if (entities.email != null && entities.email !== "") {
    checks.push(
      (candidate.email || "").toLowerCase() === entities.email.toLowerCase(),
    );
  }

  // ── City: exact match or "starts with word boundary" guard ───────────────
  if (entities.city != null && entities.city !== "") {
    const cityVal   = (candidate.Profile?.city || "").toLowerCase().trim();
    const cityQuery = entities.city.toLowerCase().trim();
   
    const cityMatch =
      cityVal === cityQuery ||
      cityVal.startsWith(cityQuery + " ") ||
      cityVal.includes(cityQuery); 
    checks.push(cityMatch);
  }

  // ── State: same guard as city ─────────────────────────────────────────────
  if (entities.state != null && entities.state !== "") {
    const stateVal   = (candidate.Profile?.state || "").toLowerCase().trim();
    const stateQuery = entities.state.toLowerCase().trim();
    const stateMatch =
      stateVal === stateQuery ||
      stateVal.startsWith(stateQuery + " ") ||
      stateVal.includes(stateQuery);
    checks.push(stateMatch);
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
    if (Boolean(candidate.isVerified) !== Boolean(filters.isVerified)) return false;
  }
  if (filters.isProfileComplete != null) {
    if (Boolean(candidate.isProfileComplete) !== Boolean(filters.isProfileComplete)) return false;
  }

  // ── PROFILE ───────────────────────────────────────────────────────────────
  if (filters.isStudying != null) {
    const hasGradYearFilter =
      filters.ugGraduationYear != null || filters.pgGraduationYear != null;
    if (!hasGradYearFilter) {
      const val = candidate.Profile?.isStudying;
      if (val != null && Boolean(val) !== Boolean(filters.isStudying)) return false;
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
  if (filters.birthYear != null || filters.birthYearFrom != null || filters.birthYearTo != null) {
    const dob = candidate.Profile?.dateOfBirth;
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
      "btech":     ["bachelor of technology", "b.tech", "b.e."],
      "be":        ["bachelor of engineering", "b.e."],
      "bca":       ["bachelor of computer", "bca"],
      "bsc":       ["bachelor of science", "b.sc"],
      "ba":        ["bachelor of arts", "b.a."],
      "bcom":      ["bachelor of commerce", "b.com"],
      "bba":       ["bachelor of business", "bba", "bbm"],
      "bbm":       ["bachelor of business", "bbm"],
      "barch":     ["bachelor of architecture", "b.arch"],
      "bachelors": ["bachelor"],
      "bachelor":  ["bachelor"],
      "degree":    ["bachelor", "master"],
      "graduation":["bachelor"],
      "undergrad": ["bachelor"],
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
    const PG_ALIASES = {
      "masters":        ["master"],
      "master":         ["master"],
      "master degree":  ["master"],
      "masters degree": ["master"],
      "pg":             ["master", "m."],
      "mtech":          ["master of technology", "m.tech", "m.e."],
      "mba":            ["master of business", "mba"],
      "mca":            ["master of computer", "mca"],
      "msc":            ["master of science", "m.sc"],
      "ma":             ["master of arts", "m.a."],
      "mcom":           ["master of commerce", "m.com"],
      "me":             ["master of engineering", "m.e."],
      "march":          ["master of architecture", "m.arch"],
    };
    const keywords = PG_ALIASES[pgQuery] || [pgQuery];
    const match = edu.some((e) => {
      const deg = (e.PostGraduationDegree || "").toLowerCase();
      return keywords.some((kw) => deg.includes(kw));
    });
    if (!match) return false;
  }
  if (filters.hasPostGraduation != null) {
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
    const parseYear = (val) => {
      if (val == null || val === "" || val === "null") return null;
      if (typeof val === "string" && val.includes("-")) {
        const y = parseInt(val.split("-")[0], 10);
        return isNaN(y) ? null : y;
      }
      const n = parseInt(String(val), 10);
      return isNaN(n) ? null : n;
    };
    const match = edu.some((e) => {
      const ugYear = parseYear(e.underGraduationEndYear);
      const pgYear = parseYear(e.postGraduationEndYear);
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
if (
  filters.experienceYears != null ||
  filters.experienceYearsFrom != null ||
  filters.experienceYearsTo != null
) {
  const totalYears = getTotalExperienceYears(candidate);

  if (filters.experienceYears != null) {
    const target = Number(filters.experienceYears);
    const op = filters.experienceYearsOperator || "eq";
    const TOLERANCE = 0.5; 
    const passes =
      op === "eq"  ? Math.abs(totalYears - target) <= TOLERANCE :
      op === "gt"  ? totalYears > target :
      op === "lt"  ? totalYears < target :
      op === "gte" ? totalYears >= target - TOLERANCE :
      op === "lte" ? totalYears <= target + TOLERANCE :
                     Math.abs(totalYears - target) <= TOLERANCE;
    if (!passes) return false;
  }

  if (filters.experienceYearsFrom != null && totalYears < Number(filters.experienceYearsFrom)) return false;
  if (filters.experienceYearsTo   != null && totalYears > Number(filters.experienceYearsTo))   return false;
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
  
  const safeInterviews = (candidate.Applications || []).flatMap(
    (a) => (Array.isArray(a.Interviews) ? a.Interviews : []),
  );

  if (filters.isQualified != null) {
    const match = safeInterviews.some(
      (i) => Boolean(i.isQualified) === Boolean(filters.isQualified),
    );
    if (!match) return false;
  }
  if (filters.hasInterview != null) {
    const hasIt = safeInterviews.length > 0;
    if (hasIt !== Boolean(filters.hasInterview)) return false;
  }
  if (filters.hasFeedback != null) {
    const hasIt = safeInterviews.some(
      (i) => i.feedback && i.feedback.trim() !== "",
    );
    if (hasIt !== Boolean(filters.hasFeedback)) return false;
  }
  if (filters.interviewer) {
    const target = filters.interviewer.toLowerCase().trim();
    const match = safeInterviews.some((i) =>
      (i.Interviewer || "").toLowerCase().includes(target),
    );
    if (!match) return false;
  }
  if (filters.interviewStatus) {
    const target = filters.interviewStatus.toLowerCase();
    const match = safeInterviews.some((i) =>
      (i.InterviewStatus || "").toLowerCase().includes(target),
    );
    if (!match) return false;
  }
  if (filters.marksObtained != null) {
    const target = Number(filters.marksObtained);
    const op = filters.marksOperator || "eq";
   const allMarks = safeInterviews
      .map((i) => parseFloat(i.marksObtained))
      .filter((m) => !isNaN(m));
    if (allMarks.length === 0) return false;
    const passes = allMarks.some((m) => {
      if (op === "eq")  return Math.abs(m - target) < 0.01;
      if (op === "gt")  return m > target;
      if (op === "lt")  return m < target;
      if (op === "gte") return m >= target;
      if (op === "lte") return m <= target;
      return Math.abs(m - target) < 0.01;
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
    if (!!doc.expierenceLetter !== Boolean(filters.hasExperienceLetter)) return false;
  }
  if (filters.hasOfferLetter != null) {
    if (!!doc.offerLetter !== Boolean(filters.hasOfferLetter)) return false;
  }
  if (filters.hasItr != null) {
    if (!!doc.itr !== Boolean(filters.hasItr)) return false;
  }


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
      op === "eq"  ? count === target :
      op === "gt"  ? count > target   :
      op === "lt"  ? count < target   :
      op === "gte" ? count >= target  :
      op === "lte" ? count <= target  :
                     count === target;
    if (!passes) return false;
  }

  return true;
}

// ─────────────────────────────────────────────────────────────────────────────
// getSummarySections
// ─────────────────────────────────────────────────────────────────────────────

function getSummarySections(filters, llmSectionsNeeded) {
  
  const llmIsRootOnly =
    Array.isArray(llmSectionsNeeded) &&
    llmSectionsNeeded.length === 1 &&
    llmSectionsNeeded[0] === "root";

  if (llmIsRootOnly) {
    
    const needsEducation =
      filters.ugDegree || filters.pgDegree ||
      filters.hasPostGraduation != null ||
      filters.ugGraduationYear != null || filters.pgGraduationYear != null;
    const needsExperience =
      filters.hasExperience != null || filters.isCurrentlyWorking != null;
    const needsApplications =
      filters.isShortlisted != null || filters.isAccepted != null ||
      filters.hasApplied != null || filters.applicationStatus ||
      filters.hasInterview != null || filters.interviewStatus ||
      filters.interviewer || filters.isQualified != null ||
      filters.hasFeedback != null || filters.marksObtained != null;

    const sections = new Set(["root"]);
    if (needsEducation)    sections.add("Education");
    if (needsExperience)   sections.add("Experience");
    if (needsApplications) { sections.add("Applications"); sections.add("Applications.Interviews"); }
    return [...sections];
  }


  const sections = new Set(["root", "Profile", "Skills"]);

  if (filters.birthYear != null || filters.birthYearFrom != null || filters.birthYearTo != null) {
    sections.add("Profile");
  }
  if (
    filters.ugDegree || filters.pgDegree ||
    filters.hasPostGraduation != null ||
    filters.ugGraduationYear != null || filters.pgGraduationYear != null
  ) {
    sections.add("Education");
  }
  if (filters.hasExperience != null || filters.isCurrentlyWorking != null) {
    sections.add("Experience");
  }
  if (
    filters.isShortlisted != null || filters.isAccepted != null ||
    filters.hasApplied != null || filters.applicationStatus ||
    filters.hasInterview != null || filters.interviewStatus ||
    filters.interviewer || filters.isQualified != null ||
    filters.hasFeedback != null || filters.marksObtained != null
  ) {
    sections.add("Applications");
    sections.add("Applications.Interviews");
  }

 
  const allowed = new Set([
    "root", "Profile", "Skills", "Education", "Experience",
    "Applications", "Applications.Interviews",
  ]);
  if (Array.isArray(llmSectionsNeeded)) {
    llmSectionsNeeded.forEach((s) => { if (allowed.has(s)) sections.add(s); });
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


function getTotalExperienceYears(candidate) {
  const experiences = (candidate.Experience || []).filter(
    (e) => e.companyName && e.companyName.trim() !== "" && e.startDate
  );
  if (experiences.length === 0) return 0;

  let totalMs = 0;
  const now = new Date();

  for (const exp of experiences) {
    const start = new Date(exp.startDate);
    if (isNaN(start.getTime())) continue;

    const end = exp.isCurrentCompany || !exp.endDate
      ? now
      : new Date(exp.endDate);

    if (isNaN(end.getTime())) continue;

    const diff = end.getTime() - start.getTime();
    if (diff > 0) totalMs += diff;
  }

  return totalMs / (1000 * 60 * 60 * 24 * 365.25); // returns decimal years
}

// ─────────────────────────────────────────────────────────────────────────────
// HELPERS
// ─────────────────────────────────────────────────────────────────────────────

function buildFilterLabel(entities, filters) {
 
  const parts = [];

  if (entities?.candidateName) return `named "${entities.candidateName}"`;
  if (entities?.jobTitle)      parts.push(`applied for "${entities.jobTitle}"`);
  if (entities?.skill)         parts.push(`with skill "${entities.skill}"`);
  if (entities?.city)          parts.push(`from "${entities.city}"`);
  if (entities?.state)         parts.push(`from "${entities.state}"`);
  if (entities?.companyName)   parts.push(`who worked at "${entities.companyName}"`);
  if (entities?.universityName) parts.push(`from "${entities.universityName}" university/college`);

  if (filters.isStudying === true)           parts.push("currently studying");
  if (filters.isStudying === false)          parts.push("not currently studying");
  if (filters.isShortlisted === true)        parts.push("shortlisted");
  if (filters.isVerified === true)           parts.push("verified");
  if (filters.isVerified === false)          parts.push("unverified");
  if (filters.isProfileComplete === true)    parts.push("with complete profile");
  if (filters.isProfileComplete === false)   parts.push("with incomplete profile");
  if (filters.hasExperience === false)       parts.push("with no work experience (freshers)");
  if (filters.hasExperience === true)        parts.push("with work experience");
  if (filters.isCurrentlyWorking === true)   parts.push("currently working");
  if (filters.gender)                        parts.push(`who are ${filters.gender}`);
  if (filters.languageKnown)                 parts.push(`who speak ${filters.languageKnown}`);
  if (filters.ugDegree)                      parts.push(`with ${filters.ugDegree} degree`);
  if (filters.pgDegree)                      parts.push(`with ${filters.pgDegree} degree`);
  if (filters.hasPostGraduation === true)    parts.push("with post-graduation degree");
  if (filters.hasPostGraduation === false)   parts.push("without post-graduation degree");
  if (filters.applicationStatus)             parts.push(`with application status "${filters.applicationStatus}"`);
  if (filters.interviewStatus)               parts.push(`with interview status "${filters.interviewStatus}"`);
  if (filters.isQualified === true)          parts.push("who qualified the interview");
  if (filters.isQualified === false)         parts.push("who did not qualify the interview");
  if (filters.marksObtained != null) {
    const opLabel = {
      eq: "exactly", gt: "more than", lt: "less than", gte: "at least", lte: "at most",
    }[filters.marksOperator || "eq"];
    parts.push(`who scored ${opLabel} ${filters.marksObtained} marks`);
  }
  if (filters.hasResume === true)   parts.push("with uploaded resume");
  if (filters.hasResume === false)  parts.push("with no resume uploaded");
  if (filters.hasAadhar === false)  parts.push("with missing Aadhar card");
  if (filters.hasPanCard === false) parts.push("with missing PAN card");
  if (filters.experienceYears != null) {
  const opLabel = {
    eq: "exactly", gt: "more than", lt: "less than", gte: "at least", lte: "at most",
  }[filters.experienceYearsOperator || "eq"];
  parts.push(`with ${opLabel} ${filters.experienceYears} year(s) of experience`);
}
if (filters.experienceYearsFrom != null || filters.experienceYearsTo != null) {
  const from = filters.experienceYearsFrom ?? "?";
  const to   = filters.experienceYearsTo   ?? "?";
  parts.push(`with ${from}–${to} years of experience`);
}

  return parts.length ? parts.join(", ") : "matching your query";
}