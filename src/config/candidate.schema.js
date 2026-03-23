// ============================================================
// candidate.cache.js
// Location: src/services/candidate.cache.js
//
// Structured Data RAG for sp_GetAllCandidateDetails_Full
//
// ─────────────────────────────────────────────────────────────
// DESIGN: ONE smart LLM call (analyzeQuestion) handles all:
//   • intent   — CANDIDATE / COUNT / PDF / REPORT / OOS
//   • entities — name, city, skill, jobTitle, companyName...
//   • filters  — every filterable column from the SP
//   • sections — which SP sections to project
//
// All filtering runs in JS on the in-memory cache.
// The answer LLM only receives pre-filtered, pre-projected data.
//
// BUG FIXES APPLIED:
//   [FIX-1] MAX_CANDIDATES_TO_LLM no longer slices before COUNT —
//           filterCandidates() returns the full matched pool;
//           slicing to MAX_CANDIDATES_TO_LLM happens only when
//           projecting data to send to the answer LLM.
//
//   [FIX-2] hasExperience / isCurrentlyWorking now ignore
//           empty-company placeholder rows
//           (companyName === "" counts as no experience).
//
//   [FIX-3] matchesEntity() now uses AND logic across all
//           supplied entities instead of OR, so
//           "Python developers from Ahmedabad" correctly
//           requires BOTH skill=Python AND city=Ahmedabad.
//
//   [FIX-4] Name list sent to analyzeQuestion is capped at
//           3 000 characters (not raw .slice(0,300)) to
//           prevent prompt-size blowout at scale.
// ─────────────────────────────────────────────────────────────

import { getPool } from "../db/connection.js";
import { getOpenAIClient } from "./ai.service.js";
import { generateNaturalAnswer } from "./ai.service.js";
import { log } from "../utils/logger.js";

const SP_NAME = "dbo.sp_GetAllCandidateDetails_Full";
const CACHE_TTL_MS = 30 * 60 * 1000;
const MAX_CANDIDATES_TO_LLM = 50; // cap only for answer-LLM payload, NOT for filtering/counting
const NAME_LIST_CHAR_CAP = 3000; // [FIX-4] hard character limit for name list in prompt

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

export async function answerFromCache(question) {
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
    return {
      success: true,
      type: "CACHE",
      dataframe: [],
      cachedAt: candidateStore.cachedAt,
      answer:
        `The interview round type (e.g. "Coding Round", "HR Round") is not stored in the database — only interview **status** is tracked.\n\n` +
        `You can filter by status instead:\n` +
        `- **Completed** — *"candidates whose interview is completed"*\n` +
        `- **Scheduled** — *"candidates with scheduled interviews"*\n` +
        `- **In Progress** — *"candidates with interview in progress"*\n` +
        `- **Postponed** — *"candidates with postponed interviews"*\n` +
        `- **Cancelled** — *"candidates with cancelled interviews"*`,
    };
  }

  // ── COUNT intent ────────────────────────────────────────────────────────
  // [FIX-1] filterCandidates() now returns the FULL matched pool (no slice).
  //         COUNT therefore reflects the real number, not a capped subset.
  if (analysis.intent === "COUNT") {
    const hasEntity = Object.values(analysis.entities || {}).some(
      (v) => v != null && v !== "",
    );
    const hasFilter = Object.values(analysis.filters || {}).some(
      (v) => v != null,
    );

    if (!hasEntity && !hasFilter) {
      const total = candidateStore.candidates.length;
      return {
        success: true,
        type: "CACHE_COUNT",
        dataframe: [],
        cachedAt: candidateStore.cachedAt,
        answer: `There are currently **${total}** candidate${total !== 1 ? "s" : ""} registered in the system (as of ${candidateStore.cachedAt.toLocaleString()}).`,
      };
    }

    // Full unsliced pool — accurate count
    const countFiltered = filterCandidates(
      analysis.entities,
      analysis.filters || {},
    );
    log.info(`[Count] Filtered candidates: ${countFiltered.length}`);

    const label = buildFilterLabel(analysis.entities, analysis.filters || {});
    if (countFiltered.length === 0) {
      return {
        success: true,
        type: "CACHE_COUNT",
        dataframe: [],
        cachedAt: candidateStore.cachedAt,
        answer: `No candidates found ${label}.`,
      };
    }
    return {
      success: true,
      type: "CACHE_COUNT",
      dataframe: [],
      cachedAt: candidateStore.cachedAt,
      answer: `There are **${countFiltered.length}** candidate${countFiltered.length !== 1 ? "s" : ""} ${label}.`,
    };
  }

  // ── CANDIDATE intent ─────────────────────────────────────────────────────

  // Step 2: filter — returns full matched pool (no slice)  [FIX-1]
  const filtered = filterCandidates(analysis.entities, analysis.filters || {});
  log.info(`[Step 2] Candidates after filter: ${filtered.length}`);

  if (filtered.length === 0) {
    const label = buildFilterLabel(analysis.entities, analysis.filters || {});
    const msg = analysis.entities?.candidateName
      ? `I couldn't find any candidate named "${analysis.entities.candidateName}". Please check the name and try again.`
      : `No candidates found ${label}.`;
    return {
      success: true,
      answer: msg,
      dataframe: [],
      cachedAt: candidateStore.cachedAt,
    };
  }

  // Step 3: project — slice HERE (not inside filterCandidates)  [FIX-1]
  const sections =
    analysis.sectionsNeeded === "ALL"
      ? ALL_SECTIONS
      : analysis.sectionsNeeded || ALL_SECTIONS;

  const slicedForLLM = filtered.slice(0, MAX_CANDIDATES_TO_LLM);
  const projected = projectData(slicedForLLM, sections);
  log.info(
    `[Step 3] Sections: ${JSON.stringify(sections)} — sending ${projected.length} of ${filtered.length} candidates to LLM`,
  );

  // Step 4: answer
  const answer = await generateNaturalAnswer(
    question,
    `${SP_NAME} (cached at ${candidateStore.cachedAt.toLocaleString()})`,
    projected,
  );

  return {
    success: true,
    type: "CACHE",
    answer,
    dataframe: projected,
    cachedAt: candidateStore.cachedAt,
    totalFound: filtered.length, // real total, not capped
    shownToLLM: projected.length, // how many were actually sent to LLM
    sectionsUsed: sections,
    entities: analysis.entities,
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
// analyzeQuestion — THE SINGLE LLM CALL
// ─────────────────────────────────────────────────────────────────────────────

async function analyzeQuestion(question, candidates) {
  const client = getOpenAIClient();

  // [FIX-4] Cap name list by CHARACTER LENGTH, not by array index.
  //         This prevents the prompt from blowing up at scale
  //         (e.g. 500+ candidates × average 15 chars/name = 7 500+ chars).
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

  "filters": {
    "isVerified":          boolean or null,
    "isProfileComplete":   boolean or null,
    "isStudying":          boolean or null,
    "gender":              string or null,
    "languageKnown":       string or null,
    "hasLinkedin":         boolean or null,
    "hasPortfolio":        boolean or null,
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
    "hasResume":           boolean or null
  },

  "sectionsNeeded": "ALL" or string[]
}

────────────────────────────────────────
INTENT RULES:
- CANDIDATE : any question about candidate data — lists, filters, profiles, specific fields
             KEY: if the answer needs candidate names or records → CANDIDATE, never COUNT
- COUNT     : ONLY when user wants a NUMBER. "how many X" with a filter → still COUNT but run the filter.
             NOT count: "list all candidates", "show candidates who X", "all candidates with Y"
- PDF       : company policy, HR documents, process guides — not candidate data
- REPORT    : user wants to generate/download a report file
- OOS       : completely unrelated (weather, cooking, general knowledge)

────────────────────────────────────────
ENTITY RULES:
- candidateName: match ANY name resembling one in the list (partial/lowercase/typo OK)
- Extract city, state, skill, jobTitle, companyName when clearly mentioned
- universityName: extract when user mentions a college, university, or institute name
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

EDUCATION:
  "B.Tech candidates"             → ugDegree: "B.Tech"
  "MBA candidates"                → pgDegree: "MBA"
  "has masters/PG"                → hasPostGraduation: true
  "no PG degree"                  → hasPostGraduation: false
  "graduated in 2023"             → ugGraduationYear: 2023

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
  "passed interview" / "qualified"→ isQualified: true
  "failed interview"              → isQualified: false
  "has interview"                 → hasInterview: true
  "has feedback"                  → hasFeedback: true
  "completed interview"           → interviewStatus: "Completed"
  "scheduled interview"           → interviewStatus: "Scheduled"
  "in progress interview"         → interviewStatus: "In Progress"
  "postponed"                     → interviewStatus: "Postponed"
  "cancelled interview"           → interviewStatus: "Cancelled"
  "coding round" / "HR round"     → interviewRound: "<round name>" (not in SP)
  marks comparisons               → marksObtained + marksOperator

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
  "has resume" / "uploaded resume"→ hasResume: true
  "no resume" / "missing resume"  → hasResume: false

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
  interview/marks/feedback/qualified             → ["root", "Applications", "Applications.Interviews"]
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
    return {
      intent: parsed.intent || "CANDIDATE",
      entities: parsed.entities || {},
      filters: {
        ...(parsed.filters || {}),
        interviewRound: parsed.filters?.interviewRound || null,
      },
      sectionsNeeded: parsed.sectionsNeeded || "ALL",
    };
  } catch (err) {
    log.error(
      `analyzeQuestion failed: ${err.message} — defaulting to CANDIDATE/ALL`,
    );
    return {
      intent: "CANDIDATE",
      entities: {},
      filters: {},
      sectionsNeeded: "ALL",
    };
  }
}

// ─────────────────────────────────────────────────────────────────────────────
// FILTER CANDIDATES
// ─────────────────────────────────────────────────────────────────────────────
// [FIX-1] Returns the FULL matched pool — NO .slice() here.
//         Slicing to MAX_CANDIDATES_TO_LLM is done in answerFromCache
//         only for the LLM projection step, so COUNT is always accurate.

function filterCandidates(entities, filters = {}) {
  if (!candidateStore) return [];
  const candidates = candidateStore.candidates;
  const hasEntity = Object.values(entities).some((v) => v != null && v !== "");
  const hasFilter = Object.values(filters).some((v) => v != null);

  let pool = hasEntity
    ? candidates.filter((c) => matchesEntity(c, entities))
    : candidates;
  if (hasFilter) pool = pool.filter((c) => matchesFilters(c, filters));
  return pool; // full pool — no slice
}

// ─────────────────────────────────────────────────────────────────────────────
// ENTITY MATCHING  [FIX-3]
// ─────────────────────────────────────────────────────────────────────────────
// Previous version used OR: any entity match → include candidate.
// This caused "Python developers from Ahmedabad" to return all Python
// developers AND all Ahmedabad residents instead of the intersection.
//
// New version builds a checks[] array — one boolean per supplied entity —
// and requires ALL checks to pass (AND logic).

function matchesEntity(candidate, entities) {
  const checks = [];

  // ── candidateName ────────────────────────────────────────────────────────
  if (entities.candidateName != null && entities.candidateName !== "") {
    const search = entities.candidateName.toLowerCase().trim();
    const fullName = (candidate.FullName || "").toLowerCase();
    const directMatch = fullName.includes(search);
    const wordMatch = search
      .split(/\s+/)
      .filter((p) => p.length > 2)
      .some((p) => fullName.includes(p));
    checks.push(directMatch || wordMatch);
  }

  // ── email ────────────────────────────────────────────────────────────────
  if (entities.email != null && entities.email !== "") {
    checks.push(
      (candidate.email || "").toLowerCase() === entities.email.toLowerCase(),
    );
  }

  // ── city ─────────────────────────────────────────────────────────────────
  if (entities.city != null && entities.city !== "") {
    checks.push(
      (candidate.Profile?.city || "")
        .toLowerCase()
        .includes(entities.city.toLowerCase()),
    );
  }

  // ── state ────────────────────────────────────────────────────────────────
  if (entities.state != null && entities.state !== "") {
    checks.push(
      (candidate.Profile?.state || "")
        .toLowerCase()
        .includes(entities.state.toLowerCase()),
    );
  }

  // ── skill ────────────────────────────────────────────────────────────────
  if (entities.skill != null && entities.skill !== "") {
    const skills = (candidate.Skills || []).map((s) =>
      (s.Skill || "").toLowerCase(),
    );
    checks.push(skills.some((sk) => sk.includes(entities.skill.toLowerCase())));
  }

  // ── jobTitle ─────────────────────────────────────────────────────────────
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

  // ── companyName ──────────────────────────────────────────────────────────
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

  // ── universityName ───────────────────────────────────────────────────────
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

  // All supplied entities must match (AND logic)
  return checks.length === 0 ? false : checks.every(Boolean);
}

// ─────────────────────────────────────────────────────────────────────────────
// VALUE FILTERS  [FIX-2]
// ─────────────────────────────────────────────────────────────────────────────
// hasExperience and isCurrentlyWorking previously counted placeholder rows
// where companyName === "" (the SP inserts these for candidates who tapped
// "Add Experience" but left the form blank).  Now both checks require that
// at least one Experience entry has a non-empty companyName.

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
    if (Boolean(candidate.Profile?.isStudying) !== Boolean(filters.isStudying))
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

  // ── EDUCATION ─────────────────────────────────────────────────────────────
  if (filters.ugDegree) {
    const edu = candidate.Education || [];
    const match = edu.some((e) =>
      (e.UnderGraduationDegree || "")
        .toLowerCase()
        .includes(filters.ugDegree.toLowerCase()),
    );
    if (!match) return false;
  }
  if (filters.pgDegree) {
    const edu = candidate.Education || [];
    const match = edu.some((e) =>
      (e.PostGraduationDegree || "")
        .toLowerCase()
        .includes(filters.pgDegree.toLowerCase()),
    );
    if (!match) return false;
  }
  if (filters.hasPostGraduation != null) {
    const edu = candidate.Education || [];
    const hasPg = edu.some(
      (e) => e.PostGraduationDegree && e.PostGraduationDegree.trim() !== "",
    );
    if (hasPg !== Boolean(filters.hasPostGraduation)) return false;
  }
  if (filters.ugGraduationYear != null) {
    const edu = candidate.Education || [];
    const match = edu.some(
      (e) =>
        Number(e.underGraduationEndYear) === Number(filters.ugGraduationYear),
    );
    if (!match) return false;
  }
  if (filters.pgGraduationYear != null) {
    const edu = candidate.Education || [];
    const match = edu.some(
      (e) =>
        Number(e.postGraduationEndYear) === Number(filters.pgGraduationYear),
    );
    if (!match) return false;
  }

  // ── EXPERIENCE  [FIX-2] ───────────────────────────────────────────────────
  // Ignore placeholder rows where companyName is blank ("") — the SP creates
  // these when a candidate opens the experience form but never fills it in.
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
      if (op === "eq") return m === target;
      if (op === "gt") return m > target;
      if (op === "lt") return m < target;
      if (op === "gte") return m >= target;
      if (op === "lte") return m <= target;
      return m === target;
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

  return true;
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
