// src/services/ai.service.js
// ============================================================
// Improvements applied:
//  1. Float-safe marks parsing throughout — parseFloat() replaces
//     Number() casts so "30.0", "30", and 30 all parse correctly
//  2. Null-guard on Interviews — Array.isArray() check before flatMap
//     prevents crashes when Interviews field is null from the SP
//  3. AI intro call is intentionally KEPT (generates contextual intro sentence)
// ============================================================

import OpenAI from "openai";
import { OPENAI_API_KEY } from "../config/constants.js";
import { log } from "../utils/logger.js";

let openaiClient = null;

export function getOpenAIClient() {
  if (!openaiClient) {
    openaiClient = new OpenAI({ apiKey: OPENAI_API_KEY });
  }
  return openaiClient;
}

export async function classifyIntent(question,contextMemory={}) {

  // 🔥 CONTEXT-AWARE SHORT QUERY FIX
const shortQuery = question.toLowerCase().trim();

const vagueDataQueries = [
  "interview",
  "interview date",
  "interview status",
  "education",
  "skills",
  "experience",
  "job",
  "application"
];

const isVague = vagueDataQueries.some(q => shortQuery.includes(q));

if (isVague) {
  // if we have previous context → it's SQL
  if (question.trim().split(" ").length <= 3) {
  if (contextMemory?.candidateName) {
    log.info("[IntentFix] Short follow-up → SQL");
    return "SQL";
  }
}
  if (contextMemory?.candidateName) {
    log.info("[IntentFix] Context-based SQL (vague query)");
    return "SQL";
  }

  // even without context, still likely SQL
  log.info("[IntentFix] Vague data query → SQL");
  return "SQL";
}
  const client = getOpenAIClient();

  const INTENT_SYSTEM = `
You are a strict intent classifier for a Recruitment Management chatbot.

Classify the user question into exactly one of these four labels:

SQL — the question asks for DATA from the database.
  This includes ANY question that asks to show, list, find, get, count, describe,
  explain, or summarize specific records — candidates, jobs, users, applications,
  interviews, skills, resumes, salaries, or any other stored data.
  SQL questions often name a specific person, job, location, or value.
  Examples:
    "show me all candidates"
    "list jobs with salary above 50000"
    "describe this candidate" (asking about a specific record)
    "explain why rahul was rejected" (asking about stored data)
    "what are the top jobs?" (asking for data from DB)
    "how many candidates applied?" (count from DB)
    "show candidates from Ahmedabad"
    "describe the candidate profile"

PDF — the question asks about POLICIES, PROCESSES, or GUIDELINES — not specific data records.
  PDF questions ask HOW something works, WHAT a process is, or WHY a rule exists.
  They do NOT ask for specific people, numbers, or records from the database.
  Examples:
    "what is the pre-boarding process?"
    "explain the interview policy"
    "how does the hiring process work?"
    "what are the selection criteria?"
    "what is online communication training?"
    "describe the recruitment workflow" (asking about process, not a specific record)

REPORT — user explicitly asks to generate or download a report/PDF document.
  Examples: "generate a report", "export as PDF", "create a report of all interviews"

OUT_OF_SCOPE — completely unrelated to recruitment, HR, candidates, or jobs.
  Examples: "what is 2+2", "write a poem", "what is Python", "tell me a joke"

DECISION RULES (apply in order):
1. If question mentions a SPECIFIC candidate, job, user, location, salary, or skill → SQL
2. If question asks HOW a process works or WHAT a policy is (no specific record) → PDF
3. If question asks to SHOW, LIST, GET, FIND, COUNT any records → SQL
4. If question is about general recruitment concepts without asking for data → PDF
5. If completely unrelated → OUT_OF_SCOPE

Respond with ONLY the label: SQL, PDF, REPORT, or OUT_OF_SCOPE
`.trim();

  const resp = await client.chat.completions.create({
    model: "gpt-4o-mini",
    messages: [
      { role: "system", content: INTENT_SYSTEM },
      {
  role: "user",
  content: `
Conversation context:
Candidate: ${contextMemory?.candidateName || "none"}

User question:
${question}
  `.trim()
},
    ],
    max_tokens: 10,
    temperature: 0,
  });

  const intent = (resp.choices[0].message.content || "").trim().toUpperCase();
  const valid = ["SQL", "PDF", "REPORT", "OUT_OF_SCOPE"];
 let result = valid.includes(intent) ? intent : "OUT_OF_SCOPE";

if (result === "OUT_OF_SCOPE") {
  if (
    /interview|education|skill|experience|job|candidate/i.test(question)
  ) {
    log.info("[IntentFix] OOS overridden → SQL");
    result = "SQL";
  }
}
  log.info(`Intent: ${result}`);
  return result;
}

// ═════════════════════════════════════════════════════════════════════════════
// RESPONSE TYPE DETECTION
// ═════════════════════════════════════════════════════════════════════════════

function detectResponseType(question, isSingleCandidate, totalFound, sectionsNeeded) {
  const q = question.toLowerCase();
// ── YES / NO QUESTIONS ───────────────────────────────────────────────
const yesNoPatterns = [
  "is ", "are ", "was ", "were ", "does ", "do ", "did ",
  "can ", "has ", "have "
];
if (q.includes("how many") || q.includes("count")) {
  return "COUNT";
}

if (yesNoPatterns.some(p => q.startsWith(p))) {
  return "YES_NO";
}
  // ── COMPARE ───────────────────────────────────────────────────────────────
  const compareWords = ["compare", "vs", "versus", "difference between", "contrast", "side by side"];
  if (compareWords.some((w) => q.includes(w))) return "COMPARE";


  const isRootOnly =
    Array.isArray(sectionsNeeded) &&
    sectionsNeeded.length === 1 &&
    sectionsNeeded[0] === "root";


  const nameListPhrases = [
    "full name", "name of ", "names of", "give me name",
    "list of all candidate", "list all candidate", "all candidate name",
    "show all candidate", "all candidates list", "candidates list",
    "candidate name", "candidate names",
  ];
  const isNameListQuestion = nameListPhrases.some((p) => q.includes(p));

  if (isRootOnly || isNameListQuestion) return "NAME_LIST";

  // ── SKILLS_ONLY — user asked for a single specific field ──────────────────
  const fieldKeywords = [
    "skill", "skills", "education", "degree", "qualification",
    "experience", "work", "company", "interview", "marks", "score",
    "document", "resume", "phone", "email", "location", "city",
  ];
  const isFieldQuery = fieldKeywords.some((w) => q.includes(w));
  const isSpecificSections =
    Array.isArray(sectionsNeeded) &&
    sectionsNeeded.length <= 3 &&
    !sectionsNeeded.includes("ALL");

  if (isFieldQuery && isSpecificSections) return "SKILLS_ONLY";

  // ── FULL_DETAILS / SUMMARY / LIST ─────────────────────────────────────────
  if (isSingleCandidate && totalFound <= 3) return "FULL_DETAILS";
  if (totalFound > 10 && !isSingleCandidate) return "SUMMARY";
  return "LIST";
}

// ═════════════════════════════════════════════════════════════════════════════
// COLUMN & SECTION MAPS
// ═════════════════════════════════════════════════════════════════════════════

const DETAIL_COLUMNS = [
  "Name", "Email", "Phone", "Verified", "Profile Complete", "Registered On",
  "Gender", "City", "State", "Languages", "Currently Studying",
  "LinkedIn", "Portfolio/GitHub", "Date of Birth",
  "Skills", "Education", "Experience",
  "Jobs Applied", "Application Status", "Shortlisted", "Accepted",
  "Interview Details", "Best Marks", "Documents Uploaded", "Resumes Uploaded",
];

const SECTION_TO_COLUMNS = {
  "root":                    ["Name", "Email", "Phone", "Verified", "Profile Complete", "Registered On"],
  "Profile":                 ["Gender", "City", "State", "Languages", "Currently Studying", "LinkedIn", "Portfolio/GitHub", "Date of Birth"],
  "Skills":                  ["Skills"],
  "Education":               ["Education"],
  "Experience":              ["Experience"],
  "Applications":            ["Jobs Applied", "Application Status", "Shortlisted", "Accepted"],
  "Applications.Interviews": ["Interview Details", "Interview Status", "Best Marks"],
  "Documents":               ["Documents Uploaded"],
  "Resumes":                 ["Resumes Uploaded"],
};

function getColumnsForSections(sectionsNeeded) {
  if (!Array.isArray(sectionsNeeded) || sectionsNeeded.length === 0) return DETAIL_COLUMNS;
  const cols = new Set(["Name"]);
  for (const sec of sectionsNeeded) {
    (SECTION_TO_COLUMNS[sec] || []).forEach((c) => cols.add(c));
  }
  return [...cols];
}

// ═════════════════════════════════════════════════════════════════════════════
// FLATTEN CANDIDATE
// ═════════════════════════════════════════════════════════════════════════════

function flattenCandidate(candidate, mode = "summary", sectionsNeeded = null) {
  const flat = {};
  const isDetail = mode === "detail";

  const wantedSections = Array.isArray(sectionsNeeded) ? new Set(sectionsNeeded) : null;
  const wants = (sec) => !wantedSections || wantedSections.has(sec);

  // ── Root ──────────────────────────────────────────────────────────────────
  flat["Name"] = candidate.FullName || "—";
  if (wants("root")) {
    flat["Email"] = candidate.email || "—";
    flat["Phone"] = candidate.phoneNumber || "—";
    if (isDetail) {
      flat["Verified"] = candidate.isVerified ? "Yes" : "No";
      flat["Profile Complete"] = candidate.isProfileComplete ? "Yes" : "No";
      if (candidate.RegisteredOn)
        flat["Registered On"] = new Date(candidate.RegisteredOn).toLocaleDateString(
          "en-IN", { day: "2-digit", month: "short", year: "numeric" },
        );
    }
  }

  // ── Profile ───────────────────────────────────────────────────────────────
  const p = candidate.Profile;
  if (p) {
    if (wants("Profile")) {
      if (p.city) flat["City"] = p.city;
      if (p.state) flat["State"] = p.state;
      if (isDetail) {
        if (p.Gender) flat["Gender"] = p.Gender;
        if (p.languageKnown) flat["Languages"] = p.languageKnown;
        if (p.isStudying != null)
          flat["Currently Studying"] = p.isStudying ? "Yes" : "No";
        if (p.linkedinProfileUrl) flat["LinkedIn"] = p.linkedinProfileUrl;
        if (p.portfolioGithubWebsiteUrl)
          flat["Portfolio/GitHub"] = p.portfolioGithubWebsiteUrl;
        if (p.dateOfBirth)
          flat["Date of Birth"] = new Date(p.dateOfBirth).toLocaleDateString(
            "en-IN", { day: "2-digit", month: "short", year: "numeric" },
          );
      }
    } else if (!wantedSections) {
      if (p.city) flat["City"] = p.city;
      if (p.state) flat["State"] = p.state;
    }
  }

  // ── Skills ────────────────────────────────────────────────────────────────
  if (wants("Skills") && Array.isArray(candidate.Skills) && candidate.Skills.length > 0) {
    const all = candidate.Skills.map((s) => s.Skill).filter(Boolean);
    flat["Skills"] = isDetail
      ? all.join(", ")
      : all.slice(0, 5).join(", ") + (all.length > 5 ? ` +${all.length - 5} more` : "");
    flat["_allSkills"] = all;
  }

  // ── Education ─────────────────────────────────────────────────────────────
  if (wants("Education") && Array.isArray(candidate.Education) && candidate.Education.length > 0) {
    const lines = candidate.Education.map((e) => {
      const parts = [];
      if (e.UnderGraduationDegree)
        parts.push(`UG: ${e.UnderGraduationDegree}${e.underGraduationUniversityName ? ` (${e.underGraduationUniversityName})` : ""}`);
      if (e.PostGraduationDegree)
        parts.push(`PG: ${e.PostGraduationDegree}${e.postGraduationUniversityName ? ` (${e.postGraduationUniversityName})` : ""}`);
      return parts.join(" | ");
    }).filter(Boolean);
    if (lines.length) flat["Education"] = lines.join("; ");
  }

  // ── Experience ────────────────────────────────────────────────────────────
  if (wants("Experience") && Array.isArray(candidate.Experience) && candidate.Experience.length > 0) {
    const lines = candidate.Experience
      .filter((e) => e.companyName && e.companyName.trim() !== "")
      .map((e) => `${e.companyName} — ${e.role || "N/A"}${e.isCurrentCompany ? " (Current)" : ""}`);
    if (lines.length) flat["Experience"] = lines.join("; ");
  }

  // ── Applications + Interviews ─────────────────────────────────────────────
  if (wants("Applications") && Array.isArray(candidate.Applications) && candidate.Applications.length > 0) {
    const jobs = [...new Set(candidate.Applications.map((a) => a.JobTitle).filter(Boolean))];
    if (jobs.length) flat["Jobs Applied"] = jobs.join(", ");

    const statuses = [...new Set(candidate.Applications.map((a) => a.Status).filter(Boolean))];
    if (statuses.length) flat["Application Status"] = statuses.join(", ");

    flat["Shortlisted"] = candidate.Applications.some((a) => a.isShortlisted) ? "Yes" : "No";
    flat["Accepted"]    = candidate.Applications.some((a) => a.isAccepted)    ? "Yes" : "No";

    // Null-guard: only flatMap Interviews when it is actually an array
    const allInterviews = candidate.Applications.flatMap(
      (a) => (Array.isArray(a.Interviews) ? a.Interviews : []).map((iv) => ({ ...iv, JobTitle: a.JobTitle }))
    );

    if (allInterviews.length > 0) {
      if (isDetail) {
        const ivLines = allInterviews.map((iv) => {
          const parts = [];
          if (iv.JobTitle)        parts.push(`Job: ${iv.JobTitle}`);
          if (iv.InterviewStatus) parts.push(`Status: ${iv.InterviewStatus}`);
          if (iv.Interviewer)     parts.push(`Interviewer: ${iv.Interviewer}`);
          if (iv.isQualified != null) parts.push(`Qualified: ${iv.isQualified ? "Yes" : "No"}`);
          if (iv.marksObtained != null) parts.push(`Marks: ${iv.marksObtained}/${iv.totalMarks ?? "?"}`);
          if (iv.feedback?.trim()) parts.push(`Feedback: "${iv.feedback.trim()}"`);
          return parts.join(" | ");
        }).filter(Boolean);
        if (ivLines.length) flat["Interview Details"] = ivLines.join("\n");
      } else {
        const uniqueStatuses = [...new Set(allInterviews.map((iv) => iv.InterviewStatus).filter(Boolean))];
        if (uniqueStatuses.length) flat["Interview Status"] = uniqueStatuses.join(", ");
      }

      // Float-safe marks — parseFloat handles "30.0", "30", and bare 30
      const allMarks = allInterviews
        .map((iv) => parseFloat(iv.marksObtained))
        .filter((m) => !isNaN(m));
      if (allMarks.length > 0) {
        const best = Math.max(...allMarks);
        const iv = allInterviews.find((i) => parseFloat(i.marksObtained) === best);
        flat["Best Marks"] = `${best}/${iv?.totalMarks ?? "?"}`;
      }
    }
  }

  // ── Documents (detail only) ───────────────────────────────────────────────
  if (isDetail && candidate.Documents && typeof candidate.Documents === "object") {
    const doc = candidate.Documents;
    const list = [
      doc.adharPath        ? "Aadhar"           : null,
      doc.pancardPath      ? "PAN Card"          : null,
      doc.bankpassbook     ? "Bank Passbook"     : null,
      doc.bankStatement    ? "Bank Statement"    : null,
      doc.salarySlip       ? "Salary Slip"       : null,
      doc.expierenceLetter ? "Experience Letter" : null,
      doc.offerLetter      ? "Offer Letter"      : null,
      doc.itr              ? "ITR"               : null,
    ].filter(Boolean);
    if (list.length) flat["Documents Uploaded"] = list.join(", ");
  }

  // ── Resumes (detail only) ─────────────────────────────────────────────────
  if (isDetail && Array.isArray(candidate.Resumes) && candidate.Resumes.length > 0)
    flat["Resumes Uploaded"] = candidate.Resumes.length.toString();

  return flat;
}

// ═════════════════════════════════════════════════════════════════════════════
// RESPONSE BUILDERS
// ═════════════════════════════════════════════════════════════════════════════

function degreeShort(eduStr) {
  if (!eduStr || eduStr === "—") return null;
  return eduStr
    .split(";")[0]
    .trim()
    .replace(/\s*\([^)]*\)/g, "")
    .replace(/^(UG:|PG:)\s*/i, "")
    .trim();
}

function currentRole(expStr) {
  if (!expStr || expStr === "—") return null;
  const current = expStr.split(";").find((e) => e.includes("(Current)"));
  return current ? current.replace("(Current)", "").trim() : null;
}

function locationStr(r) {
  return [r["City"], r["State"]].filter((v) => v && v !== "—").join(", ") || null;
}

// ── TYPE 0 — NAME_LIST ───────────────────────────────────────────────────────
// User only wants names. Produces a clean numbered list, paginated at 50.
// No location/education/skill context — just sequential names.
function buildNameList(flatRows, totalFound) {
  const SHOW = 50;
  const shown = flatRows.slice(0, SHOW);
  const remaining = totalFound - shown.length;

  const lines = shown.map((r, i) => `${i + 1}. **${r["Name"]}**`);

  const footer = remaining > 0
    ? `

*Showing ${shown.length} of ${totalFound} candidates. Ask for a report to get the full list.*`
    : `

*Total: ${totalFound} candidate${totalFound !== 1 ? "s" : ""}.*`;

  return lines.join("") + footer;
}

// ── TYPE 1 — LIST ─────────────────────────────────────────────────────────────
function buildList(flatRows, totalFound) {
  const SHOW = 8;
  const shown = flatRows.slice(0, SHOW);
  const remaining = totalFound - shown.length;

  const parts = shown.map((r) => {
    const segments = [];
    const loc = locationStr(r);
    if (loc) segments.push(`who lives in ${loc}`);
    const deg = degreeShort(r["Education"]);
    if (deg) segments.push(`with education in ${deg}`);
    if (r["Skills"] && r["Skills"] !== "—") segments.push(`and skills in ${r["Skills"]}`);
    const role = currentRole(r["Experience"]);
    if (role) segments.push(`currently working as ${role}`);
    if (r["Shortlisted"] === "Yes") segments.push("shortlisted");
    if (r["Accepted"] === "Yes") segments.push("accepted");
    return `**${r["Name"]}**${segments.length ? ", " + segments.join(", ") : ""}`;
  });

  if (remaining > 0) parts.push(`and **${remaining} more candidate${remaining > 1 ? "s" : ""}**`);
  return parts.join("; ") + ".";
}

// ── TYPE 2 — SKILLS_ONLY ──────────────────────────────────────────────────────
function buildSkillsOnly(flatRows, totalFound, sectionsNeeded) {
  const sec = Array.isArray(sectionsNeeded) ? sectionsNeeded : [];
  const featuredField = sec.includes("Skills")                  ? "Skills"
    : sec.includes("Education")                                  ? "Education"
    : sec.includes("Experience")                                 ? "Experience"
    : sec.includes("Applications.Interviews")                    ? "Interview Details"
    : sec.includes("Applications")                               ? "Jobs Applied"
    : sec.includes("Documents")                                  ? "Documents Uploaded"
    : sec.includes("Resumes")                                    ? "Resumes Uploaded"
    : "Skills";

  const fieldLabel = {
    "Skills":            "skills in",
    "Education":         "education:",
    "Experience":        "experience at",
    "Interview Details": "interview details:",
    "Jobs Applied":      "applied for",
    "Documents Uploaded":"documents uploaded:",
    "Resumes Uploaded":  "resumes uploaded:",
  }[featuredField] || "details:";

  const nameCounts = {};
  flatRows.forEach((r) => {
    nameCounts[r["Name"]] = (nameCounts[r["Name"]] || 0) + 1;
  });

  const lines = flatRows.map((r) => {
    const val = r[featuredField];
    let label = `**${r["Name"]}**`;
    if (nameCounts[r["Name"]] > 1) {
      const disambig = r["City"] && r["City"] !== "—"
        ? r["City"]
        : r["Email"] && r["Email"] !== "—"
        ? r["Email"]
        : null;
      if (disambig) label = `**${r["Name"]}** (${disambig})`;
    }
    if (!val || val === "—")
      return `${label} has no ${featuredField.toLowerCase()} recorded.`;
    return `${label} has ${fieldLabel} ${val}.`;
  });

  const hidden = totalFound - flatRows.length;
  const footer = hidden > 0 ? `\n\n*Showing ${flatRows.length} of ${totalFound} candidates.*` : "";
  return lines.join("\n") + footer;
}

// ── TYPE 3 — FULL_DETAILS ─────────────────────────────────────────────────────
function buildFullDetails(flatRows) {
  const nameCounts = {};
  flatRows.forEach((r) => { nameCounts[r["Name"]] = (nameCounts[r["Name"]] || 0) + 1; });

  const profiles = flatRows.map((r) => {
    const sentences = [];
    const loc = locationStr(r);
    const nameLabel = nameCounts[r["Name"]] > 1 && r["Email"] && r["Email"] !== "—"
      ? `**${r["Name"]}** (${r["Email"]})`
      : `**${r["Name"]}**`;
    sentences.push(`${nameLabel}${loc ? ` lives in ${loc}` : ""}.`);
    if (r["Email"] && r["Email"] !== "—") sentences.push(`Email: ${r["Email"]}, Phone: ${r["Phone"] || "—"}.`);
    if (r["Education"] && r["Education"] !== "—") sentences.push(`Education: ${r["Education"]}.`);
    if (r["Skills"] && r["Skills"] !== "—") sentences.push(`Skills: ${r["Skills"]}.`);
    if (r["Experience"] && r["Experience"] !== "—") {
      sentences.push(`Experience: ${r["Experience"]}.`);
    } else {
      sentences.push("No work experience listed.");
    }
    if (r["Jobs Applied"] && r["Jobs Applied"] !== "—") sentences.push(`Applied for: ${r["Jobs Applied"]}.`);
    if (r["Shortlisted"] === "Yes") sentences.push("Currently shortlisted ✓.");
    if (r["Accepted"] === "Yes") sentences.push("Offer accepted ✓.");
    if (r["Interview Details"] && r["Interview Details"] !== "—")
      sentences.push(`Interview: ${r["Interview Details"].replace(/\n/g, " | ")}.`);
    else if (r["Interview Status"] && r["Interview Status"] !== "—")
      sentences.push(`Interview status: ${r["Interview Status"]}.`);
    if (r["Best Marks"] && r["Best Marks"] !== "—") sentences.push(`Best marks: ${r["Best Marks"]}.`);
    if (r["Documents Uploaded"] && r["Documents Uploaded"] !== "—") sentences.push(`Documents: ${r["Documents Uploaded"]}.`);
    return sentences.join(" ");
  });

  return profiles.join("\n\n");
}

// ── TYPE 4 — COMPARE ──────────────────────────────────────────────────────────
function buildCompare(flatRows, totalFound) {
  const compared = flatRows.slice(0, 4);
  const remaining = totalFound - compared.length;
  const lines = [];

  const eduDiff = compared.some((r) => r["Education"] !== compared[0]["Education"]);
  if (eduDiff) {
    const eduParts = compared.map((r) => `**${r["Name"]}** has ${degreeShort(r["Education"]) || "no degree listed"}`);
    lines.push(eduParts.join(", while ") + ".");
  }

  const skillParts = compared.map((r) => {
    const s = r["Skills"] && r["Skills"] !== "—" ? r["Skills"] : "no skills listed";
    return `**${r["Name"]}** has skills in ${s}`;
  });
  lines.push(skillParts.join(", while ") + ".");

  const expParts = compared.map((r) => {
    const role = currentRole(r["Experience"]);
    return role
      ? `**${r["Name"]}** is currently working as ${role}`
      : `**${r["Name"]}** has no current role listed`;
  });
  if (expParts.length > 0) lines.push(expParts.join(", whereas ") + ".");

  const locDiff = compared.some((r) => locationStr(r) !== locationStr(compared[0]));
  if (locDiff) {
    const locParts = compared.map((r) => {
      const loc = locationStr(r);
      return `**${r["Name"]}** is from ${loc || "unknown location"}`;
    });
    lines.push(locParts.join(" and ") + ".");
  }

  const statusParts = compared
    .filter((r) => r["Shortlisted"] === "Yes" || r["Accepted"] === "Yes")
    .map((r) => {
      if (r["Accepted"] === "Yes") return `**${r["Name"]}** has been accepted`;
      return `**${r["Name"]}** is shortlisted`;
    });
  if (statusParts.length) lines.push(statusParts.join(", and ") + ".");

  const marksParts = compared
    .filter((r) => r["Best Marks"] && r["Best Marks"] !== "—")
    .map((r) => `**${r["Name"]}** scored ${r["Best Marks"]}`);
  if (marksParts.length > 1) lines.push(marksParts.join(" while ") + ".");

  if (remaining > 0)
    lines.push(`*${remaining} more candidate${remaining > 1 ? "s" : ""} not shown. Generate a report for the full list.*`);

  return lines.join("\n");
}

// ── TYPE 5 — SUMMARY ──────────────────────────────────────────────────────────
function buildSummary(flatRows, totalFound) {
  const total = totalFound;

  const skillFreq = {};
  flatRows.forEach((r) => {
    if (Array.isArray(r["_allSkills"])) {
      r["_allSkills"].forEach((s) => {
        skillFreq[s] = (skillFreq[s] || 0) + 1;
      });
    }
  });
  const topSkills = Object.entries(skillFreq)
    .sort((a, b) => b[1] - a[1])
    .slice(0, 5)
    .map(([s]) => s);

  const hasUG = flatRows.filter((r) => r["Education"] && r["Education"].includes("UG:")).length;
  const hasPG = flatRows.filter((r) => r["Education"] && r["Education"].includes("PG:")).length;
  const hasExp = flatRows.filter((r) => r["Experience"] && r["Experience"] !== "—").length;
  const noExp  = flatRows.length - hasExp;
  const shortlisted = flatRows.filter((r) => r["Shortlisted"] === "Yes").length;
  const accepted    = flatRows.filter((r) => r["Accepted"]    === "Yes").length;
  const cities = [...new Set(flatRows.map((r) => r["City"]).filter((c) => c && c !== "—"))];

  const lines = [];
  lines.push(`Out of **${total} candidates** found:`);
  if (topSkills.length > 0) lines.push(`• Most common skills are **${topSkills.join(", ")}**.`);
  if (hasUG > 0 || hasPG > 0) {
    const eduParts = [];
    if (hasUG > 0) eduParts.push(`${hasUG} have an undergraduate degree`);
    if (hasPG > 0) eduParts.push(`${hasPG} have a postgraduate degree`);
    lines.push(`• ${eduParts.join(", and ")}.`);
  }
  if (hasExp > 0 || noExp > 0) {
    lines.push(`• **${hasExp}** have work experience, **${noExp}** are freshers.`);
  }
  if (shortlisted > 0) lines.push(`• **${shortlisted}** are shortlisted${accepted > 0 ? `, **${accepted}** have been accepted` : ""}.`);
  if (cities.length > 0) {
    const cityStr = cities.length <= 4
      ? cities.join(", ")
      : `${cities.slice(0, 4).join(", ")} and ${cities.length - 4} more cities`;
    lines.push(`• Candidates are from **${cityStr}**.`);
  }

  const shown = flatRows.length;
  if (shown < total)
    lines.push(`\n*Showing summary based on ${shown} of ${total} candidates. Generate a report for full details.*`);

  return lines.join("\n");
}

// ═════════════════════════════════════════════════════════════════════════════
// formatTable — used when user explicitly asks for table/compare in table
// ═════════════════════════════════════════════════════════════════════════════
function formatTable(flatRows, options = {}) {
  const { totalFound = flatRows.length, isSingleCandidate = false, sectionsNeeded = null } = options;
  if (flatRows.length === 0) return null;

  let priorityCols;
  if (isSingleCandidate && Array.isArray(sectionsNeeded) && sectionsNeeded.length > 0) {
    priorityCols = getColumnsForSections(sectionsNeeded);
  } else {
    priorityCols = isSingleCandidate ? DETAIL_COLUMNS : [
      "Name", "City", "State", "Skills", "Education", "Experience",
      "Jobs Applied", "Application Status", "Shortlisted", "Accepted",
      "Best Marks", "Interview Status",
    ];
  }

  const usedKeys = priorityCols.filter((k) =>
    flatRows.some((r) => r[k] && r[k] !== "—" && !k.startsWith("_")),
  );
  if (usedKeys.length === 0) return null;

  const headers   = usedKeys.join(" | ");
  const separator = usedKeys.map(() => "---").join(" | ");
  const rows = flatRows
    .map((row) => usedKeys.map((k) => String(row[k] || "—").replace(/\n/g, "<br>")).join(" | "))
    .join("\n");

  const hidden = totalFound - flatRows.length;
  const footer = hidden > 0
    ? `\n\n*Showing ${flatRows.length} of ${totalFound}. Generate a report for all results.*`
    : "";

  return `${headers}\n${separator}\n${rows}${footer}`;
}

export async function generateNaturalAnswer(
  question,
  sqlQuery,
  rows,
  options = {},
  onChunk = null,
) {
  const client = getOpenAIClient();

  if (!rows || rows.length === 0) {
    const msg = "No records found matching your query.";
    if (onChunk) await onChunk(msg);
    return msg;
  }

  const {
    totalFound = rows.length,
    isSingleCandidate = false,
    sectionsNeeded = null,
  } = options;

  // 🧠 Detect type
  const responseType = options.responseType || detectResponseType(
    question,
    isSingleCandidate,
    totalFound,
    sectionsNeeded
  );

  // 🧾 Flatten
  const flatMode =
    responseType === "FULL_DETAILS" || responseType === "SKILLS_ONLY"
      ? "detail"
      : "summary";

  const flatRows = rows.map((r) =>
    flattenCandidate(
      r,
      flatMode,
      responseType === "SKILLS_ONLY" ? sectionsNeeded : null
    )
  );

  // ============================================================
  // ✅ ALWAYS SHOW FULL NAME LIST (IMPORTANT FIX)
  // ============================================================
  let fullList = "";

  if (responseType === "NAME_LIST" || totalFound > 10) {
    fullList = buildNameList(flatRows, totalFound);
  }

  // ============================================================
  // ⚡ SIMPLE CASE (NO AI)
  // ============================================================
 if (
  responseType === "NAME_LIST" ||
  responseType === "SKILLS_ONLY" ||
  responseType === "FULL_DETAILS" ||
  responseType === "MULTI_SECTION"
) {
  let body;

  switch (responseType) {
    case "NAME_LIST":
      body = buildNameList(flatRows, totalFound);
      break;
    case "SKILLS_ONLY":
      body = buildSkillsOnly(flatRows, totalFound, sectionsNeeded);
      break;
    case "FULL_DETAILS":
      body = buildFullDetails(flatRows);
      break;
    case "MULTI_SECTION":
      body = buildMultiSection(rows, sectionsNeeded);
      break;
    default:
      body = buildList(flatRows, totalFound);
  }

  return body;
}

  // In generateNaturalAnswer, where you check options.responseType
// if (options.responseType === "MULTI_SECTION") {
//   return rows.map((c, i) => {
//     const edu = (c.Education || [])[0];
//     const skills = (c.Skills || []).map(s => s.Skill).filter(Boolean).join(", ");

//     const lines = [`**${c.FullName}**`];

//     if (skills) {
//       lines.push(`- **Skills:** ${skills}`);
//     } else {
//       lines.push(`- **Skills:** Not listed`);
//     }

//     if (edu) {
//       const ugPart = edu.UnderGraduationDegree
//         ? `${edu.UnderGraduationDegree}${edu.underGraduationUniversityName ? ` from ${edu.underGraduationUniversityName}` : ""}${edu.underGraduationEndYear ? ` (${edu.underGraduationEndYear})` : ""}`
//         : null;
//       const pgPart = edu.PostGraduationDegree
//         ? `${edu.PostGraduationDegree}${edu.postGraduationUniversityName ? ` from ${edu.postGraduationUniversityName}` : ""}${edu.postGraduationEndYear ? ` (${edu.postGraduationEndYear})` : ""}`
//         : null;

//       if (ugPart) lines.push(`- **UG:** ${ugPart}`);
//       if (pgPart) lines.push(`- **PG:** ${pgPart}`);
//       if (!ugPart && !pgPart) lines.push(`- **Education:** Not listed`);
//     } else {
//       lines.push(`- **Education:** Not listed`);
//     }

//     return lines.join("\n");
//   }).join("\n\n");
// }

  // ============================================================
  // 🤖 AI + FULL LIST MERGED
  // ============================================================
  try {
    const limitedRows = flatRows.slice(0, 5); // only top for explanation

    const aiResponse = await client.chat.completions.create({
      model: "gpt-4o-mini",
      temperature: 0.4,
      max_tokens: 300,
      messages: [
        {
          role: "system",
          content: `
You are a smart HR assistant.

Your job:
- Explain candidates naturally like ChatGPT
- DO NOT list all candidates
- Focus on top relevant candidates
- Mention filters like skills, city, experience, shortlist

Keep it natural and short.
`,
        },
        {
          role: "user",
          content: `
User question: "${question}"

Total candidates: ${totalFound}

Top candidates:
${JSON.stringify(limitedRows, null, 2)}

Write a natural explanation.
`,
        },
      ],
    });

    const aiText = aiResponse.choices[0].message.content.trim();

    const finalAnswer =
      fullList
        ? `${fullList}\n\n---\n\n${aiText}`
        : aiText;

    if (onChunk) {
      await onChunk(finalAnswer);
      return null;
    }

    return finalAnswer;

  } catch (err) {
    log.error("AI failed, fallback:", err);

    const fallback = buildList(flatRows, totalFound);
    return `${fullList}\n\n${fallback}`;
  }
}

function buildMultiSection(rows, sectionsNeeded) {
  const sec = new Set(Array.isArray(sectionsNeeded) ? sectionsNeeded : []);

  return rows.map((c) => {
    const lines = [`**${c.FullName}**`];

    // ── Profile / born year / location ───────────────────────
    if (sec.has("Profile")) {
      const p = c.Profile || {};
      if (p.dateOfBirth) {
        const dob = new Date(p.dateOfBirth);
        const born = isNaN(dob) ? p.dateOfBirth : 
          dob.toLocaleDateString("en-IN", { day: "2-digit", month: "short", year: "numeric" });
        lines.push(`- **Date of Birth:** ${born} (Born: ${isNaN(dob) ? "?" : dob.getFullYear()})`);
      }
      if (p.Gender)          lines.push(`- **Gender:** ${p.Gender}`);
      if (p.city || p.state) lines.push(`- **Location:** ${[p.city, p.state].filter(Boolean).join(", ")}`);
      if (p.languageKnown)   lines.push(`- **Languages:** ${p.languageKnown}`);
      if (p.isStudying != null) lines.push(`- **Currently Studying:** ${p.isStudying ? "Yes" : "No"}`);
      if (p.linkedinProfileUrl) lines.push(`- **LinkedIn:** ${p.linkedinProfileUrl}`);
      if (p.portfolioGithubWebsiteUrl) lines.push(`- **Portfolio:** ${p.portfolioGithubWebsiteUrl}`);
    }

    // ── Skills ────────────────────────────────────────────────
    if (sec.has("Skills")) {
      const skills = (c.Skills || []).map(s => s.Skill).filter(Boolean).join(", ");
      lines.push(`- **Skills:** ${skills || "Not listed"}`);
    }

    // ── Education ─────────────────────────────────────────────
    if (sec.has("Education")) {
      const edu = (c.Education || [])[0];
      if (edu) {
        const ugPart = edu.UnderGraduationDegree
          ? `${edu.UnderGraduationDegree}${edu.underGraduationUniversityName ? ` from ${edu.underGraduationUniversityName}` : ""}${edu.underGraduationEndYear ? ` (${edu.underGraduationEndYear})` : ""}`
          : null;
        const pgPart = edu.PostGraduationDegree
          ? `${edu.PostGraduationDegree}${edu.postGraduationUniversityName ? ` from ${edu.postGraduationUniversityName}` : ""}${edu.postGraduationEndYear ? ` (${edu.postGraduationEndYear})` : ""}`
          : null;
        if (ugPart) lines.push(`- **UG:** ${ugPart}`);
        if (pgPart) lines.push(`- **PG:** ${pgPart}`);
        if (!ugPart && !pgPart) lines.push(`- **Education:** Not listed`);
      } else {
        lines.push(`- **Education:** Not listed`);
      }
    }

    // ── Experience ────────────────────────────────────────────
    if (sec.has("Experience")) {
      const exps = (c.Experience || [])
        .filter(e => e.companyName?.trim())
        .map(e => `${e.companyName} — ${e.role || "N/A"}${e.isCurrentCompany ? " (Current)" : ""}`)
        .join("; ");
      lines.push(`- **Experience:** ${exps || "Not listed"}`);
    }

    // ── Applications ──────────────────────────────────────────
    if (sec.has("Applications") && !sec.has("Applications.Interviews")) {
      const apps = (c.Applications || [])
        .map(a => `${a.JobTitle || "?"}${a.Status ? ` (${a.Status})` : ""}${a.isShortlisted ? " ✓ Shortlisted" : ""}`)
        .join("; ");
      lines.push(`- **Applications:** ${apps || "None"}`);
    }

    // ── Interviews ────────────────────────────────────────────
    if (sec.has("Applications.Interviews")) {
      const interviews = (c.Applications || [])
        .flatMap(a => (Array.isArray(a.Interviews) ? a.Interviews : [])
          .map(iv => {
            const parts = [`Job: ${a.JobTitle || "?"}`];
            if (iv.InterviewStatus) parts.push(`Status: ${iv.InterviewStatus}`);
            if (iv.Interviewer)     parts.push(`Interviewer: ${iv.Interviewer}`);
            if (iv.isQualified != null) parts.push(`Qualified: ${iv.isQualified ? "Yes" : "No"}`);
            if (iv.marksObtained != null) parts.push(`Marks: ${iv.marksObtained}${iv.totalMarks ? `/${iv.totalMarks}` : ""}`);
            if (iv.feedback?.trim()) parts.push(`Feedback: "${iv.feedback.trim()}"`);
            return parts.join(" | ");
          })
        );
      lines.push(`- **Interviews:** ${interviews.join("\n  ") || "None"}`);
    }

    // ── Documents ─────────────────────────────────────────────
    if (sec.has("Documents")) {
      const doc = c.Documents || {};
      const uploaded = [
        doc.adharPath        ? "Aadhar"           : null,
        doc.pancardPath      ? "PAN Card"          : null,
        doc.bankpassbook     ? "Bank Passbook"     : null,
        doc.bankStatement    ? "Bank Statement"    : null,
        doc.salarySlip       ? "Salary Slip"       : null,
        doc.expierenceLetter ? "Experience Letter" : null,
        doc.offerLetter      ? "Offer Letter"      : null,
        doc.itr              ? "ITR"               : null,
      ].filter(Boolean);
      lines.push(`- **Documents:** ${uploaded.length ? uploaded.join(", ") : "None uploaded"}`);
    }

    // ── Resumes ───────────────────────────────────────────────
    if (sec.has("Resumes")) {
      const resumes = (c.Resumes || []).filter(r => r.resume?.trim());
      lines.push(`- **Resumes:** ${resumes.length ? `${resumes.length} uploaded` : "None uploaded"}`);
    }

    return lines.join("\n");
  }).join("\n\n");
}