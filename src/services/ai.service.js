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

export async function classifyIntent(question) {
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
  Examples: "what is 2+2", "hi", "write a poem", "what is Python", "tell me a joke"

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
      { role: "user", content: question },
    ],
    max_tokens: 10,
    temperature: 0,
  });

  const intent = (resp.choices[0].message.content || "").trim().toUpperCase();
  const valid = ["SQL", "PDF", "REPORT", "OUT_OF_SCOPE"];
  const result = valid.includes(intent) ? intent : "OUT_OF_SCOPE";
  log.info(`Intent: ${result}`);
  return result;
}

// ═════════════════════════════════════════════════════════════════════════════
// RESPONSE TYPE DETECTION
// ═════════════════════════════════════════════════════════════════════════════
//
// Six response types — picked automatically from the question + data:
//
//   COUNT_ANSWER  — "how many candidates have React?" → plain count sentence
//   SKILLS_ONLY   — "skills of avinash", "avinash education" → focused single-field
//   FULL_DETAILS  — "tell me about avinash", result ≤ 3 → complete prose profile
//   COMPARE       — user says "compare / vs / difference between" → side-by-side prose
//   SUMMARY       — result > 10, broad list → aggregated insight paragraph
//   LIST          — everything else → inline prose list, first 8 + "and X more"
//
function detectResponseType(question, isSingleCandidate, totalFound, sectionsNeeded) {
  const q = question.toLowerCase();

  // ── COMPARE — explicit comparison keywords ────────────────────────────────
  const compareWords = ["compare", "vs", "versus", "difference between", "contrast", "side by side"];
  if (compareWords.some((w) => q.includes(w))) return "COMPARE";

  // ── SKILLS_ONLY — user asked for a single specific field ──────────────────
  // Triggered when sectionsNeeded is a small focused array (1–2 sections,
  // neither of which is "ALL") AND the question mentions a field keyword.
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

  // ── FULL_DETAILS — specific candidate, broad ask, small result set ────────
  if (isSingleCandidate && totalFound <= 3) return "FULL_DETAILS";

  // ── SUMMARY — large result set with no specific filter ────────────────────
  if (totalFound > 10 && !isSingleCandidate) return "SUMMARY";

  // ── LIST — default for moderate result sets ───────────────────────────────
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
//
// mode = "detail"   → all fields, full interview breakdown
// mode = "summary"  → summary fields only (name, city, edu, top skills, exp, status)
//
// sectionsNeeded — when provided, gates each section so only requested
//                  fields are populated (e.g. ["root","Skills"] → Name + Skills only)
//
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
      // summary mode without section filter → always include city/state
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
    flat["_allSkills"] = all; // raw array for SUMMARY aggregation
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

    const allInterviews = candidate.Applications.flatMap(
      (a) => (a.Interviews || []).map((iv) => ({ ...iv, JobTitle: a.JobTitle }))
    );

    if (allInterviews.length > 0) {
      if (isDetail) {
        const ivLines = allInterviews.map((iv) => {
          const p = [];
          if (iv.JobTitle) p.push(`Job: ${iv.JobTitle}`);
          if (iv.InterviewStatus) p.push(`Status: ${iv.InterviewStatus}`);
          if (iv.Interviewer) p.push(`Interviewer: ${iv.Interviewer}`);
          if (iv.isQualified != null) p.push(`Qualified: ${iv.isQualified ? "Yes" : "No"}`);
          if (iv.marksObtained != null) p.push(`Marks: ${iv.marksObtained}/${iv.totalMarks ?? "?"}`);
          if (iv.feedback?.trim()) p.push(`Feedback: "${iv.feedback.trim()}"`);
          return p.join(" | ");
        }).filter(Boolean);
        if (ivLines.length) flat["Interview Details"] = ivLines.join("\n");
      } else {
        const uniqueStatuses = [...new Set(allInterviews.map((iv) => iv.InterviewStatus).filter(Boolean))];
        if (uniqueStatuses.length) flat["Interview Status"] = uniqueStatuses.join(", ");
      }

      // Best marks — always compute for both modes
      const allMarks = allInterviews
        .map((iv) => iv.marksObtained)
        .filter((m) => m != null)
        .map(Number);
      if (allMarks.length > 0) {
        const best = Math.max(...allMarks);
        const iv = allInterviews.find((i) => Number(i.marksObtained) === best);
        flat["Best Marks"] = `${best}/${iv?.totalMarks ?? "?"}`;
      }
    }
  }

  // ── Documents (detail only) ───────────────────────────────────────────────
  if (isDetail && candidate.Documents && typeof candidate.Documents === "object") {
    const doc = candidate.Documents;
    const list = [
      doc.adharPath       ? "Aadhar"            : null,
      doc.pancardPath     ? "PAN Card"           : null,
      doc.bankpassbook    ? "Bank Passbook"      : null,
      doc.bankStatement   ? "Bank Statement"     : null,
      doc.salarySlip      ? "Salary Slip"        : null,
      doc.expierenceLetter? "Experience Letter"  : null,
      doc.offerLetter     ? "Offer Letter"       : null,
      doc.itr             ? "ITR"                : null,
    ].filter(Boolean);
    if (list.length) flat["Documents Uploaded"] = list.join(", ");
  }

  // ── Resumes (detail only) ─────────────────────────────────────────────────
  if (isDetail && Array.isArray(candidate.Resumes) && candidate.Resumes.length > 0)
    flat["Resumes Uploaded"] = candidate.Resumes.length.toString();

  return flat;
}

// ═════════════════════════════════════════════════════════════════════════════
// RESPONSE BUILDERS — one per type
// ═════════════════════════════════════════════════════════════════════════════

// ── Helpers ───────────────────────────────────────────────────────────────────

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

// ─────────────────────────────────────────────────────────────────────────────
// TYPE 1 — LIST
// Inline prose, first 8 candidates, "and X more" at end.
// Format: "Name1, who lives in Location with education in Degree and skills in
//          S1, S2, S3; Name2, who...; and X more candidates."
// ─────────────────────────────────────────────────────────────────────────────
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

// ─────────────────────────────────────────────────────────────────────────────
// TYPE 2 — SKILLS_ONLY
// Focused single-field answer. Reads from whatever field the question asked about.
// ─────────────────────────────────────────────────────────────────────────────
function buildSkillsOnly(flatRows, totalFound, sectionsNeeded) {
  // Determine which field to feature based on requested sections
  const sec = Array.isArray(sectionsNeeded) ? sectionsNeeded : [];
  const featuredField = sec.includes("Skills")                  ? "Skills"
    : sec.includes("Education")                                  ? "Education"
    : sec.includes("Experience")                                 ? "Experience"
    : sec.includes("Applications.Interviews")                    ? "Interview Details"
    : sec.includes("Applications")                               ? "Jobs Applied"
    : sec.includes("Documents")                                  ? "Documents Uploaded"
    : sec.includes("Resumes")                                    ? "Resumes Uploaded"
    : "Skills"; // fallback

  const fieldLabel = {
    "Skills":            "skills in",
    "Education":         "education:",
    "Experience":        "experience at",
    "Interview Details": "interview details:",
    "Jobs Applied":      "applied for",
    "Documents Uploaded":"documents uploaded:",
    "Resumes Uploaded":  "resumes uploaded:",
  }[featuredField] || "details:";

  // Check if multiple candidates share the same name — if so, add a
  // disambiguator (city or email) so the user knows which is which.
  const nameCounts = {};
  flatRows.forEach((r) => {
    nameCounts[r["Name"]] = (nameCounts[r["Name"]] || 0) + 1;
  });

  const lines = flatRows.map((r) => {
    const val = r[featuredField];

    // Build a disambiguated label when name is not unique
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

// ─────────────────────────────────────────────────────────────────────────────
// TYPE 3 — FULL_DETAILS
// Complete prose profile per candidate. Used when ≤ 3 candidates match.
// ─────────────────────────────────────────────────────────────────────────────
function buildFullDetails(flatRows) {
  // Check for duplicate names — add email as disambiguator if needed
  const nameCounts = {};
  flatRows.forEach((r) => { nameCounts[r["Name"]] = (nameCounts[r["Name"]] || 0) + 1; });

  const profiles = flatRows.map((r) => {
    const sentences = [];

    // Identity + location (with disambiguator if name is shared)
    const loc = locationStr(r);
    const nameLabel = nameCounts[r["Name"]] > 1 && r["Email"] && r["Email"] !== "—"
      ? `**${r["Name"]}** (${r["Email"]})`
      : `**${r["Name"]}**`;
    sentences.push(`${nameLabel}${loc ? ` lives in ${loc}` : ""}.`);

    // Contact
    if (r["Email"] && r["Email"] !== "—") sentences.push(`Email: ${r["Email"]}, Phone: ${r["Phone"] || "—"}.`);

    // Education
    if (r["Education"] && r["Education"] !== "—")
      sentences.push(`Education: ${r["Education"]}.`);

    // Skills
    if (r["Skills"] && r["Skills"] !== "—")
      sentences.push(`Skills: ${r["Skills"]}.`);

    // Experience
    if (r["Experience"] && r["Experience"] !== "—") {
      sentences.push(`Experience: ${r["Experience"]}.`);
    } else {
      sentences.push("No work experience listed.");
    }

    // Applications
    if (r["Jobs Applied"] && r["Jobs Applied"] !== "—")
      sentences.push(`Applied for: ${r["Jobs Applied"]}.`);
    if (r["Shortlisted"] === "Yes") sentences.push("Currently shortlisted ✓.");
    if (r["Accepted"] === "Yes") sentences.push("Offer accepted ✓.");

    // Interview
    if (r["Interview Details"] && r["Interview Details"] !== "—")
      sentences.push(`Interview: ${r["Interview Details"].replace(/\n/g, " | ")}.`);
    else if (r["Interview Status"] && r["Interview Status"] !== "—")
      sentences.push(`Interview status: ${r["Interview Status"]}.`);

    // Best marks
    if (r["Best Marks"] && r["Best Marks"] !== "—")
      sentences.push(`Best marks: ${r["Best Marks"]}.`);

    // Documents
    if (r["Documents Uploaded"] && r["Documents Uploaded"] !== "—")
      sentences.push(`Documents: ${r["Documents Uploaded"]}.`);

    return sentences.join(" ");
  });

  return profiles.join("\n\n");
}

// ─────────────────────────────────────────────────────────────────────────────
// TYPE 4 — COMPARE
// Side-by-side prose comparison. Focuses on differences.
// ─────────────────────────────────────────────────────────────────────────────
function buildCompare(flatRows, totalFound) {
  // Only compare first 4 for readability
  const compared = flatRows.slice(0, 4);
  const remaining = totalFound - compared.length;

  const lines = [];

  // Education comparison
  const eduDiff = compared.some((r) => r["Education"] !== compared[0]["Education"]);
  if (eduDiff) {
    const eduParts = compared.map((r) => `**${r["Name"]}** has ${degreeShort(r["Education"]) || "no degree listed"}`);
    lines.push(eduParts.join(", while ") + ".");
  }

  // Skills comparison
  const skillParts = compared.map((r) => {
    const s = r["Skills"] && r["Skills"] !== "—" ? r["Skills"] : "no skills listed";
    return `**${r["Name"]}** has skills in ${s}`;
  });
  lines.push(skillParts.join(", while ") + ".");

  // Experience comparison
  const expParts = compared.map((r) => {
    const role = currentRole(r["Experience"]);
    return role
      ? `**${r["Name"]}** is currently working as ${role}`
      : `**${r["Name"]}** has no current role listed`;
  });
  if (expParts.length > 0) lines.push(expParts.join(", whereas ") + ".");

  // Location comparison
  const locDiff = compared.some((r) => locationStr(r) !== locationStr(compared[0]));
  if (locDiff) {
    const locParts = compared.map((r) => {
      const loc = locationStr(r);
      return `**${r["Name"]}** is from ${loc || "unknown location"}`;
    });
    lines.push(locParts.join(" and ") + ".");
  }

  // Shortlisted / Accepted status
  const statusParts = compared
    .filter((r) => r["Shortlisted"] === "Yes" || r["Accepted"] === "Yes")
    .map((r) => {
      if (r["Accepted"] === "Yes") return `**${r["Name"]}** has been accepted`;
      return `**${r["Name"]}** is shortlisted`;
    });
  if (statusParts.length) lines.push(statusParts.join(", and ") + ".");

  // Best marks if available
  const marksParts = compared
    .filter((r) => r["Best Marks"] && r["Best Marks"] !== "—")
    .map((r) => `**${r["Name"]}** scored ${r["Best Marks"]}`);
  if (marksParts.length > 1) lines.push(marksParts.join(" while ") + ".");

  if (remaining > 0)
    lines.push(`*${remaining} more candidate${remaining > 1 ? "s" : ""} not shown. Generate a report for the full list.*`);

  return lines.join("\n");
}

// ─────────────────────────────────────────────────────────────────────────────
// TYPE 5 — SUMMARY
// Aggregated insight paragraph for large result sets (> 10 candidates).
// ─────────────────────────────────────────────────────────────────────────────
function buildSummary(flatRows, totalFound) {
  const total = totalFound;

  // Top skills — count frequency
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

  // Education breakdown
  const hasUG = flatRows.filter((r) => r["Education"] && r["Education"].includes("UG:")).length;
  const hasPG = flatRows.filter((r) => r["Education"] && r["Education"].includes("PG:")).length;

  // Experience
  const hasExp = flatRows.filter((r) => r["Experience"] && r["Experience"] !== "—").length;
  const noExp  = flatRows.length - hasExp;

  // Shortlisted / Accepted
  const shortlisted = flatRows.filter((r) => r["Shortlisted"] === "Yes").length;
  const accepted    = flatRows.filter((r) => r["Accepted"]    === "Yes").length;

  // Location spread
  const cities = [...new Set(flatRows.map((r) => r["City"]).filter((c) => c && c !== "—"))];

  const lines = [];

  // Opening
  lines.push(`Out of **${total} candidates** found:`);

  // Skills insight
  if (topSkills.length > 0)
    lines.push(`• Most common skills are **${topSkills.join(", ")}**.`);

  // Education insight
  if (hasUG > 0 || hasPG > 0) {
    const eduParts = [];
    if (hasUG > 0) eduParts.push(`${hasUG} have an undergraduate degree`);
    if (hasPG > 0) eduParts.push(`${hasPG} have a postgraduate degree`);
    lines.push(`• ${eduParts.join(", and ")}.`);
  }

  // Experience insight
  if (hasExp > 0 || noExp > 0) {
    lines.push(`• **${hasExp}** have work experience, **${noExp}** are freshers.`);
  }

  // Application status
  if (shortlisted > 0) lines.push(`• **${shortlisted}** are shortlisted${accepted > 0 ? `, **${accepted}** have been accepted` : ""}.`);

  // Location spread
  if (cities.length > 0) {
    const cityStr = cities.length <= 4
      ? cities.join(", ")
      : `${cities.slice(0, 4).join(", ")} and ${cities.length - 4} more cities`;
    lines.push(`• Candidates are from **${cityStr}**.`);
  }

  // Footer
  const shown = flatRows.length;
  if (shown < total)
    lines.push(`\n*Showing summary based on ${shown} of ${total} candidates. Generate a report for full details.*`);

  return lines.join("\n");
}

// ═════════════════════════════════════════════════════════════════════════════
// formatTable — used only when user explicitly asks for table/compare in table
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

// ═════════════════════════════════════════════════════════════════════════════
// generateNaturalAnswer — MAIN EXPORT
// ═════════════════════════════════════════════════════════════════════════════
//
// Automatically picks the best response type for every query.
// Streams via onChunk if provided, otherwise returns full string.
//
export async function generateNaturalAnswer(
  question,
  sqlQuery,
  rows,
  options = {},
  onChunk = null,
) {
  const client = getOpenAIClient();

  // ── No data ───────────────────────────────────────────────────────────────
  if (!rows || rows.length === 0) {
    const msg = "No records found matching your query.";
    if (onChunk) await onChunk(msg);
    return msg;
  }

  const {
    userLimit    = null,
    totalFound   = rows.length,
    isSingleCandidate = false,
    sectionsNeeded    = null,
  } = options;

  // ── Detect response type ──────────────────────────────────────────────────
  const responseType = detectResponseType(question, isSingleCandidate, totalFound, sectionsNeeded);
  log.info(`[ResponseType] ${responseType} | totalFound: ${totalFound} | isSingle: ${isSingleCandidate} | sections: ${JSON.stringify(sectionsNeeded)}`);

  // ── Flatten rows with the right mode ─────────────────────────────────────
  // "detail"  → all fields (FULL_DETAILS, SKILLS_ONLY with section filter)
  // "summary" → summary fields (LIST, COMPARE, SUMMARY)
  const flatMode = (responseType === "FULL_DETAILS" || responseType === "SKILLS_ONLY")
    ? "detail"
    : "summary";

  const flatRows = rows.map((r) =>
    flattenCandidate(
      r,
      flatMode,
      // Pass sectionsNeeded only for SKILLS_ONLY so only the asked field is populated
      responseType === "SKILLS_ONLY" ? sectionsNeeded : null,
    )
  );

  const displayCount = flatRows.length;
  const hiddenCount  = totalFound - displayCount;

  // ── Build the body (sync, instant, zero extra AI cost) ────────────────────
  let body;
  switch (responseType) {
    case "LIST":
      body = buildList(flatRows, totalFound);
      break;
    case "SKILLS_ONLY":
      body = buildSkillsOnly(flatRows, totalFound, sectionsNeeded);
      break;
    case "FULL_DETAILS":
      body = buildFullDetails(flatRows);
      break;
    case "COMPARE":
      body = buildCompare(flatRows, totalFound);
      break;
    case "SUMMARY":
      body = buildSummary(flatRows, totalFound);
      break;
    default:
      body = buildList(flatRows, totalFound);
  }

  // ── AI intro sentence (short, contextual, streamed first) ─────────────────
  // Intro is tailored per response type so it feels natural
  const introGuide = {
    LIST:        "Write a short intro like 'Here are the candidates I found:' or similar. Under 15 words.",
    SKILLS_ONLY: "Write a short intro referencing what was asked. Under 15 words.",
    FULL_DETAILS:"Write a short intro for a candidate profile. Under 15 words.",
    COMPARE:     "Write a short intro for a comparison. Under 15 words.",
    SUMMARY:     "Write a short intro for a summary overview. Under 15 words.",
  }[responseType] || "Write a short intro. Under 15 words.";

  const systemPrompt = `You are a helpful HR assistant. ${introGuide} No markdown.`;
  const userPrompt = `User asked: "${question}"\nFound: ${displayCount} candidate${displayCount !== 1 ? "s" : ""}${hiddenCount > 0 ? ` (${totalFound} total)` : ""}.\nWrite the intro sentence only.`;

  // ── STREAMING ─────────────────────────────────────────────────────────────
  if (onChunk) {
    try {
      const stream = await client.chat.completions.create({
        model: "gpt-4o-mini",
        messages: [
          { role: "system", content: systemPrompt },
          { role: "user", content: userPrompt },
        ],
        temperature: 0.3,
        max_tokens: 40,
        stream: true,
      });
      for await (const chunk of stream) {
        const token = chunk.choices[0]?.delta?.content || "";
        if (token) await onChunk(token);
      }
      await onChunk("\n\n" + body);
    } catch (err) {
      log.error("Streaming intro error:", err);
      await onChunk(`Found ${displayCount} candidate${displayCount !== 1 ? "s" : ""} matching your query.\n\n` + body);
    }
    return null;
  }

  // ── NON-STREAMING ─────────────────────────────────────────────────────────
  try {
    const resp = await client.chat.completions.create({
      model: "gpt-4o-mini",
      messages: [
        { role: "system", content: systemPrompt },
        { role: "user", content: userPrompt },
      ],
      temperature: 0.3,
      max_tokens: 40,
    });
    const intro = (resp.choices[0].message.content || "").trim();
    return `${intro}\n\n${body}`;
  } catch (err) {
    log.error("Intro generation error:", err);
    return `Found ${displayCount} candidate${displayCount !== 1 ? "s" : ""} matching your query.\n\n${body}`;
  }
}