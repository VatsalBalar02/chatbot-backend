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

// ─────────────────────────────────────────────────────────────────────────────
// flattenCandidateForDisplay
// ─────────────────────────────────────────────────────────────────────────────
// Converts a rich nested candidate object (with Applications[].Interviews[],
// Skills[], Education[], etc.) into a flat, human-readable object that the
// answer LLM can present in a table WITHOUT unrolling nested arrays into
// separate rows.
//
// The rule: every array section is collapsed into a single readable string.
// This means "5 candidates" → exactly 5 rows in the final table, no matter
// how many interviews or applications each candidate has.
//
function flattenCandidateForDisplay(candidate) {
  const flat = {};

  // ── Root ─────────────────────────────────────────────────────────────────
  flat["Name"] = candidate.FullName || "—";
  flat["Email"] = candidate.email || "—";
  flat["Phone"] = candidate.phoneNumber || "—";
  flat["Verified"] = candidate.isVerified ? "Yes" : "No";
  flat["Profile Complete"] = candidate.isProfileComplete ? "Yes" : "No";
  if (candidate.RegisteredOn) {
    flat["Registered On"] = new Date(candidate.RegisteredOn).toLocaleDateString(
      "en-IN",
      { day: "2-digit", month: "short", year: "numeric" },
    );
  }

  // ── Profile ───────────────────────────────────────────────────────────────
  const p = candidate.Profile;
  if (p) {
    if (p.Gender) flat["Gender"] = p.Gender;
    if (p.city) flat["City"] = p.city;
    if (p.state) flat["State"] = p.state;
    if (p.languageKnown) flat["Languages"] = p.languageKnown;
    if (p.isStudying != null)
      flat["Currently Studying"] = p.isStudying ? "Yes" : "No";
    if (p.linkedinProfileUrl) flat["LinkedIn"] = p.linkedinProfileUrl;
    if (p.portfolioGithubWebsiteUrl)
      flat["Portfolio/GitHub"] = p.portfolioGithubWebsiteUrl;
    if (p.dateOfBirth)
      flat["Date of Birth"] = new Date(p.dateOfBirth).toLocaleDateString(
        "en-IN",
        { day: "2-digit", month: "short", year: "numeric" },
      );
  }

  // ── Skills ────────────────────────────────────────────────────────────────
  if (Array.isArray(candidate.Skills) && candidate.Skills.length > 0) {
    flat["Skills"] = candidate.Skills.map((s) => s.Skill)
      .filter(Boolean)
      .join(", ");
  }

  // ── Education ─────────────────────────────────────────────────────────────
  if (Array.isArray(candidate.Education) && candidate.Education.length > 0) {
    const eduLines = candidate.Education.map((e) => {
      const parts = [];
      if (e.UnderGraduationDegree)
        parts.push(
          `UG: ${e.UnderGraduationDegree}${e.underGraduationUniversityName ? ` (${e.underGraduationUniversityName})` : ""}`,
        );
      if (e.PostGraduationDegree)
        parts.push(
          `PG: ${e.PostGraduationDegree}${e.postGraduationUniversityName ? ` (${e.postGraduationUniversityName})` : ""}`,
        );
      return parts.join(" | ");
    }).filter(Boolean);
    if (eduLines.length) flat["Education"] = eduLines.join("; ");
  }

  // ── Experience ────────────────────────────────────────────────────────────
  if (Array.isArray(candidate.Experience) && candidate.Experience.length > 0) {
    const expLines = candidate.Experience.filter(
      (e) => e.companyName && e.companyName.trim() !== "",
    ).map((e) => {
      const current = e.isCurrentCompany ? " (Current)" : "";
      return `${e.companyName} — ${e.role || "N/A"}${current}`;
    });
    if (expLines.length) flat["Experience"] = expLines.join("; ");
  }

  // ── Applications + Interviews ─────────────────────────────────────────────
  // Collapse ALL interviews across ALL applications into a single summary
  // string so the table stays one row per candidate.
  if (
    Array.isArray(candidate.Applications) &&
    candidate.Applications.length > 0
  ) {
    // Jobs applied for
    const jobTitles = [
      ...new Set(candidate.Applications.map((a) => a.JobTitle).filter(Boolean)),
    ];
    if (jobTitles.length) flat["Jobs Applied"] = jobTitles.join(", ");

    // Application statuses
    const appStatuses = [
      ...new Set(candidate.Applications.map((a) => a.Status).filter(Boolean)),
    ];
    if (appStatuses.length) flat["Application Status"] = appStatuses.join(", ");

    // Shortlisted / Accepted flags
    const shortlisted = candidate.Applications.some((a) => a.isShortlisted);
    const accepted = candidate.Applications.some((a) => a.isAccepted);
    flat["Shortlisted"] = shortlisted ? "Yes" : "No";
    flat["Accepted"] = accepted ? "Yes" : "No";

    // Interview summary — collapse to one readable line per application
    const interviewLines = [];
    for (const app of candidate.Applications) {
      if (!Array.isArray(app.Interviews) || app.Interviews.length === 0)
        continue;
      for (const iv of app.Interviews) {
        const parts = [];
        if (app.JobTitle) parts.push(`Job: ${app.JobTitle}`);
        if (iv.InterviewStatus) parts.push(`Status: ${iv.InterviewStatus}`);
        if (iv.Interviewer) parts.push(`Interviewer: ${iv.Interviewer}`);
        if (iv.isQualified != null)
          parts.push(`Qualified: ${iv.isQualified ? "Yes" : "No"}`);
        if (iv.marksObtained != null)
          parts.push(`Marks: ${iv.marksObtained}/${iv.totalMarks ?? "?"}`);
        if (iv.feedback && iv.feedback.trim())
          parts.push(`Feedback: "${iv.feedback.trim()}"`);
        if (parts.length) interviewLines.push(parts.join(" | "));
      }
    }
    if (interviewLines.length) {
      flat["Interview Details"] = interviewLines.join("\n");
    }
  }

  // ── Documents ─────────────────────────────────────────────────────────────
  if (candidate.Documents && typeof candidate.Documents === "object") {
    const doc = candidate.Documents;
    const docList = [
      doc.adharPath ? "Aadhar" : null,
      doc.pancardPath ? "PAN Card" : null,
      doc.bankpassbook ? "Bank Passbook" : null,
      doc.bankStatement ? "Bank Statement" : null,
      doc.salarySlip ? "Salary Slip" : null,
      doc.expierenceLetter ? "Experience Letter" : null,
      doc.offerLetter ? "Offer Letter" : null,
      doc.itr ? "ITR" : null,
    ].filter(Boolean);
    if (docList.length) flat["Documents Uploaded"] = docList.join(", ");
  }

  // ── Resumes ───────────────────────────────────────────────────────────────
  if (Array.isArray(candidate.Resumes) && candidate.Resumes.length > 0) {
    flat["Resumes Uploaded"] = candidate.Resumes.length.toString();
  }

  return flat;
}

// ─────────────────────────────────────────────────────────────────────────────
// generateNaturalAnswer
// ─────────────────────────────────────────────────────────────────────────────
// rows    — array of candidate objects from the cache (nested JSON)
// options — { userLimit: number|null, totalFound: number }
//
// userLimit: the number the user explicitly asked for ("5 candidates" → 5).
//            Used to tell the LLM "show exactly N candidates, one row each".
// totalFound: the real total matching candidates in the DB, used for the
//             "there are X more" footer note.
//
// export async function generateNaturalAnswer(
//   question,
//   sqlQuery,
//   rows,
//   options = {},
// ) {
//   const client = getOpenAIClient();

//   if (!rows || rows.length === 0) {
//     return "No records found matching your query.";
//   }

//   const { userLimit = null, totalFound = rows.length } = options;

//   // Flatten every candidate into a clean display object.
//   // This is the key step that prevents interview rows from exploding
//   // the table — each candidate becomes exactly ONE flat object.
//   const flatRows = rows.map(flattenCandidateForDisplay);

//   // Remove keys that are all "—" / empty across every row to keep table clean
//   const allKeys = [...new Set(flatRows.flatMap((r) => Object.keys(r)))];
//   const usedKeys = allKeys.filter((k) =>
//     flatRows.some((r) => r[k] && r[k] !== "—"),
//   );
//   const cleanRows = flatRows.map((r) =>
//     Object.fromEntries(usedKeys.map((k) => [k, r[k] ?? "—"])),
//   );

//   const displayCount = cleanRows.length; // rows actually sent
//   const hiddenCount = totalFound - displayCount; // still in DB but not shown
//   const requestedCount = userLimit ?? displayCount;

//   const dataContext = JSON.stringify(cleanRows, null, 2);

//   const systemPrompt = `
// You are a helpful HR and recruitment assistant chatbot.

// The data below has already been pre-processed: each object represents
// exactly ONE candidate. Do NOT split a candidate into multiple rows.

// STRICT RULES:
// 1. ONE ROW PER CANDIDATE in any table you produce — never one row per interview,
//    one row per application, or one row per skill. The data is already flattened.
// 2. The user asked for ${requestedCount} candidate(s). Show exactly ${displayCount} candidate row(s) — no more, no less.
// 3. Use a clean markdown table. Column headers come from the JSON keys.
// 4. If a field contains multiple values separated by newlines (like "Interview Details"),
//    keep it in a single table cell — do not split it into rows.
// 5. Format dates as human-readable (e.g. "12 Feb 2026"). Never show raw ISO strings.
// 6. Never show raw UUIDs as identifiers — use Name or Email instead.
// 7. Never mention SQL, queries, stored procedures, cache, or any technical terms.
// 8. Keep tone friendly and professional.
// 9. Start with one natural sentence summarising what you found.
// ${hiddenCount > 0 ? `10. After the table, add a note: "Showing ${displayCount} of ${totalFound} matching candidates. Generate a report to see all results."` : ""}
// `.trim();

//   const userPrompt = `
// User question: "${question}"

// Candidate data (${displayCount} candidate${displayCount !== 1 ? "s" : ""}):
// ${dataContext}

// Please give a clear, well-formatted answer with a markdown table.
// One row per candidate only.
// `.trim();

//   const resp = await client.chat.completions.create({
//     model: "gpt-4o-mini",
//     messages: [
//       { role: "system", content: systemPrompt },
//       { role: "user", content: userPrompt },
//     ],
//     temperature: 0.3,
//     max_tokens: 1500,
//   });

//   return (resp.choices[0].message.content || "").trim();
// }

// ─────────────────────────────────────────────────────────────────────────────
// formatCandidatesAsMarkdown (NEW HELPER FUNCTION)
// ─────────────────────────────────────────────────────────────────────────────
// Formats candidate data as a clean markdown table CLIENT-SIDE (instant)
// This replaces the slow LLM-based table formatting
//
function formatCandidatesAsMarkdown(flatRows, options = {}) {
  const { totalFound = flatRows.length } = options;

  if (flatRows.length === 0) return null;

  // Get all unique keys across all rows
  const allKeys = [...new Set(flatRows.flatMap((r) => Object.keys(r)))];
  
  // Keep only columns that have at least one non-empty value
  const usedKeys = allKeys.filter((k) =>
    flatRows.some((r) => r[k] && r[k] !== "—")
  );

  // Build markdown table
  const headers = usedKeys.join(" | ");
  const separator = usedKeys.map(() => "---").join(" | ");
  const rows = flatRows
    .map((row) =>
      usedKeys
        .map((key) => {
          const value = row[key] || "—";
          // Replace newlines with <br> for markdown rendering
          return value.replace(/\n/g, "<br>");
        })
        .join(" | ")
    )
    .join("\n");

  const table = `${headers}\n${separator}\n${rows}`;

  // Add footer if there are hidden results
  const hiddenCount = totalFound - flatRows.length;
  const footer =
    hiddenCount > 0
      ? `\n\n*Showing ${flatRows.length} of ${totalFound} matching candidates. Generate a report to see all results.*`
      : "";

  return table + footer;
}

// ─────────────────────────────────────────────────────────────────────────────
// generateNaturalAnswer (OPTIMIZED VERSION)
// ─────────────────────────────────────────────────────────────────────────────
// PERFORMANCE IMPROVEMENTS:
// 1. Client-side table formatting (instant instead of 2-4 seconds)
// 2. Minimal AI call only for natural intro sentence
// 3. Reduced token count (50 tokens vs 1500+)
// 4. Optional streaming for even faster perceived performance
//
export async function generateNaturalAnswer(
  question,
  sqlQuery,
  rows,
  options = {},
) {
  const client = getOpenAIClient();

  if (!rows || rows.length === 0) {
    return "No records found matching your query.";
  }

  const { userLimit = null, totalFound = rows.length } = options;

  // Flatten every candidate into a clean display object
  const flatRows = rows.map(flattenCandidateForDisplay);

  // Remove keys that are all "—" / empty across every row
  const allKeys = [...new Set(flatRows.flatMap((r) => Object.keys(r)))];
  const usedKeys = allKeys.filter((k) =>
    flatRows.some((r) => r[k] && r[k] !== "—"),
  );
  const cleanRows = flatRows.map((r) =>
    Object.fromEntries(usedKeys.map((k) => [k, r[k] ?? "—"])),
  );

  const displayCount = cleanRows.length;
  const hiddenCount = totalFound - displayCount;

  // Format table client-side (instant!)
  const table = formatCandidatesAsMarkdown(cleanRows, { totalFound });

  // Use AI only for a natural intro sentence (much faster)
  const systemPrompt = `You are a helpful HR assistant. Write ONE brief, friendly intro sentence that summarizes what was found. Keep it under 20 words.`;

  const userPrompt = `User asked: "${question}"
Found: ${displayCount} candidate${displayCount !== 1 ? "s" : ""}${hiddenCount > 0 ? ` (${totalFound} total match, showing ${displayCount})` : ""}

Write a brief intro sentence only.`;

  try {
    const resp = await client.chat.completions.create({
      model: "gpt-4o-mini",
      messages: [
        { role: "system", content: systemPrompt },
        { role: "user", content: userPrompt },
      ],
      temperature: 0.3,
      max_tokens: 50, // Reduced from 1500!
    });

    const intro = (resp.choices[0].message.content || "").trim();
    return `${intro}\n\n${table}`;
  } catch (error) {
    log.error("Error generating intro:", error);
    // Fallback to simple intro if AI fails
    const fallbackIntro = `Found ${displayCount} candidate${displayCount !== 1 ? "s" : ""} matching your query:`;
    return `${fallbackIntro}\n\n${table}`;
  }
}
