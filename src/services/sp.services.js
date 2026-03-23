// // src/services/sp.service.js
// //
// // Stored Procedure Engine — hardened for SPs with 200+ parameters
// //
// // ─────────────────────────────────────────────────────────────────────────────
// // Flow:
// //   1. discoverStoredProcedures()  — fetch SP names + params from MSSQL
// //   2. generateSpDescription()     — LLM description (param-sampled, token-safe)
// //   3. buildSpEmbeddings()         — embed descriptions (cached in-memory)
// //   4. selectBestSp()              — cosine similarity → top-3 → confidence check
// //   5. classifySpParams()          — split params into REQUIRED / OPTIONAL
// //   6. extractSpParams()           — LLM extracts values (required params only)
// //   7. executeStoredProcedure()    — safe mssql request.input() execution
// //   8. runSpMode()                 — orchestrates the full SP flow
// // ─────────────────────────────────────────────────────────────────────────────

// import { getPool } from "../db/connection.js";
// import { getOpenAIClient } from "./ai.service.js";
// import { embedText } from "../utils/embedding.js";
// import { generateNaturalAnswer } from "./ai.service.js";
// import { log } from "../utils/logger.js";
// import { SP_METADATA_HINTS } from "../config/sp.metadata.js";

// // ─── In-memory cache ──────────────────────────────────────────────────────────
// let spMetadataCache = null;   // [{ name, params[], requiredParams[], optionalParams[] }]
// let spEmbeddingCache = null;  // [{ ...metadata, description, embedding[] }]

// const SP_CONFIDENCE_THRESHOLD = 0.45;  // 0.45 catches near-matches like 0.4999 while blocking true misses (<0.40)

// // ─── Param-level hints — helps LLM understand what each param means ───────────
// // Key = param name (exact, case-sensitive). Value = plain English description.
// // Add any param here whose name alone is ambiguous to the LLM.
// const PARAM_HINTS = {
//   // GetJobsAdvanced
//   "Search":    "keyword to search in job title (e.g. 'react', 'manager', 'developer')",
//   "MinSalary": "minimum salary amount as a number",
//   "MaxSalary": "maximum salary amount as a number",
//   "JobType":   "type/category of job as a UUID — leave null if not mentioned",
//   "Status":    "job status UUID — leave null if not mentioned",
//   "Page":      "page number for pagination, default 1",
//   "PageSize":  "number of results per page, default 10",

//   // usp_GetCandidateListWithFilters
//   "SearchName":        "candidate's name to search for",
//   "SearchEmail":       "candidate's email address to search for",
//   "IsVerified":        "1 if user wants verified candidates, 0 for unverified, null for both",
//   "IsProfileComplete": "1 if user wants profile-complete candidates, 0 for incomplete, null for both",
//   "CityFilter":        "city name to filter candidates by location",
//   "StateFilter":       "state name to filter candidates by location",
//   "SkillId":           "specific skill UUID — leave null if not mentioned",
//   "PageNumber":        "page number for pagination, default 1",

//   // usp_GetCandidateFullProfile
//   "CandidateId": "unique candidate UUID — required, must be provided by user",
// };
// const MAX_SP_CANDIDATES       = 3;

// // ─── Limits that make large SPs safe ─────────────────────────────────────────
// // FIX #1 — description generation: sample params instead of sending all 200+
// const DESC_HEAD_PARAMS = 15;   // first N params (usually the key filter params)
// const DESC_TAIL_PARAMS = 5;    // last N params  (often output/flag params)

// // FIX #2 — param extraction: only send REQUIRED params to LLM
// // A param is "required" if its name suggests it is a meaningful filter.
// // Everything else is OPTIONAL and will be bound as NULL without asking the LLM.
// const MAX_REQUIRED_PARAMS = 20;  // hard ceiling even after classification

// // ─── 1. Discover all SPs from MSSQL ──────────────────────────────────────────
// export async function discoverStoredProcedures() {
//   if (spMetadataCache) {
//     log.info(`SP cache hit — ${spMetadataCache.length} SPs loaded`);
//     return spMetadataCache;
//   }

//   const pool = await getPool();

//   const spResult = await pool.request().query(`
//     SELECT ROUTINE_NAME
//     FROM INFORMATION_SCHEMA.ROUTINES
//     WHERE ROUTINE_TYPE = 'PROCEDURE'
//       AND ROUTINE_SCHEMA = 'dbo'
//     ORDER BY ROUTINE_NAME
//   `);

//   const spNames = spResult.recordset.map((r) => r.ROUTINE_NAME);

//   if (spNames.length === 0) {
//     log.warn("No stored procedures found in database.");
//     spMetadataCache = [];
//     return spMetadataCache;
//   }

//   const metadata = [];

//   for (const name of spNames) {
//     const paramResult = await pool.request().query(`
//       SELECT
//         PARAMETER_NAME,
//         DATA_TYPE,
//         PARAMETER_MODE,
//         ORDINAL_POSITION
//       FROM INFORMATION_SCHEMA.PARAMETERS
//       WHERE SPECIFIC_NAME = '${name}'
//         AND PARAMETER_NAME <> ''
//       ORDER BY ORDINAL_POSITION
//     `);

//     const params = paramResult.recordset.map((p) => {
//       const rawName = p.PARAMETER_NAME.replace(/^@/, "");
//       const rawMode = (p.PARAMETER_MODE || "IN").toUpperCase();

//       // MSSQL INFORMATION_SCHEMA quirk:
//       // OUTPUT params come back as 'INOUT' not 'OUT'.
//       // Also catch common OUTPUT param naming as a safety net.
//       const nameUpper = rawName.toUpperCase();
//       const isOutputByName = (
//         nameUpper === "RESULTCODE"   ||
//         nameUpper === "RESULTMESSAGE" ||
//         nameUpper === "TOTALCOUNT"   ||
//         nameUpper === "OUTPUTMESSAGE"||
//         nameUpper === "ERRORMESSAGE" ||
//         nameUpper === "ERRORCODE"
//       );
//       const isOutput = rawMode === "OUT" || rawMode === "INOUT" || isOutputByName;

//       return {
//         name:     rawName,
//         type:     p.DATA_TYPE,
//         mode:     isOutput ? "OUT" : "IN",
//         position: p.ORDINAL_POSITION,
//       };
//     });

//     // Classify immediately at discovery time so we never re-do it
//     const { requiredParams, optionalParams } = classifySpParams(params);

//     metadata.push({ name, params, requiredParams, optionalParams });
//   }

//   spMetadataCache = metadata;
//   log.info(`SP discovery complete — ${metadata.length} SPs found`);
//   return metadata;
// }

// // ─── FIX #2 — Classify params into REQUIRED vs OPTIONAL ─────────────────────
// //
// // Problem: an SP with 200 params cannot be sent entirely to the LLM for param
// // extraction — it blows the token budget and confuses the model.
// //
// // Strategy:
// //   REQUIRED — first positional params up to MAX_REQUIRED_PARAMS, and any
// //              param whose name contains a "filter keyword" (id, code, date,
// //              type, status, state, flag, from, to, start, end, filter, search).
// //              These are the ones the user is likely to mention in their query.
// //
// //   OPTIONAL  — everything else. Will be bound as NULL automatically.
// //              The LLM never sees these — zero token cost.

// const REQUIRED_KEYWORDS = [
//   "id", "code", "date", "type", "status", "state", "flag",
//   "from", "to", "start", "end", "filter", "search", "name",
//   "category", "region", "district", "year", "month",
// ];

// export function classifySpParams(params) {
//   const required = [];
//   const optional = [];

//   for (const p of params) {
//     const lname = p.name.toLowerCase();
//     const isKeyword = REQUIRED_KEYWORDS.some((kw) => lname.includes(kw));
//     const isEarlyPosition = p.position <= MAX_REQUIRED_PARAMS;

//     if (isKeyword || isEarlyPosition) {
//       required.push(p);
//     } else {
//       optional.push(p);
//     }
//   }

//   // Hard ceiling: never send more than MAX_REQUIRED_PARAMS to LLM
//   const cappedRequired = required.slice(0, MAX_REQUIRED_PARAMS);
//   // Anything pushed out of required goes to optional
//   const overflow = required.slice(MAX_REQUIRED_PARAMS);

//   return {
//     requiredParams: cappedRequired,
//     optionalParams: [...optional, ...overflow],
//   };
// }

// // ─── 2. Generate LLM description (FIX #1 — token-safe for large SPs) ─────────
// //
// // Problem: 200 params = ~3 000 tokens just for the param list.
// // We only need a representative sample to understand SP intent.
// // Take head (key filter params) + tail (output/flag params).

// async function generateSpDescription(spName, params) {
//   // If metadata sentences exist, use the first as the description.
//   // Full sentence embedding is handled in buildSpEmbeddings — this
//   // function only needs to return a single representative string.
//   const hint = SP_METADATA_HINTS[spName];
//   if (hint) {
//     if (Array.isArray(hint.sentences) && hint.sentences.length > 0) {
//       log.info(`  SP "${spName}" using metadata sentences`);
//       return hint.sentences[0];
//     }
//     if (typeof hint === "string" && hint.trim()) {
//       log.info(`  SP "${spName}" using metadata string hint`);
//       return hint.trim();
//     }
//   }

//   // No hint — ask LLM using a token-safe param sample
//   const client = getOpenAIClient();

//   let sampledParams = params;
//   let truncationNote = "";

//   if (params.length > DESC_HEAD_PARAMS + DESC_TAIL_PARAMS) {
//     const head = params.slice(0, DESC_HEAD_PARAMS);
//     const tail = params.slice(-DESC_TAIL_PARAMS);
//     const seen = new Set(head.map((p) => p.name));
//     const uniqueTail = tail.filter((p) => !seen.has(p.name));
//     sampledParams = [...head, ...uniqueTail];
//     truncationNote = ` (${params.length} params total, showing ${sampledParams.length})`;
//   }

//   const paramList =
//     sampledParams.length > 0
//       ? sampledParams.map((p) => `@${p.name} (${p.type})`).join(", ") + truncationNote
//       : "no parameters";

//   const prompt = `
// You are a database analyst. Given a stored procedure name and its parameters,
// write a single concise sentence (max 20 words) describing what this SP likely does.

// Stored Procedure: ${spName}
// Parameters (sample): ${paramList}

// Respond with ONLY the description sentence. No explanation, no formatting.
// `.trim();

//   const resp = await client.chat.completions.create({
//     model: "gpt-4o-mini",
//     messages: [{ role: "user", content: prompt }],
//     max_tokens: 60,
//     temperature: 0,
//   });

//   return (resp.choices[0].message.content || "").trim();
// }

// // ─── 2b. Expand hint into multiple natural sentences ─────────────────────────
// //
// // ROOT CAUSE OF LOW SCORES:
// //
// //   Keyword dump hint:  "show jobs list jobs search jobs salary range status"
// //   Expanded query:     "Retrieve the first 5 jobs from the dataset."
// //   Cosine score:       0.45  ← still low — keyword bags embed differently
// //                               from complete sentences
// //
// // FIX: Convert the hint into 4-6 natural sentences BEFORE embedding.
// //      Each sentence represents a different way a user might ask for this SP.
// //      We embed all of them separately and score the query against every one.
// //      The MAX score wins. This is called "multi-vector retrieval".
// //
// //   Sentence 1: "Retrieve a list of all available job openings."
// //   Sentence 2: "Search and filter jobs by title, salary range, or status."
// //   Sentence 3: "Show me jobs matching a keyword with pagination."
// //   Sentence 4: "Get job listings with total application count."
// //   Query:      "show me first 5 jobs"  →  hits sentence 1 at 0.72  ✅

// async function expandHintToSentences(spName, hint) {
//   const client = getOpenAIClient();

//   const prompt = `
// You are a search query expert.
// Given a stored procedure name and a description hint, generate exactly 5 natural
// English sentences that represent different ways a user might ask for this data.

// SP Name: ${spName}
// Hint: ${hint}

// Rules:
// - Write exactly 5 sentences, one per line
// - Each sentence must be a complete, natural question or request a user would type
// - Vary the phrasing: some as questions, some as commands
// - Use simple everyday language — not technical database terms
// - Do NOT number them, do NOT use bullet points
// - Each line = exactly one sentence

// Output (5 lines):`.trim();

//   try {
//     const resp = await client.chat.completions.create({
//       model: "gpt-4o-mini",
//       messages: [{ role: "user", content: prompt }],
//       max_tokens: 200,
//       temperature: 0.3,
//     });

//     const raw = (resp.choices[0].message.content || "").trim();
//     const sentences = raw
//       .split(" ")
//       .map((s) => s.replace(/^\d+[\.\)]\s*/, "").trim())  // strip any numbering
//       .filter((s) => s.length > 5);

//     // Always include the original hint as a fallback sentence
//     if (!sentences.includes(hint)) sentences.push(hint);

//     return sentences.slice(0, 6);  // max 6 embeddings per SP
//   } catch {
//     // If LLM fails, fall back to just the original hint
//     return [hint];
//   }
// }

// // ─── 3. Build embeddings for all SPs (cached) ────────────────────────────────
// export async function buildSpEmbeddings() {
//   if (spEmbeddingCache) {
//     log.info("SP embedding cache hit — skipping rebuild");
//     return spEmbeddingCache;
//   }

//   const metadata = await discoverStoredProcedures();

//   if (metadata.length === 0) {
//     spEmbeddingCache = [];
//     return spEmbeddingCache;
//   }

//   log.info(`Building embeddings for ${metadata.length} SPs...`);
//   const enriched = [];

//   for (const sp of metadata) {
//     try {
//       const hint = SP_METADATA_HINTS[sp.name];

//       let sentences;
//       let description;
//       let defaults = {};

//       if (hint && Array.isArray(hint.sentences) && hint.sentences.length > 0) {
//         // New format: { sentences[], defaults{} }
//         sentences   = hint.sentences;
//         description = sentences[0];
//         defaults    = hint.defaults || {};
//         log.info(`  SP "${sp.name}" using metadata sentences (no LLM call)`);
//       } else {
//         // No metadata or old string format — ask LLM then expand
//         description = await generateSpDescription(sp.name, sp.params);
//         sentences   = await expandHintToSentences(sp.name, description);
//       }

//       const embeddings = await Promise.all(sentences.map((s) => embedText(s)));

//       enriched.push({ ...sp, description, sentences, embeddings, defaults });
//       log.info(`  ✓ ${sp.name} (${sp.params.length} params) → ${sentences.length} embeddings, ${Object.keys(defaults).length} defaults`);
//       sentences.forEach((s, i) => log.info(`      [${i+1}] "${s}"`));
//     } catch (err) {
//       log.warn(`  ✗ Failed to embed SP "${sp.name}": ${err.message}`);
//     }
//   }

//   spEmbeddingCache = enriched;
//   log.info(`SP embedding build complete — ${enriched.length} SPs embedded`);
//   return spEmbeddingCache;
// }

// // ─── 4a. Query expansion ──────────────────────────────────────────────────────
// // Root cause of low scores: user says "show me all jobs" (6 words, casual) but
// // SP description says "retrieves job listings based on search criteria, salary,
// // job type, status and pagination" (technical, long). They embed into different
// // vector regions — cosine similarity is low even though intent is identical.
// //
// // Fix: rewrite the user query into a technical description sentence BEFORE
// // embedding it. Now both sides speak the same language and score much higher.

// // ─── Query expansion — rule-based (zero LLM cost, ~0ms) ─────────────────────
// // Previous version called LLM for expansion = 600ms wasted per query.
// // Rule-based prefix achieves same result: bridges casual→technical vocabulary.
// // "show me react jobs" → "retrieve and list react jobs"
// // The embedding model handles the rest via semantic similarity.

// const CASUAL_PREFIXES = /^(show\s+me|show|list|get|find|fetch|give\s+me|what\s+are|who\s+are|display|tell\s+me)/i;

// // Common typo map — fixes user typos before embedding so similarity is not hurt
// const TYPO_MAP = {
//   " form ": " from ",   // "candidate form baksa" → "candidate from baksa"
//   " fom ":  " from ",
//   " cnadidate": " candidate",
//   " condiate":  " candidate",
//   " candidte":  " candidate",
//   " joob ": " job ",
//   " jbo ":  " job ",
// };

// // ─── Smart query expansion ───────────────────────────────────────────────────
// //
// // RULE: Only expand queries that contain a recruitment DATA keyword.
// // This is the correct fix — not pattern-blocking on question starters.
// //
// // WHY starter-pattern-blocking is wrong:
// //   "what are the top jobs?"     → SQL ✅ but blocked by "what are"
// //   "describe this candidate"    → SQL ✅ but blocked by "describe"
// //   "explain why rahul failed"   → SQL ✅ but blocked by "explain"
// //
// // WHY data-keyword detection is correct:
// //   "what is Pre-Boarding?"      → no data keyword → don't expand → SP score low → PDF ✅
// //   "what are the top jobs?"     → "jobs" is a data keyword → expand → SP score high ✅
// //   "describe this candidate"    → "candidate" is a data keyword → expand → SP score high ✅
// //   "explain the hiring process" → no specific data keyword → don't expand → PDF ✅

// const DATA_KEYWORDS = [
//   "job", "jobs", "candidate", "candidates", "user", "users",
//   "profile", "application", "applications", "interview", "interviews",
//   "skill", "skills", "resume", "salary", "hiring", "recruitment",
//   "applicant", "applicants", "opening", "openings", "position", "positions",
//   "verified", "unverified", "shortlisted", "rejected", "selected",
// ];

// function expandUserQuery(userQuery) {
//   // Step 1: normalise — lowercase + fix typos
//   let q = userQuery.trim().toLowerCase();
//   for (const [typo, fix] of Object.entries(TYPO_MAP)) {
//     q = q.split(typo).join(fix);
//   }

//   // Step 2: check if query contains any recruitment data keyword.
//   // If NO data keyword → this is likely a PDF/definition/OOS question.
//   // Return unchanged — SP cosine score will naturally be low → correct fallthrough.
//   const hasDataKeyword = DATA_KEYWORDS.some((kw) => q.includes(kw));
//   if (!hasDataKeyword) return q;

//   // Step 3: query has a data keyword → it is a data request → expand it.
//   // Already sounds technical — don't modify further
//   if (/^(retrieve|fetch|get all|list all)/.test(q)) return q;

//   // Strip casual opener and replace with "retrieve and list"
//   const stripped = q.replace(CASUAL_PREFIXES, "").trim();
//   return stripped ? `retrieve and list ${stripped}` : q;
// }

// // ─── 4. Cosine similarity + SP selection ─────────────────────────────────────
// function cosineSimilarity(a, b) {
//   let dot = 0, magA = 0, magB = 0;
//   for (let i = 0; i < a.length; i++) {
//     dot  += a[i] * b[i];
//     magA += a[i] * a[i];
//     magB += b[i] * b[i];
//   }
//   if (magA === 0 || magB === 0) return 0;
//   return dot / (Math.sqrt(magA) * Math.sqrt(magB));
// }

// export async function selectBestSp(userQuery) {
//   const spEmbeddings = await buildSpEmbeddings();

//   if (spEmbeddings.length === 0) {
//     return { sp: null, score: 0, candidates: [] };
//   }

//   // Expand the user query into a richer sentence before embedding.
//   // This bridges the vocabulary gap between casual user language ("show me all jobs")
//   // and SP description language ("retrieves job listings with filters and pagination").
//   // The expanded query lives in the same vector space as the SP descriptions.
//   const expandedQuery = expandUserQuery(userQuery);  // sync — no await needed
//   log.info(`Query expansion: "${userQuery}" → "${expandedQuery}"`);

//   // Run both embeddings in parallel — saves ~200ms per query
//   const [queryEmbedding, originalEmbedding] = await Promise.all([
//     embedText(expandedQuery),
//     embedText(userQuery),
//   ]);

//   const scored = spEmbeddings.map((sp) => {
//     // Each SP now has multiple sentence embeddings — score all, take the best
//     const embeddings = sp.embeddings || [sp.embedding];
//     let bestScore = 0;
//     for (const emb of embeddings) {
//       const s1 = cosineSimilarity(queryEmbedding, emb);
//       const s2 = cosineSimilarity(originalEmbedding, emb);
//       bestScore = Math.max(bestScore, s1, s2);
//     }
//     return { ...sp, score: bestScore };
//   });

//   scored.sort((a, b) => b.score - a.score);
//   const top  = scored.slice(0, MAX_SP_CANDIDATES);
//   const best = top[0];

//   log.info(
//     `SP selection — best: "${best.name}" score: ${best.score.toFixed(4)} | ` +
//     `threshold: ${SP_CONFIDENCE_THRESHOLD}`
//   );

//   if (best.score < SP_CONFIDENCE_THRESHOLD) {
//     log.info("SP confidence below threshold → fallback to SQL");
//     return { sp: null, score: best.score, candidates: top };
//   }

//   return { sp: best, score: best.score, candidates: top };
// }

// // ─── 5+6. LLM param extraction (FIX #2 — REQUIRED params only) ───────────────
// //
// // Problem (old code): sp.params.slice(0, 30) — arbitrary cut, still sends
// // irrelevant params, and for a 200-param SP position 1-30 might all be
// // internal flags with no user-facing meaning.
// //
// // Fix: use the pre-classified requiredParams list (≤ MAX_REQUIRED_PARAMS).
// // Optional params are never sent to the LLM — they are bound as NULL directly
// // in executeStoredProcedure(). This keeps the prompt tiny and focused.

// export async function extractSpParams(userQuery, sp) {
//   const client = getOpenAIClient();

//   // Only send INPUT params to LLM — OUTPUT params (mode === "OUT") are never
//   // user-supplied. Sending them confuses the LLM and wastes tokens.
//   const allRequired = sp.requiredParams || sp.params.slice(0, MAX_REQUIRED_PARAMS);
//   const paramsForLlm = allRequired.filter((p) => p.mode !== "OUT");

//   if (paramsForLlm.length === 0) {
//     log.info("SP has no INPUT params to extract — skipping LLM");
//     return {};
//   }

//   // Build a rich param description for the LLM so it understands each param's purpose
//   const paramList = paramsForLlm
//     .map((p) => {
//       const hint = PARAM_HINTS[p.name] || "";
//       return hint
//         ? `${p.name} (${p.type}) — ${hint}`
//         : `${p.name} (${p.type})`;
//     })
//     .join("\n  ");

//   const prompt = `
// You are a parameter extraction assistant for a recruitment chatbot.
// Extract parameter values for the stored procedure from the user query.

// Stored Procedure: ${sp.name}
// Description: ${sp.description}

// Parameters to fill:
//   ${paramList}

// User query: "${userQuery}"

// Rules:
// - Extract ONLY parameters clearly mentioned or implied in the query.
// - For a Search/keyword param: extract ONLY the actual search term (e.g. "react", not "react developer jobs").
// - Set any parameter NOT mentioned in the query to null.
// - For boolean params: use 1 for yes/true/verified/complete, 0 for no/false/unverified/incomplete.
// - Return ONLY a valid JSON object — no markdown, no explanation.
// - Format: { "paramName": value_or_null, ... }
// - String values must be quoted. Numbers unquoted. Booleans as 1 or 0. Null as null.

// JSON output:
// `.trim();

//   const resp = await client.chat.completions.create({
//     model: "gpt-4o-mini",
//     messages: [{ role: "user", content: prompt }],
//     max_tokens: 400,
//     temperature: 0,
//   });

//   let raw = (resp.choices[0].message.content || "").trim();
//   raw = raw.replace(/^```[a-z]*\n?/, "").replace(/\n?```$/, "");

//   let extracted = {};
//   try {
//     extracted = JSON.parse(raw);
//   } catch {
//     log.warn("SP param extraction JSON parse failed — using empty params");
//   }

//   // Inject SP-level defaults for any param that is null or missing.
//   // Critical for pagination params like @Page, @PageSize — if these are NULL
//   // MSSQL throws "A TOP or FETCH clause contains an invalid value".
//   const defaults = sp.defaults || {};
//   for (const [key, defaultVal] of Object.entries(defaults)) {
//     if (extracted[key] === null || extracted[key] === undefined) {
//       extracted[key] = defaultVal;
//       log.info(`  Injected default for @${key}: ${defaultVal}`);
//     }
//   }

//   return extracted;
// }

// // ─── 7. Safe SP execution (FIX #3 — bind ALL params, optional ones as NULL) ──
// //
// // Problem (old code): only iterates spMeta.params and binds what the LLM
// // returned. For a 200-param SP the LLM only saw ~20 params, so the other 180
// // were never bound — mssql throws "parameter not supplied" errors.
// //
// // Fix: iterate ALL params from spMeta. For each param:
// //   • If it was in the LLM-extracted map → use that value
// //   • Otherwise → bind NULL automatically
// // This ensures every declared param is always bound, no exceptions.

// export async function executeStoredProcedure(spName, extractedParams, spMeta) {
//   // Safety: only execute known SPs from the discovered whitelist
//   const knownSps = (spMetadataCache || []).map((s) => s.name.toLowerCase());
//   if (!knownSps.includes(spName.toLowerCase())) {
//     throw new Error(`SP "${spName}" is not in the allowed list.`);
//   }

//   const pool    = await getPool();
//   const request = pool.request();

//   // Lazy-import mssql types (avoids circular import issues)
//   const sql = await import("mssql");

//   const typeMap = {
//     int:              sql.default.Int,
//     bigint:           sql.default.BigInt,
//     smallint:         sql.default.SmallInt,
//     tinyint:          sql.default.TinyInt,
//     bit:              sql.default.Bit,
//     float:            sql.default.Float,
//     real:             sql.default.Real,
//     decimal:          sql.default.Decimal,
//     numeric:          sql.default.Numeric,
//     money:            sql.default.Money,
//     smallmoney:       sql.default.SmallMoney,
//     varchar:          sql.default.VarChar,
//     nvarchar:         sql.default.NVarChar,
//     char:             sql.default.Char,
//     nchar:            sql.default.NChar,
//     text:             sql.default.Text,
//     ntext:            sql.default.NText,
//     date:             sql.default.Date,
//     datetime:         sql.default.DateTime,
//     datetime2:        sql.default.DateTime2,
//     smalldatetime:    sql.default.SmallDateTime,
//     time:             sql.default.Time,
//     uniqueidentifier: sql.default.UniqueIdentifier,
//     xml:              sql.default.Xml,
//     varbinary:        sql.default.VarBinary,
//     binary:           sql.default.Binary,
//     image:            sql.default.Image,
//   };

//   // Split params into INPUT vs OUTPUT — mssql needs different calls for each.
//   // OUTPUT params (like @ResultCode, @TotalCount) must use request.output()
//   // INPUT params use request.input() as normal.
//   const inputParams  = spMeta.params.filter((p) => p.mode !== "OUT");
//   const outputParams = spMeta.params.filter((p) => p.mode === "OUT");

//   // Bind all INPUT params (extracted value or NULL)
//   for (const paramMeta of inputParams) {
//     const paramName  = paramMeta.name;
//     const sqlType    = typeMap[paramMeta.type.toLowerCase()] || sql.default.NVarChar;
//     const paramValue = Object.prototype.hasOwnProperty.call(extractedParams, paramName)
//       ? extractedParams[paramName]
//       : null;
//     request.input(paramName, sqlType, paramValue);
//   }

//   // Register all OUTPUT params so mssql knows to capture their return values
//   for (const paramMeta of outputParams) {
//     const paramName = paramMeta.name;
//     const sqlType   = typeMap[paramMeta.type.toLowerCase()] || sql.default.NVarChar;
//     request.output(paramName, sqlType);
//   }

//   log.info(
//     `Executing SP "${spName}" — ` +
//     `${inputParams.length} INPUT (${Object.keys(extractedParams).length} from LLM, ` +
//     `${inputParams.length - Object.keys(extractedParams).length} as NULL), ` +
//     `${outputParams.length} OUTPUT registered`
//   );

//   const result = await request.execute(spName);

//   // Log output param values for debugging
//   if (outputParams.length > 0 && result.output) {
//     log.info(`SP output params: ${JSON.stringify(result.output)}`);

//     // If SP signals an error via ResultCode, throw so caller can fallback
//     const resultCode = result.output["ResultCode"] ?? result.output["resultCode"];
//     if (resultCode !== undefined && resultCode < 0) {
//       const msg = result.output["ResultMessage"] ?? result.output["resultMessage"] ?? "SP returned error code";
//       throw new Error(`SP error (code ${resultCode}): ${msg}`);
//     }
//   }

//   // SP may return multiple recordsets (e.g. usp_GetCandidateFullProfile returns 7).
//   // Merge all non-empty recordsets into one flat array for the answer generator.
//   if (result.recordsets && result.recordsets.length > 1) {
//     const merged = result.recordsets.filter((rs) => rs && rs.length > 0).flat();
//     log.info(`SP returned ${result.recordsets.length} recordsets → merged ${merged.length} rows`);
//     return merged;
//   }

//   return result.recordset || result.recordsets?.[0] || [];
// }

// // ─── 8. runSpMode — full SP flow ──────────────────────────────────────────────
// export async function runSpMode(question) {
//   const { sp, score, candidates } = await selectBestSp(question);

//   if (!sp) {
//     return { success: false, reason: "low_confidence", score };
//   }

//   log.info(`Running SP: ${sp.name} | params: ${sp.params.length} total, ${sp.requiredParams.length} required`);

//   // Extract values for REQUIRED params only
//   let extractedParams = {};
//   try {
//     extractedParams = await extractSpParams(question, sp);
//     log.info(`Extracted params: ${JSON.stringify(extractedParams)}`);
//   } catch (err) {
//     log.warn(`Param extraction failed: ${err.message} — binding all as NULL`);
//   }

//   // Execute with ALL params bound (optional ones auto-NULL)
//   let rows = [];
//   try {
//     rows = await executeStoredProcedure(sp.name, extractedParams, sp);
//     log.info(`SP "${sp.name}" returned ${rows.length} row(s)`);
//   } catch (err) {
//     log.error(`SP execution failed: ${err.message}`);
//     return { success: false, reason: "sp_execution_error", error: err.message };
//   }

//   const answer = await generateNaturalAnswer(question, `EXEC ${sp.name}`, rows);

//   return {
//     success:   true,
//     type:      "SP",
//     answer,
//     dataframe: rows,
//     sql:       `EXEC ${sp.name}`,
//     spName:    sp.name,
//     score,
//   };
// }

// // ─── Cache invalidation ───────────────────────────────────────────────────────
// export function clearSpCache() {
//   spMetadataCache  = null;
//   spEmbeddingCache = null;
//   log.info("SP cache cleared.");
// }


// ============================================================
// sp.service.js
//
// Stored Procedure Engine — hardened for SPs with 200+ parameters
// + Structured Data RAG for candidate data (cache-first)
//
// ─────────────────────────────────────────────────────────────
// TWO MODES:
//
//   MODE A — CACHE MODE (candidate questions)
//     sp_GetAllCandidateDetails fires ONCE on server boot.
//     All subsequent candidate questions are answered from the
//     in-memory cache. Zero DB calls per question.
//     → answerFromCache() in candidate.cache.js
//
//   MODE B — SP MODE (everything else)
//     Traditional flow: embed query → find best SP → extract
//     params → execute SP → generate answer.
//     → runSpMode() in this file
//
// ROUTING LOGIC:
//   isCandidateQuestion(query)
//     → true  → MODE A (cache)
//     → false → MODE B (SP selection + execution)
//
// FLOW (MODE B):
//   1. discoverStoredProcedures()  — fetch SP names + params
//   2. generateSpDescription()     — LLM description
//   3. buildSpEmbeddings()         — embed descriptions
//   4. selectBestSp()              — cosine similarity → top-3
//   5. classifySpParams()          — REQUIRED / OPTIONAL split
//   6. extractSpParams()           — LLM extracts values
//   7. executeStoredProcedure()    — safe mssql execution
//   8. runSpMode()                 — orchestrates full SP flow
// ─────────────────────────────────────────────────────────────

import { getPool }               from "../db/connection.js";
import { getOpenAIClient }       from "./ai.service.js";
import { embedText }             from "../utils/embedding.js";
import { generateNaturalAnswer } from "./ai.service.js";
import { log }                   from "../utils/logger.js";
import { SP_METADATA_HINTS }     from "../config/sp.metadata.js";
import {
  warmUp,
  answerFromCache,
  getCacheStatus,
  forceRefresh,
  clearCandidateCache,
} from "./candidate.cache.js";

export { getCacheStatus, forceRefresh, clearCandidateCache };

// ─── In-memory cache (SP discovery + embeddings) ─────────────────────────────
let spMetadataCache  = null;
let spEmbeddingCache = null;

const SP_CONFIDENCE_THRESHOLD = 0.45;

// ─── Param-level hints ────────────────────────────────────────────────────────
const PARAM_HINTS = {
  "Search":            "keyword to search in job title (e.g. 'react', 'manager', 'developer')",
  "MinSalary":         "minimum salary amount as a number",
  "MaxSalary":         "maximum salary amount as a number",
  "JobType":           "type/category of job as a UUID — leave null if not mentioned",
  "Status":            "job status UUID — leave null if not mentioned",
  "Page":              "page number for pagination, default 1",
  "PageSize":          "number of results per page, default 10",
  "SearchName":        "candidate's name to search for",
  "SearchEmail":       "candidate's email address to search for",
  "IsVerified":        "1 if user wants verified candidates, 0 for unverified, null for both",
  "IsProfileComplete": "1 if user wants profile-complete candidates, 0 for incomplete, null for both",
  "CityFilter":        "city name to filter candidates by location",
  "StateFilter":       "state name to filter candidates by location",
  "SkillId":           "specific skill UUID — leave null if not mentioned",
  "PageNumber":        "page number for pagination, default 1",
  "CandidateId":       "unique candidate UUID — required, must be provided by user",
};

const MAX_SP_CANDIDATES  = 3;
const DESC_HEAD_PARAMS   = 15;
const DESC_TAIL_PARAMS   = 5;
const MAX_REQUIRED_PARAMS = 20;

// ─────────────────────────────────────────────────────────────────────────────
// ROUTING — isCandidateQuestion
// ─────────────────────────────────────────────────────────────────────────────
//
// Detects whether a user question is about candidate data.
// If YES → answer from in-memory cache (zero DB calls).
// If NO  → fall through to SP selection mode.
//
// Detection uses two signals:
//   1. CANDIDATE_KEYWORDS — words that indicate candidate data intent
//   2. SP_BYPASS_KEYWORDS — words that indicate a non-candidate SP query
//
// Deliberately broad — it is better to route to cache and get a fast answer
// than to route to SP selection and miss.
//
// EXAMPLES:
//   "how many marks did Naresh get"         → cache ✅
//   "show me all candidates from Ahmedabad" → cache ✅
//   "who is shortlisted for React Dev job"  → cache ✅
//   "list all open job positions"           → SP mode ✅
//   "show me the job with highest salary"   → SP mode ✅

const CANDIDATE_KEYWORDS = [
  "candidate", "candidates", "applicant", "applicants",
  "marks", "score", "percentage", "interview", "interviews",
  "shortlisted", "selected", "rejected", "applied",
  "resume", "profile", "skill", "skills", "experience",
  "education", "degree", "university", "college",
  "feedback", "qualified", "pass", "fail", "result",
  "stage", "pipeline", "hired", "offer",
  "verified", "registered", "document", "aadhar", "pan",
  "naresh", "rahul", "priya",  // common names — extend as needed
];

// These indicate the user wants non-candidate SP data even if candidate
// keywords are present (e.g. "list all jobs for candidates to apply to")
const SP_BYPASS_KEYWORDS = [
  "open positions", "available jobs", "job listings",
  "create job", "post a job", "all workflows",
  "question bank", "question paper", "add role",
];

export function isCandidateQuestion(query) {
  const q = query.toLowerCase();

  // If bypass keyword is present → NOT a candidate question
  if (SP_BYPASS_KEYWORDS.some((kw) => q.includes(kw))) return false;

  // If any candidate keyword present → IS a candidate question
  return CANDIDATE_KEYWORDS.some((kw) => q.includes(kw));
}

// ─────────────────────────────────────────────────────────────────────────────
// STARTUP — call this from your server entry point (index.js / app.js)
// ─────────────────────────────────────────────────────────────────────────────
//
// Usage in your app startup:
//   import { initializeServices } from "./services/sp.service.js";
//   await initializeServices();
//
// This fires the SP once, loads all candidate data into memory,
// and pre-builds all column embeddings — ready before first request.

export async function initializeServices() {
  log.info("🚀 Initializing SP services...");

  // Fire SP once — loads all candidate data into in-memory cache
  await warmUp();

  // Pre-build SP embeddings for non-candidate SP routing
  await buildSpEmbeddings();

  log.info("✅ All services initialized");
}

// ─────────────────────────────────────────────────────────────────────────────
// PRIMARY ENTRY POINT — handleQuestion
// ─────────────────────────────────────────────────────────────────────────────
//
// Call this from your chat controller instead of runSpMode() directly.
//
//   const result = await handleQuestion(userMessage);
//   res.json({ answer: result.answer, data: result.dataframe });

export async function handleQuestion(question) {
  log.info(`\n${"─".repeat(60)}`);
  log.info(`Question: "${question}"`);

  // Route to cache mode for candidate questions
  if (isCandidateQuestion(question)) {
    log.info("→ Route: CACHE MODE (candidate data)");

    const cacheResult = await answerFromCache(question);

    if (cacheResult.success) {
      log.info(`✅ Cache answered — ${cacheResult.rowsScanned} rows scanned`);
      return cacheResult;
    }

    // Cache had no relevant columns — could be an edge case, fall through to SP
    if (cacheResult.reason === "no_relevant_columns") {
      log.info("Cache found no relevant columns — falling through to SP mode");
    } else {
      // Cache returned a graceful "not found" answer — return it
      return cacheResult;
    }
  }

  // Fall through to SP mode for non-candidate questions
  log.info("→ Route: SP MODE");
  return runSpMode(question);
}

// ─────────────────────────────────────────────────────────────────────────────
// SP MODE — unchanged core logic, now only handles non-candidate SPs
// ─────────────────────────────────────────────────────────────────────────────

// ─── 1. Discover all SPs from MSSQL ──────────────────────────────────────────
export async function discoverStoredProcedures() {
  if (spMetadataCache) {
    log.info(`SP cache hit — ${spMetadataCache.length} SPs loaded`);
    return spMetadataCache;
  }

  const pool = await getPool();

  const spResult = await pool.request().query(`
    SELECT ROUTINE_NAME
    FROM INFORMATION_SCHEMA.ROUTINES
    WHERE ROUTINE_TYPE = 'PROCEDURE'
      AND ROUTINE_SCHEMA = 'dbo'
    ORDER BY ROUTINE_NAME
  `);

  const spNames = spResult.recordset.map((r) => r.ROUTINE_NAME);

  if (spNames.length === 0) {
    log.warn("No stored procedures found in database.");
    spMetadataCache = [];
    return spMetadataCache;
  }

  const metadata = [];

  for (const name of spNames) {
    // Skip the candidate SP — it is handled entirely by the cache layer
    if (name === "sp_GetAllCandidateDetails") {
      log.info(`  Skipping ${name} — handled by candidate cache`);
      continue;
    }

    const paramResult = await pool.request().query(`
      SELECT
        PARAMETER_NAME,
        DATA_TYPE,
        PARAMETER_MODE,
        ORDINAL_POSITION
      FROM INFORMATION_SCHEMA.PARAMETERS
      WHERE SPECIFIC_NAME = '${name}'
        AND PARAMETER_NAME <> ''
      ORDER BY ORDINAL_POSITION
    `);

    const params = paramResult.recordset.map((p) => {
      const rawName = p.PARAMETER_NAME.replace(/^@/, "");
      const rawMode = (p.PARAMETER_MODE || "IN").toUpperCase();

      const nameUpper = rawName.toUpperCase();
      const isOutputByName = (
        nameUpper === "RESULTCODE"    ||
        nameUpper === "RESULTMESSAGE" ||
        nameUpper === "TOTALCOUNT"    ||
        nameUpper === "OUTPUTMESSAGE" ||
        nameUpper === "ERRORMESSAGE"  ||
        nameUpper === "ERRORCODE"
      );
      const isOutput = rawMode === "OUT" || rawMode === "INOUT" || isOutputByName;

      return {
        name:     rawName,
        type:     p.DATA_TYPE,
        mode:     isOutput ? "OUT" : "IN",
        position: p.ORDINAL_POSITION,
      };
    });

    const { requiredParams, optionalParams } = classifySpParams(params);
    metadata.push({ name, params, requiredParams, optionalParams });
  }

  spMetadataCache = metadata;
  log.info(`SP discovery complete — ${metadata.length} SPs found`);
  return metadata;
}

// ─── 2. Classify params ───────────────────────────────────────────────────────
const REQUIRED_KEYWORDS = [
  "id", "code", "date", "type", "status", "state", "flag",
  "from", "to", "start", "end", "filter", "search", "name",
  "category", "region", "district", "year", "month",
];

export function classifySpParams(params) {
  const required = [];
  const optional = [];

  for (const p of params) {
    const lname = p.name.toLowerCase();
    const isKeyword    = REQUIRED_KEYWORDS.some((kw) => lname.includes(kw));
    const isEarlyPos   = p.position <= MAX_REQUIRED_PARAMS;
    if (isKeyword || isEarlyPos) required.push(p);
    else optional.push(p);
  }

  const cappedRequired = required.slice(0, MAX_REQUIRED_PARAMS);
  const overflow       = required.slice(MAX_REQUIRED_PARAMS);

  return {
    requiredParams: cappedRequired,
    optionalParams: [...optional, ...overflow],
  };
}

// ─── 3. Generate SP description ───────────────────────────────────────────────
async function generateSpDescription(spName, params) {
  const hint = SP_METADATA_HINTS[spName];
  if (hint) {
    if (Array.isArray(hint.sentences) && hint.sentences.length > 0) return hint.sentences[0];
    if (typeof hint === "string" && hint.trim()) return hint.trim();
  }

  const client = getOpenAIClient();
  let sampledParams = params;
  let truncationNote = "";

  if (params.length > DESC_HEAD_PARAMS + DESC_TAIL_PARAMS) {
    const head = params.slice(0, DESC_HEAD_PARAMS);
    const tail = params.slice(-DESC_TAIL_PARAMS);
    const seen = new Set(head.map((p) => p.name));
    const uniqueTail = tail.filter((p) => !seen.has(p.name));
    sampledParams = [...head, ...uniqueTail];
    truncationNote = ` (${params.length} params total, showing ${sampledParams.length})`;
  }

  const paramList =
    sampledParams.length > 0
      ? sampledParams.map((p) => `@${p.name} (${p.type})`).join(", ") + truncationNote
      : "no parameters";

  const prompt = `
You are a database analyst. Given a stored procedure name and its parameters,
write a single concise sentence (max 20 words) describing what this SP likely does.
Stored Procedure: ${spName}
Parameters (sample): ${paramList}
Respond with ONLY the description sentence.`.trim();

  const resp = await client.chat.completions.create({
    model:       "gpt-4o-mini",
    messages:    [{ role: "user", content: prompt }],
    max_tokens:  60,
    temperature: 0,
  });

  return (resp.choices[0].message.content || "").trim();
}

// ─── 4. Expand hint to sentences ──────────────────────────────────────────────
async function expandHintToSentences(spName, hint) {
  const client = getOpenAIClient();

  const prompt = `
You are a search query expert.
Given a stored procedure name and a description hint, generate exactly 5 natural
English sentences that represent different ways a user might ask for this data.

SP Name: ${spName}
Hint: ${hint}

Rules:
- Write exactly 5 sentences, one per line
- Each sentence must be a complete, natural question or request
- Vary the phrasing: some as questions, some as commands
- Use simple everyday language
- Do NOT number them, do NOT use bullet points

Output (5 lines):`.trim();

  try {
    const resp = await client.chat.completions.create({
      model:       "gpt-4o-mini",
      messages:    [{ role: "user", content: prompt }],
      max_tokens:  200,
      temperature: 0.3,
    });

    const raw = (resp.choices[0].message.content || "").trim();
    const sentences = raw
      .split("\n")
      .map((s) => s.replace(/^\d+[\.\)]\s*/, "").trim())
      .filter((s) => s.length > 5);

    if (!sentences.includes(hint)) sentences.push(hint);
    return sentences.slice(0, 6);
  } catch {
    return [hint];
  }
}

// ─── 5. Build SP embeddings ───────────────────────────────────────────────────
export async function buildSpEmbeddings() {
  if (spEmbeddingCache) {
    log.info("SP embedding cache hit — skipping rebuild");
    return spEmbeddingCache;
  }

  const metadata = await discoverStoredProcedures();

  if (metadata.length === 0) {
    spEmbeddingCache = [];
    return spEmbeddingCache;
  }

  log.info(`Building embeddings for ${metadata.length} SPs...`);
  const enriched = [];

  for (const sp of metadata) {
    try {
      const hint = SP_METADATA_HINTS[sp.name];
      let sentences, description, defaults = {};

      if (hint && Array.isArray(hint.sentences) && hint.sentences.length > 0) {
        sentences   = hint.sentences;
        description = sentences[0];
        defaults    = hint.defaults || {};
      } else {
        description = await generateSpDescription(sp.name, sp.params);
        sentences   = await expandHintToSentences(sp.name, description);
      }

      const embeddings = await Promise.all(sentences.map((s) => embedText(s)));
      enriched.push({ ...sp, description, sentences, embeddings, defaults });
      log.info(`  ✓ ${sp.name} → ${sentences.length} embeddings`);
    } catch (err) {
      log.warn(`  ✗ Failed to embed SP "${sp.name}": ${err.message}`);
    }
  }

  spEmbeddingCache = enriched;
  log.info(`SP embedding build complete — ${enriched.length} SPs embedded`);
  return spEmbeddingCache;
}

// ─── 6. Query expansion ───────────────────────────────────────────────────────
const CASUAL_PREFIXES = /^(show\s+me|show|list|get|find|fetch|give\s+me|what\s+are|who\s+are|display|tell\s+me)/i;

const TYPO_MAP = {
  " form ": " from ",
  " fom ":  " from ",
  " cnadidate": " candidate",
  " condiate":  " candidate",
  " candidte":  " candidate",
  " joob ": " job ",
  " jbo ":  " job ",
};

const DATA_KEYWORDS = [
  "job", "jobs", "candidate", "candidates", "user", "users",
  "profile", "application", "applications", "interview", "interviews",
  "skill", "skills", "resume", "salary", "hiring", "recruitment",
];

function expandUserQuery(userQuery) {
  let q = userQuery.trim().toLowerCase();
  for (const [typo, fix] of Object.entries(TYPO_MAP)) {
    q = q.split(typo).join(fix);
  }
  const hasDataKeyword = DATA_KEYWORDS.some((kw) => q.includes(kw));
  if (!hasDataKeyword) return q;
  if (/^(retrieve|fetch|get all|list all)/.test(q)) return q;
  const stripped = q.replace(CASUAL_PREFIXES, "").trim();
  return stripped ? `retrieve and list ${stripped}` : q;
}

// ─── 7. Cosine similarity ─────────────────────────────────────────────────────
function cosineSimilarity(a, b) {
  let dot = 0, magA = 0, magB = 0;
  for (let i = 0; i < a.length; i++) {
    dot  += a[i] * b[i];
    magA += a[i] * a[i];
    magB += b[i] * b[i];
  }
  if (magA === 0 || magB === 0) return 0;
  return dot / (Math.sqrt(magA) * Math.sqrt(magB));
}

// ─── 8. Select best SP ────────────────────────────────────────────────────────
export async function selectBestSp(userQuery) {
  const spEmbeddings = await buildSpEmbeddings();

  if (spEmbeddings.length === 0) {
    return { sp: null, score: 0, candidates: [] };
  }

  const expandedQuery = expandUserQuery(userQuery);
  log.info(`Query expansion: "${userQuery}" → "${expandedQuery}"`);

  const [queryEmbedding, originalEmbedding] = await Promise.all([
    embedText(expandedQuery),
    embedText(userQuery),
  ]);

  const scored = spEmbeddings.map((sp) => {
    const embeddings = sp.embeddings || [sp.embedding];
    let bestScore = 0;
    for (const emb of embeddings) {
      const s1 = cosineSimilarity(queryEmbedding, emb);
      const s2 = cosineSimilarity(originalEmbedding, emb);
      bestScore = Math.max(bestScore, s1, s2);
    }
    return { ...sp, score: bestScore };
  });

  scored.sort((a, b) => b.score - a.score);
  const top  = scored.slice(0, MAX_SP_CANDIDATES);
  const best = top[0];

  log.info(`SP selection — best: "${best.name}" score: ${best.score.toFixed(4)}`);

  if (best.score < SP_CONFIDENCE_THRESHOLD) {
    log.info("SP confidence below threshold → fallback");
    return { sp: null, score: best.score, candidates: top };
  }

  return { sp: best, score: best.score, candidates: top };
}

// ─── 9. Extract SP params ─────────────────────────────────────────────────────
export async function extractSpParams(userQuery, sp) {
  const client = getOpenAIClient();

  const allRequired   = sp.requiredParams || sp.params.slice(0, MAX_REQUIRED_PARAMS);
  const paramsForLlm  = allRequired.filter((p) => p.mode !== "OUT");

  if (paramsForLlm.length === 0) {
    log.info("SP has no INPUT params — skipping LLM");
    return {};
  }

  const paramList = paramsForLlm
    .map((p) => {
      const hint = PARAM_HINTS[p.name] || "";
      return hint ? `${p.name} (${p.type}) — ${hint}` : `${p.name} (${p.type})`;
    })
    .join("\n  ");

  const prompt = `
You are a parameter extraction assistant for a recruitment chatbot.
Extract parameter values for the stored procedure from the user query.

Stored Procedure: ${sp.name}
Description: ${sp.description}

Parameters to fill:
  ${paramList}

User query: "${userQuery}"

Rules:
- Extract ONLY parameters clearly mentioned or implied in the query.
- Set any parameter NOT mentioned in the query to null.
- For boolean params: use 1 for yes/true, 0 for no/false.
- Return ONLY a valid JSON object — no markdown, no explanation.
- Format: { "paramName": value_or_null, ... }

JSON output:`.trim();

  const resp = await client.chat.completions.create({
    model:       "gpt-4o-mini",
    messages:    [{ role: "user", content: prompt }],
    max_tokens:  400,
    temperature: 0,
  });

  let raw = (resp.choices[0].message.content || "").trim();
  raw = raw.replace(/^```[a-z]*\n?/, "").replace(/\n?```$/, "");

  let extracted = {};
  try {
    extracted = JSON.parse(raw);
  } catch {
    log.warn("SP param extraction JSON parse failed — using empty params");
  }

  // Inject SP-level defaults
  const defaults = sp.defaults || {};
  for (const [key, defaultVal] of Object.entries(defaults)) {
    if (extracted[key] === null || extracted[key] === undefined) {
      extracted[key] = defaultVal;
      log.info(`  Injected default for @${key}: ${defaultVal}`);
    }
  }

  return extracted;
}

// ─── 10. Execute SP ───────────────────────────────────────────────────────────
export async function executeStoredProcedure(spName, extractedParams, spMeta) {
  const knownSps = (spMetadataCache || []).map((s) => s.name.toLowerCase());
  if (!knownSps.includes(spName.toLowerCase())) {
    throw new Error(`SP "${spName}" is not in the allowed list.`);
  }

  const pool    = await getPool();
  const request = pool.request();
  const sql     = await import("mssql");

  const typeMap = {
    int:              sql.default.Int,
    bigint:           sql.default.BigInt,
    smallint:         sql.default.SmallInt,
    tinyint:          sql.default.TinyInt,
    bit:              sql.default.Bit,
    float:            sql.default.Float,
    real:             sql.default.Real,
    decimal:          sql.default.Decimal,
    numeric:          sql.default.Numeric,
    money:            sql.default.Money,
    smallmoney:       sql.default.SmallMoney,
    varchar:          sql.default.VarChar,
    nvarchar:         sql.default.NVarChar,
    char:             sql.default.Char,
    nchar:            sql.default.NChar,
    text:             sql.default.Text,
    ntext:            sql.default.NText,
    date:             sql.default.Date,
    datetime:         sql.default.DateTime,
    datetime2:        sql.default.DateTime2,
    smalldatetime:    sql.default.SmallDateTime,
    time:             sql.default.Time,
    uniqueidentifier: sql.default.UniqueIdentifier,
    xml:              sql.default.Xml,
    varbinary:        sql.default.VarBinary,
    binary:           sql.default.Binary,
    image:            sql.default.Image,
  };

  const inputParams  = spMeta.params.filter((p) => p.mode !== "OUT");
  const outputParams = spMeta.params.filter((p) => p.mode === "OUT");

  for (const paramMeta of inputParams) {
    const sqlType    = typeMap[paramMeta.type.toLowerCase()] || sql.default.NVarChar;
    const paramValue = Object.prototype.hasOwnProperty.call(extractedParams, paramMeta.name)
      ? extractedParams[paramMeta.name]
      : null;
    request.input(paramMeta.name, sqlType, paramValue);
  }

  for (const paramMeta of outputParams) {
    const sqlType = typeMap[paramMeta.type.toLowerCase()] || sql.default.NVarChar;
    request.output(paramMeta.name, sqlType);
  }

  log.info(`Executing SP "${spName}" — ${inputParams.length} INPUT, ${outputParams.length} OUTPUT`);

  const result = await request.execute(spName);

  if (outputParams.length > 0 && result.output) {
    log.info(`SP output params: ${JSON.stringify(result.output)}`);
    const resultCode = result.output["ResultCode"] ?? result.output["resultCode"];
    if (resultCode !== undefined && resultCode < 0) {
      const msg = result.output["ResultMessage"] ?? "SP returned error code";
      throw new Error(`SP error (code ${resultCode}): ${msg}`);
    }
  }

  if (result.recordsets && result.recordsets.length > 1) {
    const merged = result.recordsets.filter((rs) => rs && rs.length > 0).flat();
    log.info(`SP returned ${result.recordsets.length} recordsets → merged ${merged.length} rows`);
    return merged;
  }

  return result.recordset || result.recordsets?.[0] || [];
}

// ─── 11. runSpMode — SP flow (non-candidate questions only) ───────────────────
export async function runSpMode(question) {
  const { sp, score, candidates } = await selectBestSp(question);

  if (!sp) {
    return { success: false, reason: "low_confidence", score };
  }

  log.info(`Running SP: ${sp.name}`);

  let extractedParams = {};
  try {
    extractedParams = await extractSpParams(question, sp);
    log.info(`Extracted params: ${JSON.stringify(extractedParams)}`);
  } catch (err) {
    log.warn(`Param extraction failed: ${err.message}`);
  }

  let rows = [];
  try {
    rows = await executeStoredProcedure(sp.name, extractedParams, sp);
    log.info(`SP "${sp.name}" returned ${rows.length} row(s)`);
  } catch (err) {
    log.error(`SP execution failed: ${err.message}`);
    return { success: false, reason: "sp_execution_error", error: err.message };
  }

  const answer = await generateNaturalAnswer(question, `EXEC ${sp.name}`, rows);

  return {
    success:   true,
    type:      "SP",
    answer,
    dataframe: rows,
    sql:       `EXEC ${sp.name}`,
    spName:    sp.name,
    score,
  };
}

// ─── 12. Cache invalidation ───────────────────────────────────────────────────
export function clearSpCache() {
  spMetadataCache  = null;
  spEmbeddingCache = null;
  log.info("SP discovery cache cleared.");
}