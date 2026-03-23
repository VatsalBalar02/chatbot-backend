import fs from "fs";
import { LocalIndex } from "vectra";
import { getOpenAIClient } from "./ai.service.js";
import { embedText } from "../utils/embedding.js";
import { log } from "../utils/logger.js";
import { MAX_HISTORY } from "../config/constants.js";

const PDF_SYSTEM = `
  You are an expert on recruitment processes, hiring guidelines, and HR policies.

  Answer the user's question using ONLY the context passages provided below from the Recruitment Guide PDF.

  Guidelines:
  - Only use the given context — do NOT make up information
  - If the answer is not found, say: "This information is not available in the document."
  - Be clear, concise, and professional
  - Use bullet points when helpful
  - Focus on HR-related topics such as:
    - Hiring process
    - Interview stages
    - Candidate evaluation
    - Recruitment policies
    - Roles and responsibilities
    - Selection criteria

  CONTEXT:
  {context}
  `.trim();

export async function loadPdfVectorstore(pdfPath, vectraDir) {
  if (!fs.existsSync(pdfPath)) {
    log.warn(`PDF not found (${pdfPath}) — PDF mode disabled.`);
    return null;
  }

  const index = new LocalIndex(vectraDir);

  if (await index.isIndexCreated()) {
    const items = await index.listItems();

    if (items.length > 0) {
      log.info(`Loading existing Vectra index from ${vectraDir}`);
      return index;
    }

    log.warn("Index exists but empty. Rebuilding...");
    await index.deleteIndex();
  }

  log.info(`Building Vectra index from PDF: ${pdfPath}`);
  await index.createIndex();

  // Lazy import — avoids pdf-parse test file bug on startup
  const pdfjsLib = (await import("pdfjs-dist/legacy/build/pdf.js")).default;

  async function extractPdfText(pdfPath) {
    const data = new Uint8Array(fs.readFileSync(pdfPath));
    const pdf = await pdfjsLib.getDocument({ data }).promise;

    let text = "";

    for (let i = 1; i <= pdf.numPages; i++) {
      const page = await pdf.getPage(i);
      const content = await page.getTextContent();

      const strings = content.items.map((item) => item.str.trim());

      let cleanLine = strings.join(" ");

      // aggressive cleaning
      cleanLine = cleanLine
        .replace(/\s+/g, " ")
        .replace(/[^\x00-\x7F]/g, "") // remove weird symbols
        // .replace(//g, "")
        .trim();

      text += cleanLine + "\n";
    }

    return text;
  }

  const fullText = await extractPdfText(pdfPath);

  if (!fullText || fullText.trim().length === 0) {
    throw new Error("PDF text extraction failed (empty content)");
  }

  // ── Improved chunking strategy ────────────────────────────────────────────
  // Old strategy: fixed 800-char sliding window — splits topic sections mid-sentence.
  // "Pre-Boarding" content was split across 3 chunks → each chunk scored 0.42
  // individually but none passed the 0.5 threshold.
  //
  // New strategy: paragraph-based chunking with overlap.
  // Paragraphs keep related sentences together → higher per-chunk relevance scores.
  // Overlap ensures no sentence is lost at a chunk boundary.

  // Chunking strategy:
  // extractPdfText() produces one line per page separated by \n.
  // We split on single \n to get page-level segments, then group them
  // into 600-char chunks with 1-sentence overlap between chunks.
  // This keeps topic sections (like "Pre-Boarding") together in one chunk
  // instead of being split across multiple small fragments.

  const lines = fullText
    .split("\n")
    .map((l) => l.trim())
    .filter((l) => l.length > 30); // drop very short lines/headers

  const chunks = [];
  const TARGET_CHUNK_SIZE = 600;
  let current = "";
  let lastSentence = "";

  for (const line of lines) {
    // Start new chunk carrying overlap sentence from previous chunk
    if (current === "" && lastSentence) {
      current = lastSentence + " ";
    }

    if ((current + " " + line).length <= TARGET_CHUNK_SIZE) {
      current += (current.trim() ? " " : "") + line;
    } else {
      if (current.trim().length > 30) {
        chunks.push(current.trim());
        // Carry last sentence into next chunk for context continuity
        const parts = current.trim().split(". ");
        lastSentence = parts.length > 1 ? parts[parts.length - 1] : "";
      }
      current = line;
    }
  }
  if (current.trim().length > 30) chunks.push(current.trim());

  log.info(`PDF chunking complete — ${chunks.length} chunks from ${lines.length} lines`);

  log.info(
    `Embedding ${chunks.length} chunks — this may take a minute on first run...`,
  );
  for (const chunk of chunks) {
    log.info(`Embedding chunk: ${chunk}`);
    const vector = await embedText(chunk);
    await index.insertItem({
      vector,
      metadata: { text: chunk },
    });
  }

  log.info(`Vectra index built — ${chunks.length} chunks indexed`);
  return index;
}

export async function runPdfMode(question, conversationHistory, vectorstore) {
  const client = getOpenAIClient();

  if (!vectorstore) {
    return {
      type: "PDF",
      answer:
        "PDF knowledge base is not available (file not found at startup).",
      dataframe: null,
      sql: "",
    };
  }

  const queryVector = await embedText(question);

  // Fetch top 5 chunks — more candidates = better chance of finding the right passage.
  // Your PDF scores peak at ~0.43 for specific topics — fetching more gives
  // the LLM enough context to piece together a complete answer.
  const results = await vectorstore.queryItems(queryVector, 5);

  log.info(`PDF retrieval — top scores: ${results.map(r => r.score.toFixed(3)).join(", ")}`);

  // Threshold lowered from 0.5 → 0.20
  // Your PDF's best score for "Pre-Boarding" is 0.429 — the old 0.5 threshold
  // rejected ALL results even though the answer was clearly in result #2.
  // 0.20 is safe — it still blocks completely unrelated chunks (scored < 0.15)
  // while accepting relevant but loosely-worded matches.
  // The LLM prompt already instructs: "only answer from context — say not found if unsure"
  // so hallucination risk from lower threshold is minimal.
  const RAG_SCORE_THRESHOLD = 0.20;
  const filtered = results.filter((r) => r.score > RAG_SCORE_THRESHOLD);

  log.info(`PDF chunks after threshold (>${RAG_SCORE_THRESHOLD}): ${filtered.length}/${results.length}`);

  if (filtered.length === 0) {
    return {
      type: "PDF",
      answer: "I couldn't find relevant information in the document.",
      dataframe: null,
      sql: "",
    };
  }

  // Sort by score descending — best chunks first for LLM context window
  filtered.sort((a, b) => b.score - a.score);

  const context = filtered.map((r) => r.item.metadata.text).join("\n\n---\n\n");

  const messages = [
    { role: "system", content: PDF_SYSTEM.replace("{context}", context) },
    ...conversationHistory.slice(-MAX_HISTORY),
    { role: "user", content: question },
  ];

  const resp = await client.chat.completions.create({
    model: "gpt-4o-mini",
    messages,
    temperature: 0.2,
  });
  const answer = (resp.choices[0].message.content || "").trim();
  return { type: "PDF", answer, dataframe: null, sql: "" };
}