import fs from "fs";
import { LocalIndex } from "vectra";
import { getOpenAIClient } from "./ai.service.js";
import { embedText } from "../utils/embedding.js";
import { log } from "../utils/logger.js";
import { MAX_HISTORY } from "../config/constants.js";

const PDF_SYSTEM = `
You are an expert HR assistant for a Recruitment Management System.

Your job is to answer questions about recruitment processes, HR policies, hiring guidelines, and related topics.

You are given context passages extracted from the company's Recruitment Guide PDF.

INSTRUCTIONS:
1. Answer using the provided context as your PRIMARY source.
2. If the context contains RELATED information (even if not an exact match), use it to give a helpful answer. For example, if asked about "Recruitment Cycle" and the context discusses recruitment stages/process, explain it using that content.
3. If the context is partially relevant, answer what you can and note what's not covered.
4. Only say "This information is not available in the document" if the context has NO relevance at all to the question.
5. Be clear, concise, and professional.
6. Use bullet points or numbered steps when explaining processes.
7. Never make up specific company policies — only use what the context provides.

CONTEXT FROM RECRUITMENT GUIDE:
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

      cleanLine = cleanLine
        .replace(/\s+/g, " ")
        .replace(/[^\x00-\x7F]/g, "")
        .trim();

      text += cleanLine + "\n";
    }

    return text;
  }

  const fullText = await extractPdfText(pdfPath);

  if (!fullText || fullText.trim().length === 0) {
    throw new Error("PDF text extraction failed (empty content)");
  }

  const lines = fullText
    .split("\n")
    .map((l) => l.trim())
    .filter((l) => l.length > 30);

  const chunks = [];
  const TARGET_CHUNK_SIZE = 600;
  let current = "";
  let lastSentence = "";

  for (const line of lines) {
    if (current === "" && lastSentence) {
      current = lastSentence + " ";
    }

    if ((current + " " + line).length <= TARGET_CHUNK_SIZE) {
      current += (current.trim() ? " " : "") + line;
    } else {
      if (current.trim().length > 30) {
        chunks.push(current.trim());
        const parts = current.trim().split(". ");
        lastSentence = parts.length > 1 ? parts[parts.length - 1] : "";
      }
      current = line;
    }
  }
  if (current.trim().length > 30) chunks.push(current.trim());

  log.info(`PDF chunking complete — ${chunks.length} chunks from ${lines.length} lines`);

  log.info(`Embedding ${chunks.length} chunks — this may take a minute on first run...`);
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
      answer: "PDF knowledge base is not available (file not found at startup).",
      dataframe: null,
      sql: "",
    };
  }

  const queryVector = await embedText(question);

  // Fetch top 6 chunks for better coverage
  const results = await vectorstore.queryItems(queryVector, 6);

  log.info(`PDF retrieval — top scores: ${results.map((r) => r.score.toFixed(3)).join(", ")}`);

  const RAG_SCORE_THRESHOLD = 0.20;
  const filtered = results.filter((r) => r.score > RAG_SCORE_THRESHOLD);

  log.info(`PDF chunks after threshold (>${RAG_SCORE_THRESHOLD}): ${filtered.length}/${results.length}`);

  if (filtered.length === 0) {
    return {
      type: "PDF",
      answer:
        "I couldn't find relevant information in the document for your question. " +
        "Try rephrasing, or ask about specific topics like the hiring process, " +
        "interview stages, onboarding, or selection criteria.",
      dataframe: null,
      sql: "",
    };
  }

  // Sort by score descending — best chunks first
  filtered.sort((a, b) => b.score - a.score);

  const context = filtered
    .map((r, i) => `[Passage ${i + 1}]\n${r.item.metadata.text}`)
    .join("\n\n---\n\n");

  // Build a query-aware system prompt — tells the LLM what was asked
  // so it can make better use of loosely-related context
  const systemWithContext = PDF_SYSTEM.replace("{context}", context);

  const messages = [
    { role: "system", content: systemWithContext },
    ...conversationHistory.slice(-MAX_HISTORY),
    { role: "user", content: question },
  ];

  const resp = await client.chat.completions.create({
    model: "gpt-4o-mini",
    messages,
    temperature: 0.3,
  });

  const answer = (resp.choices[0].message.content || "").trim();
  return { type: "PDF", answer, dataframe: null, sql: "" };
}