import { getOpenAIClient, generateNaturalAnswer } from "./ai.service.js";
import { getPool } from "../db/connection.js";
import { log } from "../utils/logger.js";
import { MAX_HISTORY } from "../config/constants.js";

let DB_SCHEMA = "";

export function setDbSchema(schema) {
  DB_SCHEMA = schema;
}

export function getDbSchema() {
  return DB_SCHEMA;
}

export async function runSqlMode(question, conversationHistory) {
  const client = getOpenAIClient();

  const SQL_SYSTEM = `
  You are an expert MSSQL (Microsoft SQL Server) analyst for an agricultural-finance database.
  The full database schema (all tables, columns, types, keys, and sample rows)
  is provided below.

  Your job:
  1. Write a single, valid T-SQL query that answers the user's question.
  2. Use JOINs whenever the answer requires data from multiple tables.
  3. Return ONLY a JSON object — no markdown, no explanation — in this exact shape:
    {"sql": "<your SQL here>", "explanation": "<one sentence>"}

  Rules:
  - ONLY generate SELECT queries. Never generate INSERT, UPDATE, DELETE, DROP,
    TRUNCATE, ALTER, CREATE, EXEC, EXECUTE, MERGE, or any other data-modifying
    or schema-changing statements under any circumstances.
  - Always wrap table names and column names in square brackets (e.g. [user], [order], [table])
    to avoid reserved-word conflicts in T-SQL.
  - If the user asks to delete, update, insert, or modify data in any way,
    refuse and reply: "I can only read data. I cannot modify, delete, or insert records."
  - Use only tables and columns that exist in the schema below.
  - Never use LIMIT — use TOP instead (e.g. SELECT TOP 10 * FROM table).
  - NEVER select password, otp, otpExpiry, or any column containing tokens or secrets.
  - Always qualify column names with their table name or alias when joining
    (e.g. t1.column_name) to avoid ambiguity errors.
  - String comparisons are case-sensitive; match sample values exactly.
  - Do not use semi-colons inside the JSON string value.
  - Prefer INNER JOIN unless an outer join is clearly needed.
  - For aggregation across joined tables, always GROUP BY the appropriate key.

  DATABASE SCHEMA:
  {schema}
  `.trim();

  const systemPrompt = SQL_SYSTEM.replace("{schema}", DB_SCHEMA);

  const messages = [
    { role: "system", content: systemPrompt },
    ...conversationHistory.slice(-MAX_HISTORY),
    { role: "user", content: question },
  ];

  const resp = await client.chat.completions.create({
    model: "gpt-4o-mini",
    messages,
    temperature: 0,
  });

  let raw = (resp.choices[0].message.content || "").trim();
  raw = raw.replace(/^```[a-z]*\n?/, "").replace(/\n?```$/, "");

  let sqlQuery = "";
  let explanation = "";

  try {
    const parsed = JSON.parse(raw);
    sqlQuery = (parsed.sql || "").trim().replace(/;$/, "");
    explanation = parsed.explanation || "";
  } catch {
    sqlQuery = raw.trim().replace(/;$/, "");
  }

  if (!sqlQuery.toLowerCase().includes("top")) {
    sqlQuery = sqlQuery.replace("SELECT", "SELECT TOP 50");
  }

  const BLOCKED_KEYWORDS =
    /^\s*(INSERT|UPDATE|DELETE|DROP|TRUNCATE|ALTER|CREATE|EXEC|EXECUTE|MERGE)\b/i;
  if (BLOCKED_KEYWORDS.test(sqlQuery)) {
    log.warn("Blocked destructive SQL query:", sqlQuery);
    return {
      type: "SQL",
      answer:
        "I can only read data. I cannot modify, delete, or insert records.",
      dataframe: null,
      sql: sqlQuery,
    };
  }

  try {
    const pool = await getPool();
    const result = await pool.request().query(sqlQuery);
    const resultRows = result.recordset || [];

    const naturalAnswer = await generateNaturalAnswer(
      question,
      sqlQuery,
      resultRows,
    );
    return {
      type: "SQL",
      answer: naturalAnswer,
      dataframe: resultRows,
      sql: sqlQuery,
    };
  } catch (err) {
    log.error("SQL execution error:", err.message);
    return {
      type: "SQL",
      answer: `SQL error: ${err.message}\n\nGenerated SQL:\n${sqlQuery}`,
      dataframe: null,
      sql: sqlQuery,
    };
  }
}
