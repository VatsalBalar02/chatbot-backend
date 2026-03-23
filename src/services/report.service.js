// src/services/report.service.js

import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import PDFDocument from "pdfkit";
import { runSqlMode } from "./sql.services.js";
import { cleanForPdf } from "../utils/formatter.js";
import { REPORTS_DIR } from "../config/constants.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
fs.mkdirSync(REPORTS_DIR, { recursive: true });

// ─── Design Tokens ────────────────────────────────────────────────────────────
const NAVY = "#1E3A5F";
const BLUE = "#2E86C1";
const LTBLUE = "#D6EAF8";
const MINT = "#1ABC9C";
const WHITE = "#FFFFFF";
const OFFWHITE = "#F7F9FC";
const BORDER = "#BDC3C7";
const DARK = "#1C2833";
const MUTED = "#717D7E";

const PW = 595.28;
const PH = 841.89;
const M = 45;
const CW = PW - M * 2;

// ─── Helpers ──────────────────────────────────────────────────────────────────
const clean = (val, max = 40) => cleanForPdf(String(val ?? "")).slice(0, max);

const stripMd = (t = "") =>
  t
    .replace(/\*\*(.*?)\*\*/g, "$1")
    .replace(/\*(.*?)\*/g, "$1")
    .replace(/`(.*?)`/g, "$1")
    .replace(/#{1,6}\s/g, "")
    .replace(/\|.*\|/g, "")
    .replace(/---+/g, "")
    .replace(/\n{3,}/g, "\n\n")
    .trim();

const hline = (doc, x1, y, x2, color = BORDER, w = 0.5) =>
  doc.moveTo(x1, y).lineTo(x2, y).strokeColor(color).lineWidth(w).stroke();

const rect = (doc, x, y, w, h, fill, stroke, sw = 0.5) => {
  doc.rect(x, y, w, h).fill(fill);
  if (stroke) doc.rect(x, y, w, h).strokeColor(stroke).lineWidth(sw).stroke();
};

// ─── Skip list for chart columns ──────────────────────────────────────────────
const SKIP = [
  "id",
  "phone",
  "phonenumber",
  "mobile",
  "contact",
  "zip",
  "pincode",
  "postal",
  "pan",
  "aadhar",
  "aadhaar",
  "passport",
  "year",
  "month",
  "day",
  "date",
  "time",
  "code",
  "otp",
  "pin",
  "gender",
  "email",
  "name",
  "firstname",
  "lastname",
  "city",
  "state",
  "address",
  "status",
  "type",
  "role",
  "description",
  "url",
  "uuid",
];

function isChartable(col, vals) {
  const k = col.toLowerCase().replace(/[^a-z]/g, "");
  if (SKIP.some((s) => k.includes(s))) return false;
  if (new Set(vals).size < 2) return false;
  if (Math.max(...vals) > 9_999_999) return false;
  return true;
}

// ─── Single source of truth: find chartable column from first 10 rows ─────────
// Both the pre-check AND buildChartPage use this same function,
// so they can NEVER disagree and produce a blank page.
function findChartableColumn(dataframe) {
  if (!dataframe || dataframe.length < 3) return null;
  const sample = dataframe.slice(0, 10); // must match what buildChartPage renders
  const cols = Object.keys(sample[0]);

  for (const col of cols) {
    const vals = sample
      .map((r) => parseFloat(r[col]))
      .filter((v) => !isNaN(v) && v >= 0);

    if (
      vals.length === sample.length &&
      isChartable(col, vals) &&
      Math.max(...vals) > 0
    ) {
      return col; // return the first valid column name
    }
  }
  return null; // no chartable column found
}

// ─── Page footer stamp ────────────────────────────────────────────────────────
function stampFooter(doc, pageNum, total) {
  const label = `Page ${pageNum} of ${total}   •   Recruitment Management AI Report`;
  hline(doc, M, PH - 38, PW - M, BORDER, 0.4);
  doc
    .fontSize(7)
    .fillColor(MUTED)
    .text(label, M, PH - 30, { width: CW, align: "center", lineBreak: false });
}

// ─── COVER PAGE ───────────────────────────────────────────────────────────────
function buildCover(doc, query, ts, count, answer) {
  rect(doc, 0, 0, PW, PH, NAVY);
  rect(doc, 0, 0, PW, 6, MINT);

  const titleY = 180;
  doc
    .fontSize(11)
    .fillColor(LTBLUE)
    .font("Helvetica")
    .text("RECRUITMENT MANAGEMENT SYSTEM", M, titleY, {
      align: "center",
      width: CW,
      characterSpacing: 2,
    });
  doc
    .fontSize(36)
    .fillColor(WHITE)
    .font("Helvetica-Bold")
    .text("HR Analytics", M, titleY + 24, { align: "center", width: CW });
  doc
    .fontSize(36)
    .fillColor(MINT)
    .font("Helvetica-Bold")
    .text("Report", M, titleY + 68, { align: "center", width: CW });

  const divY = titleY + 120;
  rect(doc, M + 80, divY, CW - 160, 2, MINT);

  const cardY = divY + 30;
  const cardH = 80;
  const cardW = (CW - 30) / 3;
  const cards = [
    { label: "TOTAL RECORDS", value: String(count) },
    { label: "REPORT DATE", value: ts.split(",")[0] },
    { label: "GENERATED AT", value: ts.split(",")[1]?.trim() || "" },
  ];
  cards.forEach((c, i) => {
    const cx = M + i * (cardW + 15);
    rect(doc, cx, cardY, cardW, cardH, "#243F60", "#2E86C1", 0.8);
    doc
      .fontSize(8)
      .fillColor(LTBLUE)
      .font("Helvetica")
      .text(c.label, cx + 10, cardY + 14, {
        width: cardW - 20,
        align: "center",
        characterSpacing: 1,
      });
    doc
      .fontSize(20)
      .fillColor(WHITE)
      .font("Helvetica-Bold")
      .text(c.value, cx + 10, cardY + 32, {
        width: cardW - 20,
        align: "center",
      });
  });

  const qY = cardY + cardH + 30;
  rect(doc, M, qY, CW, 56, "#162D4A", "#2E86C1", 0.6);
  doc
    .fontSize(8)
    .fillColor(LTBLUE)
    .font("Helvetica")
    .text("QUERY", M + 16, qY + 10, { characterSpacing: 1 });
  doc
    .fontSize(10)
    .fillColor(WHITE)
    .font("Helvetica")
    .text(clean(query, 100), M + 16, qY + 26, {
      width: CW - 32,
      lineBreak: false,
      ellipsis: true,
    });

  const sumY = qY + 80;
  doc
    .fontSize(9)
    .fillColor(LTBLUE)
    .font("Helvetica")
    .text("EXECUTIVE SUMMARY", M, sumY, { characterSpacing: 1 });
  rect(doc, M, sumY + 16, CW, 1, MINT);
  const summaryLines = stripMd(answer).split("\n").slice(0, 4).join(" ");
  doc
    .fontSize(10)
    .fillColor("#C8D8E8")
    .font("Helvetica")
    .text(summaryLines, M, sumY + 26, { width: CW, lineGap: 4 });

  rect(doc, 0, PH - 50, PW, 50, "#162D4A");
  rect(doc, 0, PH - 50, PW, 2, MINT);
  doc
    .fontSize(8)
    .fillColor(LTBLUE)
    .font("Helvetica")
    .text(
      "CONFIDENTIAL  —  Generated by Recruitment Management AI Assistant",
      M,
      PH - 32,
      { width: CW, align: "center" },
    );
}

// ─── CHART PAGE ───────────────────────────────────────────────────────────────
// NOTE: valueCol is passed in — already validated by findChartableColumn().
// This function will ALWAYS draw something; never call it without a valid valueCol.
function buildChartPage(doc, dataframe, valueCol) {
  const data = dataframe.slice(0, 10);
  const labelCol = Object.keys(data[0])[0];
  const maxV = Math.max(...data.map((r) => parseFloat(r[valueCol]) || 0));

  // Page header
  rect(doc, 0, 0, PW, 52, NAVY);
  rect(doc, 0, 0, PW, 4, MINT);
  doc
    .fontSize(8)
    .fillColor(LTBLUE)
    .font("Helvetica")
    .text("RECRUITMENT MANAGEMENT SYSTEM  —  HR Analytics Report", M, 16, {
      width: CW,
      characterSpacing: 1,
    });
  doc
    .fontSize(14)
    .fillColor(WHITE)
    .font("Helvetica-Bold")
    .text("Data Visualization", M, 28, { width: CW });

  let y = 72;
  doc
    .fontSize(11)
    .fillColor(NAVY)
    .font("Helvetica-Bold")
    .text(`Distribution by ${valueCol}`, M, y);
  hline(doc, M, y + 18, PW - M, BLUE, 1.5);
  y += 28;

  const chartH = 180;
  const chartBase = y + chartH;
  const areaW = CW - 50;
  const barW = Math.floor(areaW / data.length) - 8;
  const axisX = M + 45;

  doc
    .moveTo(axisX, y)
    .lineTo(axisX, chartBase)
    .strokeColor(BORDER)
    .lineWidth(1)
    .stroke();
  doc
    .moveTo(axisX, chartBase)
    .lineTo(axisX + areaW, chartBase)
    .strokeColor(BORDER)
    .lineWidth(1)
    .stroke();

  [0.25, 0.5, 0.75, 1.0].forEach((pct) => {
    const gy = chartBase - pct * chartH;
    const val = Math.round(maxV * pct);
    hline(doc, axisX, gy, axisX + areaW, "#E8EEF4", 0.5);
    doc
      .fontSize(6)
      .fillColor(MUTED)
      .text(String(val), M, gy - 4, { width: 40, align: "right" });
  });

  data.forEach((row, i) => {
    const val = parseFloat(row[valueCol]) || 0;
    const barH = Math.round((val / maxV) * chartH);
    const bx = axisX + 4 + i * (barW + 8);
    const by = chartBase - barH;

    rect(doc, bx + 2, by + 2, barW, barH, "#C8D8E8");
    rect(doc, bx, by, barW, barH, BLUE);
    rect(doc, bx, by, barW, 4, "#5DADE2");

    doc
      .fontSize(7)
      .fillColor(DARK)
      .font("Helvetica-Bold")
      .text(String(Math.round(val)), bx, by - 12, {
        width: barW,
        align: "center",
      });

    const lbl = clean(String(row[labelCol] ?? ""), 10);
    doc
      .fontSize(6)
      .fillColor(MUTED)
      .font("Helvetica")
      .text(lbl, bx - 4, chartBase + 4, { width: barW + 8, align: "center" });
  });

  doc.y = chartBase + 35;
}

// ─── TABLE PAGES ──────────────────────────────────────────────────────────────
function buildTablePages(doc, dataframe, startPageNum, getTotal) {
  const cols = Object.keys(dataframe[0]);
  const colW = Math.floor(CW / cols.length);
  const rowH = 17;
  const hdrH = 22;
  const pageHeaderH = 52;

  const drawPageHeader = () => {
    rect(doc, 0, 0, PW, pageHeaderH, NAVY);
    rect(doc, 0, 0, PW, 4, MINT);
    doc
      .fontSize(8)
      .fillColor(LTBLUE)
      .font("Helvetica")
      .text("RECRUITMENT MANAGEMENT SYSTEM  —  HR Analytics Report", M, 16, {
        width: CW,
        characterSpacing: 1,
      });
    doc
      .fontSize(14)
      .fillColor(WHITE)
      .font("Helvetica-Bold")
      .text("Candidate Data Table", M, 28, { width: CW });
  };

  const drawTableHeader = (y) => {
    rect(doc, M, y, CW, hdrH, NAVY);
    let x = M;
    cols.forEach((col) => {
      const maxCh = Math.max(6, Math.floor(colW / 5.5));
      doc
        .fontSize(7.5)
        .font("Helvetica-Bold")
        .fillColor(WHITE)
        .text(clean(col, maxCh), x + 4, y + 7, {
          width: colW - 6,
          lineBreak: false,
          ellipsis: true,
        });
      if (x > M)
        doc
          .moveTo(x, y)
          .lineTo(x, y + hdrH)
          .strokeColor("#2E5C8A")
          .lineWidth(0.5)
          .stroke();
      x += colW;
    });
    return y + hdrH;
  };

  drawPageHeader();
  let y = pageHeaderH + 14;

  doc
    .fontSize(12)
    .fillColor(NAVY)
    .font("Helvetica-Bold")
    .text("DATA TABLE", M, y);
  hline(doc, M, y + 18, PW - M, BLUE, 1.5);
  y += 26;
  y = drawTableHeader(y);

  let currentPage = startPageNum;

  dataframe.forEach((row, i) => {
    if (y + rowH > PH - 55) {
      doc.addPage();
      currentPage++;
      drawPageHeader();
      y = pageHeaderH + 10;
      y = drawTableHeader(y);
    }

    const bg = i % 2 === 0 ? OFFWHITE : WHITE;
    rect(doc, M, y, CW, rowH, bg, BORDER, 0.2);
    if (i % 2 === 0) rect(doc, M, y, 3, rowH, BLUE);

    let x = M;
    cols.forEach((col) => {
      const maxCh = Math.max(6, Math.floor(colW / 5.5));
      doc
        .fontSize(7)
        .font("Helvetica")
        .fillColor(DARK)
        .text(clean(String(row[col] ?? "—"), maxCh), x + 5, y + 5, {
          width: colW - 8,
          lineBreak: false,
          ellipsis: true,
        });
      x += colW;
    });

    y += rowH;
  });

  hline(doc, M, y + 6, PW - M, NAVY, 1);
  doc
    .fontSize(7.5)
    .fillColor(MUTED)
    .text(
      "Generated by Recruitment Management AI Assistant  •  Confidential",
      M,
      y + 12,
      { width: CW, align: "center" },
    );
}

// ─── MAIN EXPORT ──────────────────────────────────────────────────────────────
export function generatePdfReport(dataframe, query, answer) {
  return new Promise((resolve, reject) => {
    const ts = new Date().toLocaleString("en-IN", {
      day: "2-digit",
      month: "short",
      year: "numeric",
      hour: "2-digit",
      minute: "2-digit",
    });

    const filename = `report_${Date.now()}.pdf`;
    const filepath = path.join(REPORTS_DIR, filename);

    const doc = new PDFDocument({
      margin: M,
      size: "A4",
      bufferPages: true,
      autoFirstPage: true,
    });
    const out = fs.createWriteStream(filepath);
    doc.pipe(out);

    // ── Page 1: Cover ─────────────────────────────────────────────────────
    buildCover(doc, query, ts, dataframe?.length ?? 0, answer);

    // ── Page 2 (optional): Chart ───────────────────────────────────────────
    // findChartableColumn() is the SINGLE source of truth used by BOTH the
    // gate check and buildChartPage — so they can never disagree.
    const chartCol = findChartableColumn(dataframe);

    if (chartCol) {
      doc.addPage();
      buildChartPage(doc, dataframe, chartCol); // pass the validated col in
    }

    // ── Table pages ────────────────────────────────────────────────────────
    if (dataframe && dataframe.length > 0) {
      doc.addPage();
      buildTablePages(doc, dataframe, chartCol ? 3 : 2, () => 0);
    }

    // ── Stamp page numbers on all pages ───────────────────────────────────
    doc.flushPages();
    const { start, count } = doc.bufferedPageRange();
    for (let i = 0; i < count; i++) {
      doc.switchToPage(start + i);
      if (i > 0) stampFooter(doc, i + 1, count);
    }

    doc.end();
    out.on("finish", () => resolve(filename));
    out.on("error", reject);
  });
}

// ─── Route handler ────────────────────────────────────────────────────────────
export async function runReportMode(question, conversationHistory) {
  const dataResult = await runSqlMode(question, conversationHistory);
  const filename = await generatePdfReport(
    dataResult.dataframe,
    question,
    dataResult.answer,
  );
  const reportUrl = `/api/reports/${filename}`;

  return {
    type: "REPORT",
    answer: `Report generated with ${dataResult.dataframe?.length ?? 0} records.\n\nDownload: [Download Report](${reportUrl})`,
    dataframe: dataResult.dataframe,
    sql: dataResult.sql,
    filename,
  };
}
