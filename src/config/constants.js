import "dotenv/config";
import path from "path";

export const OPENAI_API_KEY = process.env.OPENAI_API_KEY || "";
if (!OPENAI_API_KEY) throw new Error("OPENAI_API_KEY is not set.");

export const DB_CONFIG = {
  server: process.env.DB_HOST || "localhost",
  port: parseInt(process.env.DB_PORT || "1433", 10),
  user: process.env.DB_USER || "",
  password: process.env.DB_PASSWORD || "",
  database: process.env.DB_NAME || "",
  options: {
    encrypt: true,
    trustServerCertificate: true,
  },
  pool: {
    max: 10,
    min: 0,
    idleTimeoutMillis: 30000,
  },
};

export const PDF_PATH = process.env.PDF_PATH || "data/Recruitment-Guide.pdf";
export const VECTRA_DIR = process.env.VECTRA_DIR || "vectra_store";
export const MAX_HISTORY = 2;

// process.cwd() = the directory where you run "node" or "npm start"
// Since you run from E:\ChatBot_FInal\backend, this always resolves to:
// E:\ChatBot_FInal\backend\reports  ✅
export const REPORTS_DIR = path.join(process.cwd(), "reports");
