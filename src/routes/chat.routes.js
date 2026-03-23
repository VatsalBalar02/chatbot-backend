// src/routes/chat.routes.js

import express from "express";
import path from "path";
import { handleChat, handleReset } from "../controllers/chat.controller.js";
import { REPORTS_DIR } from "../config/constants.js";

const router = express.Router();

router.post("/chat", handleChat);
router.post("/reset", handleReset);

// ─── PDF Report Download ───────────────────────────────────────────────────
router.get("/reports/:filename", (req, res) => {
  const filename = path.basename(req.params.filename); // block path traversal
  const filepath = path.join(REPORTS_DIR, filename); // absolute path

  res.setHeader("Content-Disposition", `attachment; filename="${filename}"`);
  res.setHeader("Content-Type", "application/pdf");

  // ✅ No { root } option — filepath is already absolute, Windows safe
  res.sendFile(filepath, (err) => {
    if (err) {
      console.error("Report download error:", err.message);
      if (!res.headersSent) {
        res.status(404).json({ error: "Report not found" });
      }
    }
  });
});

export default router;
