// src/controllers/chat.controller.js

import { chatbot, resetConversation } from "../services/chatbot.service.js";


export const handleChat = async (req, res) => {
  const { message } = req.body;

  if (!message || typeof message !== "string") {
    return res.status(422).json({
      detail: "Field 'message' is required and must be a string.",
    });
  }

  if (message.length > 2000) {
    return res.status(422).json({
      detail: "Message too long.",
    });
  }

  try {
    const start = Date.now();

    const result = await chatbot(message);

    const elapsed = Date.now() - start;

    return res.json({
      type: result.type,
      answer: result.answer,
      response_time_ms: elapsed,
    });
  } catch (err) {
    console.error("[ERROR]", err);
    return res.status(500).json({ detail: err.message });
  }
};

export const handleReset = (_req, res) => {
  resetConversation();
  res.json({ message: "Conversation reset." });
};
