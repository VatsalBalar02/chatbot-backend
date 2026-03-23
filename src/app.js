// src/app.js

import express from "express";
import cors from "cors";
import chatRoutes from "./routes/chat.routes.js";

const app = express();

app.use(express.json());

app.use(
  cors({
    origin: "*",
    credentials: true,
  }),
);

app.use("/api", chatRoutes);

export default app;
