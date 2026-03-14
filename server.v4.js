import dotenv from "dotenv";
dotenv.config();

import express from "express";
import cors from "cors";
import { handleChatV4 } from "./src_v4/app/handleChatV4.js";

const app = express();
const PORT = Number(process.env.PORT || 10001);

app.use(cors());
app.use(express.json({ limit: "1mb" }));

app.post("/chat", async (req, res) => {
  try {
    const query = String(req.body?.query || "").trim();

    if (!query) {
      return res.status(400).json({
        ok: false,
        error: "Query vazia"
      });
    }

    const result = await handleChatV4(query);
    return res.json(result);
  } catch (err) {
    return res.status(500).json({
      ok: false,
      error: err.message
    });
  }
});

app.listen(PORT, () => {
  console.log("════════════════════════════════════════");
  console.log("🚀 BDC v4 iniciado");
  console.log(`📡 Porta: ${PORT}`);
  console.log("════════════════════════════════════════");
});