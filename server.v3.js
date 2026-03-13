import express from "express";
import cors from "cors";
import { handleChat } from "./src_v3/app/handleChat.js";

const app = express();

app.use(cors());
app.use(express.json());

const PORT = process.env.PORT || 10000;

app.get("/", (req, res) => {
  res.json({
    service: "BDC v3",
    status: "running"
  });
});

app.post("/chat", async (req, res) => {

  try {

    const { query } = req.body || {};

    if (!query) {
      return res.status(400).json({
        ok: false,
        error: "query ausente"
      });
    }

    const result = await handleChat(query);

    res.json(result);

  } catch (err) {

    res.status(500).json({
      ok: false,
      error: err.message
    });

  }

});

app.listen(PORT, () => {
  console.log("════════════════════════════════════════");
  console.log("🚀 BDC v3 iniciado");
  console.log("📡 Porta:", PORT);
  console.log("════════════════════════════════════════");
});