// src/utils/embedding.js
import { getOpenAIClient } from "../services/ai.service.js";

// ─── In-memory embedding cache ────────────────────────────────────────────────
// Same query asked twice returns instantly from cache — zero API cost.
// LRU-style: evict oldest when cache exceeds MAX_SIZE.
const embeddingCache = new Map();
const MAX_CACHE_SIZE = 500;

export async function embedText(text) {
  const key = text.trim().toLowerCase();

  if (embeddingCache.has(key)) {
    return embeddingCache.get(key);
  }

  const client = getOpenAIClient();
  const response = await client.embeddings.create({
    model: "text-embedding-3-small",
    input: text,
  });
  const vector = response.data[0].embedding;

  // Evict oldest entry if cache is full
  if (embeddingCache.size >= MAX_CACHE_SIZE) {
    const firstKey = embeddingCache.keys().next().value;
    embeddingCache.delete(firstKey);
  }

  embeddingCache.set(key, vector);
  return vector;
}

export function getEmbeddingCacheSize() {
  return embeddingCache.size;
}
