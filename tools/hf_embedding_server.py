#!/usr/bin/env python3

import argparse
import threading
from typing import Any

import torch
import torch.nn.functional as F
import uvicorn
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from transformers import AutoModel, AutoTokenizer


DEFAULT_ALIASES = {
    "coding-message-model": "sentence-transformers/all-MiniLM-L6-v2",
    "coding-action-model": "BAAI/bge-small-en-v1.5",
    "coding-search-chunk-model": "BAAI/bge-small-en-v1.5",
}


class EmbeddingRequest(BaseModel):
    model: str
    input: str | list[str]


class ModelBundle:
    def __init__(self, model_id: str, device: str):
        self.tokenizer = AutoTokenizer.from_pretrained(model_id)
        self.model = AutoModel.from_pretrained(model_id)
        self.model.eval()
        self.device = device
        if device != "cpu":
            self.model.to(device)

    def encode(self, texts: list[str]) -> list[list[float]]:
        encoded = self.tokenizer(
            texts,
            padding=True,
            truncation=True,
            max_length=512,
            return_tensors="pt",
        )
        encoded = {
            key: value.to(self.device) if hasattr(value, "to") else value
            for key, value in encoded.items()
        }

        with torch.inference_mode():
            outputs = self.model(**encoded)
            hidden = outputs.last_hidden_state
            mask = encoded["attention_mask"].unsqueeze(-1).expand(hidden.size()).float()
            pooled = (hidden * mask).sum(dim=1) / mask.sum(dim=1).clamp(min=1e-9)
            normalized = F.normalize(pooled, p=2, dim=1)

        return normalized.cpu().tolist()


class ModelRegistry:
    def __init__(self, aliases: dict[str, str], device: str):
        self.aliases = aliases
        self.device = device
        self.lock = threading.Lock()
        self.loaded: dict[str, ModelBundle] = {}

    def resolve(self, requested_model: str) -> tuple[str, str]:
        model_id = self.aliases.get(requested_model, requested_model)
        return requested_model, model_id

    def get(self, requested_model: str) -> ModelBundle:
        _, model_id = self.resolve(requested_model)
        with self.lock:
            if model_id not in self.loaded:
                print(f"[hf-embedding-server] loading {requested_model} -> {model_id}", flush=True)
                self.loaded[model_id] = ModelBundle(model_id, self.device)
            return self.loaded[model_id]


def create_app(registry: ModelRegistry) -> FastAPI:
    app = FastAPI()

    @app.get("/health")
    async def health() -> dict[str, Any]:
        return {
            "ok": True,
            "device": registry.device,
            "aliases": registry.aliases,
            "loaded_models": list(registry.loaded.keys()),
        }

    @app.post("/embeddings")
    @app.post("/v1/embeddings")
    async def embeddings(request: EmbeddingRequest) -> dict[str, Any]:
        if isinstance(request.input, str):
            texts = [request.input]
        elif isinstance(request.input, list) and all(isinstance(item, str) for item in request.input):
            texts = request.input
        else:
            raise HTTPException(status_code=400, detail="input must be a string or list of strings")

        bundle = registry.get(request.model)
        vectors = bundle.encode(texts)
        return {
            "object": "list",
            "data": [
                {"object": "embedding", "index": index, "embedding": vector}
                for index, vector in enumerate(vectors)
            ],
            "model": request.model,
        }

    return app


def parse_aliases(items: list[str]) -> dict[str, str]:
    aliases = dict(DEFAULT_ALIASES)
    for item in items:
        if "=" not in item:
            raise ValueError(f"invalid alias '{item}', expected alias=model_id")
        alias, model_id = item.split("=", 1)
        aliases[alias.strip()] = model_id.strip()
    return aliases


def pick_device(requested: str) -> str:
    if requested != "auto":
        return requested
    if torch.backends.mps.is_available():
        return "mps"
    return "cpu"


def main() -> None:
    parser = argparse.ArgumentParser(description="Local OpenAI-compatible embedding server")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8081)
    parser.add_argument(
        "--alias",
        action="append",
        default=[],
        help="Map a logical model name to a Hugging Face model id: alias=model_id",
    )
    parser.add_argument(
        "--device",
        choices=["auto", "cpu", "mps"],
        default="auto",
        help="Torch device to use for inference",
    )
    args = parser.parse_args()

    aliases = parse_aliases(args.alias)
    device = pick_device(args.device)
    print(f"[hf-embedding-server] device={device}", flush=True)
    print(f"[hf-embedding-server] aliases={aliases}", flush=True)

    app = create_app(ModelRegistry(aliases, device))
    uvicorn.run(app, host=args.host, port=args.port, log_level="info")


if __name__ == "__main__":
    main()
