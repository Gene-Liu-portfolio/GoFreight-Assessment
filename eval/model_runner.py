"""Unified model runner for multi-model Text-to-SQL evaluation.

All model parameters (model_id, temperature, max_tokens, provider, base_url)
are centralized in MODEL_CONFIGS. The three runner functions delegate to a
single dispatch helper so that adding a model is a single-line config change.
"""

import os
import time

import anthropic
import openai

from src.db.schema import get_schema_description
from src.shared import extract_sql
from prompts.system_prompt import SYSTEM_PROMPT_TEMPLATE


MODEL_CONFIGS: dict[str, dict] = {
    "claude-sonnet-4.6": {
        "provider": "anthropic",
        "model_id": "claude-sonnet-4-6",
        "temperature": 0,
        "max_tokens": 1024,
    },
    "gpt-4o": {
        "provider": "openai",
        "model_id": "gpt-4o-2024-11-20",
        "temperature": 0,
        "max_tokens": 1024,
    },
    "deepseek-v3": {
        "provider": "openai_compatible",
        "model_id": "deepseek-chat",
        "temperature": 0,
        "max_tokens": 1024,
        "api_key_env": "DEEPSEEK_API_KEY",
        "base_url": "https://api.deepseek.com",
    },
}


def _get_system_prompt() -> str:
    schema = get_schema_description()
    return SYSTEM_PROMPT_TEMPLATE.format(schema=schema)


def _run_anthropic(cfg: dict, question: str, system_prompt: str) -> dict:
    client = anthropic.Anthropic()
    start = time.time()
    response = client.messages.create(
        model=cfg["model_id"],
        max_tokens=cfg["max_tokens"],
        temperature=cfg["temperature"],
        system=system_prompt,
        messages=[{"role": "user", "content": question}],
    )
    elapsed = time.time() - start
    raw = response.content[0].text
    usage = {
        "input_tokens": response.usage.input_tokens,
        "output_tokens": response.usage.output_tokens,
    }
    return {"sql": extract_sql(raw), "raw": raw, "latency": elapsed, "usage": usage}


def _run_openai_chat(cfg: dict, question: str, system_prompt: str) -> dict:
    if cfg["provider"] == "openai_compatible":
        client = openai.OpenAI(
            api_key=os.environ[cfg["api_key_env"]],
            base_url=cfg["base_url"],
        )
    else:
        client = openai.OpenAI()
    start = time.time()
    response = client.chat.completions.create(
        model=cfg["model_id"],
        max_tokens=cfg["max_tokens"],
        temperature=cfg["temperature"],
        messages=[
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": question},
        ],
    )
    elapsed = time.time() - start
    raw = response.choices[0].message.content
    usage = {
        "input_tokens": response.usage.prompt_tokens,
        "output_tokens": response.usage.completion_tokens,
    }
    return {"sql": extract_sql(raw), "raw": raw, "latency": elapsed, "usage": usage}


_PROVIDER_DISPATCH = {
    "anthropic": _run_anthropic,
    "openai": _run_openai_chat,
    "openai_compatible": _run_openai_chat,
}


# Public API kept identical to prior version: callers can still import
# MODEL_RUNNERS and call run_model(name, question).
MODEL_RUNNERS = {name: _PROVIDER_DISPATCH[cfg["provider"]] for name, cfg in MODEL_CONFIGS.items()}


def run_model(model_name: str, question: str) -> dict:
    """Run a specific model on a question. Returns dict with sql, raw, latency."""
    if model_name not in MODEL_CONFIGS:
        return {"sql": "", "raw": "", "latency": 0, "error": f"Unknown model: {model_name}"}

    cfg = MODEL_CONFIGS[model_name]
    runner = _PROVIDER_DISPATCH[cfg["provider"]]
    system_prompt = _get_system_prompt()
    try:
        return runner(cfg, question, system_prompt)
    except Exception as e:
        return {"sql": "", "raw": "", "latency": 0, "error": str(e)}
