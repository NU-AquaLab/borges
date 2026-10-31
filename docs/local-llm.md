# Running Borges on open-weight models

Two stages call an LLM: `as_detection` (sibling ASNs in PeeringDB `notes` and
`aka`) and `favicon_analysis` (a vision model names the brand behind a shared
favicon). The paper used `gpt-4o-mini`. Borges also runs against any server that
speaks the OpenAI chat completions API, so the same stages can run on a GPU you
control, with a model whose weights are public and pinned.

**Changing the model changes the results**: mappings made with another model
are not the paper's.

## 1. Serve a model

[vLLM](https://docs.vllm.ai) is the recommended server for a GPU: it batches
concurrent requests, and it can constrain decoding to a JSON schema. Example
profiles live in [`docs/llm-backends/`](llm-backends/), each with the `vllm serve`
command at the top:

| Profile | Model | Hardware |
|---|---|---|
| `vllm-qwen3.yaml` | Qwen3-8B (dense) | one GPU with 24 GB or more |
| `vllm-glm.yaml` | GLM-4.5-Air (106B MoE, 12B active) | 2 × 80 GB, or one in FP8 |
| `vllm-kimi.yaml` | Kimi K2 Instruct (1T MoE, 32B active) | a node of 8 × 80 GB or more |
| `ollama.yaml` | Qwen3-8B on Ollama | a laptop, for trying things out |

Model names change quickly: check the current releases on Hugging Face and
update `vllm serve` and `model` together.

## 2. Point Borges at it

Copy the settings of a profile into the `api.llm` section of `config.yaml`:

```yaml
api:
  llm:
    base_url: http://gpu-host:8000/v1
    model: qwen3-8b              # the --served-model-name
    vision_model: qwen3-vl-8b    # a vision model, or skip favicon_analysis
    structured_output: true
    seed: 0
    max_concurrency: 32
    extra_body:
      chat_template_kwargs:
        enable_thinking: false
    cache_dir: ./data/raw/llm_cache
```

| Setting | What it does |
|---|---|
| `base_url` | The server. `api_key` can stay unset: local servers ignore it. |
| `structured_output` | Sends `as_detection`'s JSON schema as `response_format`, so the answer is always parseable. Small models break free-form JSON. |
| `max_concurrency` | Requests in flight. 1 reproduces the paper's run; 16–64 keeps a GPU busy. |
| `seed`, `temperature` | Keep `temperature: 0` and set a seed for repeatable answers. |
| `extra_body` | Server-specific options sent as is, e.g. turning off Qwen3 or GLM thinking. Borges also drops any `<think>` block before parsing. |
| `cache_dir` | Stores every answer on disk, keyed by model name, settings and prompt. A rerun sends no requests, and the answers can be audited. Serve different quantizations under different model names. |
| `max_tokens` | Caps the answer length (a runaway answer otherwise holds a GPU slot). |

The old key `api.openai` still works as an alias of `api.llm`.

Run the pipeline as usual. Failed or unparseable answers are counted in the log
("N of M LLM answers failed"); they produce no siblings for that ASN.
