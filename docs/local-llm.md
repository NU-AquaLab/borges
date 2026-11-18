# Running Borges on open-weight models

Two stages call an LLM: `as_detection` (sibling ASNs in PeeringDB `notes` and
`aka`) and `favicon_analysis` (a vision model names the brand behind a shared
favicon). The paper used `gpt-4o-mini`. Borges also runs against any server that
speaks the OpenAI chat completions API, so the same stages can run on a GPU you
control, with a model whose weights are public and pinned.

**Changing the model changes the results**: mappings made with another model
are not the paper's. See [open-weight-evaluation.md](open-weight-evaluation.md) for how
a full run with Qwen3 compares with the published artifacts.

## 1. Serve a model

[vLLM](https://docs.vllm.ai) is the recommended server for a GPU: it batches
concurrent requests, and it can constrain decoding to a JSON schema. Example
profiles live in `docs/llm-backends/`, each with the `vllm serve`
command at the top:

| Profile | Model | Hardware |
|---|---|---|
| `vllm-qwen3.yaml` | Qwen3-8B (dense) | one GPU with 24 GB or more |
| `vllm-glm.yaml` | GLM-4.5-Air (106B MoE, 12B active) | 2 × 80 GB, or one in FP8 |
| `vllm-kimi.yaml` | Kimi K2 Instruct (1T MoE, 32B active) | a node of 8 × 80 GB or more |
| `ollama.yaml` | Qwen3-8B and Qwen3-VL on Ollama | a 12 GB GPU, including older ones ([evaluated](open-weight-evaluation.md)) |
| `workers-ai.yaml` | GLM-4.7-Flash and Llama 4 Scout on Cloudflare Workers AI | none: hosted, see [below](#cloudflare-workers-ai-no-gpu) |

Model names change quickly: check the current releases on Hugging Face and
update `vllm serve` and `model` together.

## 2. Point Borges at it

The quickest way is `--llm`, which replaces `api.llm` with a profile for that
command: a shipped one by name (`workers-ai`, `ollama`, `vllm-qwen3`, ...) or
any YAML file with the same keys.

```bash
borges --llm vllm-qwen3 pipeline run
borges --llm my-server.yaml pipeline run
```

`BORGES_LLM=workers-ai` in the environment does the same. To keep the settings
in `config.yaml` instead, copy the whole profile into `api.llm`, not only
`base_url` and `model`: `timeout`, `max_tokens` and `extra_body` matter too.
The defaults (a 30 s timeout, for example) suit the OpenAI API, not every server:

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
| `max_tokens` | Caps the answer length (a runaway answer otherwise holds a GPU slot). Not enforced on Ollama: rely on `timeout` there. |

The old key `api.openai` still works as an alias of `api.llm`.

Run the pipeline as usual. Failed or unparseable answers are counted in the log
("N of M LLM answers failed"); they produce no siblings for that ASN.

## Cloudflare Workers AI (no GPU)

[Workers AI](https://developers.cloudflare.com/workers-ai/) hosts open-weight
models behind an [OpenAI-compatible endpoint](https://developers.cloudflare.com/workers-ai/configuration/open-ai-compatibility/),
so Borges can use them without a GPU. The free plan gives 10,000 neurons a day
(Cloudflare's unit of usage), reset at 00:00 UTC; the paid plan bills per neuron
([pricing](https://developers.cloudflare.com/workers-ai/platform/pricing/)).

1. In the Cloudflare dashboard, create an API token with the Workers AI
   permission, and copy your account ID.
2. Put both in the environment, not in `config.yaml`:

    ```bash
    export CLOUDFLARE_ACCOUNT_ID=...
    export CLOUDFLARE_API_TOKEN=...
    ```

3. Check the backend with a few requests before a full run:

    ```bash
    python scripts/check_llm_backend.py --llm workers-ai --vision
    ```

    It fails only if the backend does not work. If the model finds other
    siblings than expected, it says so as a warning.

4. Run the pipeline with the profile:

    ```bash
    borges --llm workers-ai pipeline run
    ```

    The `${CLOUDFLARE_ACCOUNT_ID}` and `${CLOUDFLARE_API_TOKEN}` placeholders
    in [`workers-ai.yaml`](llm-backends/workers-ai.yaml) are read from the
    environment (or a `.env` file); Borges names the variable if one is missing.
    To use `config.yaml` instead, copy the whole profile into `api.llm`,
    including `timeout: 120`: the default 30 s can be too short here.

**When the daily quota runs out**, Workers AI answers HTTP 429 with error code
3036. Retrying cannot help until the reset, so Borges stops the pipeline at once:
the stage is not saved, later stages do not run, and the message says so.
Answers received before are in `cache_dir`: run the same command after the
reset and only the missing answers are requested. A full run may take more than
one day of the free quota; the token counts in the log let you estimate it.
"Capacity temporarily exceeded" (code 3040) is different: Borges waits
`retry_delay`, as for any 429, and that answer counts as failed. Lower
`max_concurrency` if it happens often.

| Stage | Model in the profile | Why |
|---|---|---|
| `as_detection` | `@cf/zai-org/glm-4.7-flash` | Cheap, 131k context, still on the free plan |
| `favicon_analysis` | `@cf/meta/llama-4-scout-17b-16e-instruct` | Accepts images |

Avoid `@cf/qwen/qwen3-30b-a3b-fp8`: Cloudflare lists it for deprecation on
2026-11-20. Some larger models (Kimi K2.6, GLM-5.2) need the paid plan and
answer HTTP 403 (code 5035) on the free one. Check the
[model catalog](https://developers.cloudflare.com/workers-ai/models/) before
choosing another.

Checked against the service (November 2026): image input through the
OpenAI-compatible endpoint works with Llama 4 Scout, and
`chat_template_kwargs: {enable_thinking: false}` turns off GLM-4.7-Flash's
reasoning. Llama 4 Scout rejects `seed: 0` (error 5006, "seed must be >= 1"),
so the profile uses `seed: 1`. GLM-4.7-Flash does not always answer
`{"ASs": [...]}`: it may answer a bare list such as `[64497, 64498]`, or
`null`, which Borges accepts. It has also answered `null` where the notes
name siblings; those ASNs are still found by the regex when the text says
`AS64497`, but compare a sample of results before relying on this model.

Not yet verified against the service:

- `structured_output`. [JSON mode](https://developers.cloudflare.com/workers-ai/features/json-mode/)
  is documented for a few models only, and fails with "JSON Mode couldn't be
  met" when the model cannot comply, so the profile leaves it off;
- the exact error body of the OpenAI-compatible endpoint. Borges looks for code
  3036 or "daily free allocation" in the error message and body;
- how many neurons a full run takes.

As with any other model, the mappings will differ from the paper's.
