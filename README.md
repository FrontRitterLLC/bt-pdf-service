# bt-pdf-service

Layout-preserving PDF translation for the BookTranslator website, built on
[BabelDOC](https://github.com/funstory-ai/BabelDOC). PDF in, translated PDF out, over HTTP.

**License: AGPL-3.0** (see `LICENSE`), because it uses BabelDOC. This folder is self-contained and is
the complete source of the service as it runs (BabelDOC, our wrapper and any changes). The website that
calls it is a separate program that talks to it only over HTTP and is not part of this repository.

## Run

```bash
uv sync                   # Python 3.12 (BabelDOC 0.7.1 breaks on 3.14)
uv run babeldoc --warmup  # once: downloads layout model and fonts
uv run bt-pdf-service     # http://127.0.0.1:8860
```

## API

| | |
|---|---|
| `POST /jobs` | multipart: `file`, `lang_out`, `lang_in` (default `auto`), `dual` (alternating original/translated pages), `base_url`, `model`, `api_key` → `{id}` |
| `GET /jobs/{id}` | `{state: queued/running/done/error, progress 0-100, stage, position, error}` |
| `GET /jobs/{id}/result` | translated PDF |
| `DELETE /jobs/{id}` | cancel and delete (the site calls this after downloading) |
| `GET /` | engine version and source link |

The user's own API key pays for the LLM calls. It stays in memory, goes to the worker on stdin and is
never logged or written to disk. Job folders are removed on DELETE, otherwise `BT_PDF_JOB_TTL` after finishing.

## Config (env)

| Var | Default | |
|---|---|---|
| `BT_PDF_SOURCE_URL` | — | public repo of this service (AGPL §13), shown via `GET /` |
| `BT_PDF_ALLOWED_ORIGINS` | `http://127.0.0.1:8850` | CORS: the website's origin(s), comma-separated |
| `BT_PDF_LLM_HOSTS` | DeepSeek, Gemini, OpenAI | allowed `base_url` hosts (https only; stops SSRF) |
| `BT_PDF_PARALLEL_JOBS` | `1` | concurrent BabelDOC runs (~1.2 GB RAM each for small books) |
| `BT_PDF_QUEUE_LIMIT` | `20` | more pending jobs → 503 |
| `BT_PDF_QPS` | `8` | parallel LLM requests per job |
| `BT_PDF_MAX_MB` / `BT_PDF_JOB_TIMEOUT` / `BT_PDF_JOB_TTL` | `100` / `3600` / `3600` | upload limit (MB), seconds |
| `BT_PDF_DATA_DIR` | system temp | job folders (wiped on start) |
| `HOST` / `PORT` | `127.0.0.1` / `8860` | |
| `BT_PDF_DEBUG` | — | set to show BabelDOC logs |

BabelDOC is Copyright funstory.ai, used under AGPL-3.0: https://github.com/funstory-ai/BabelDOC
