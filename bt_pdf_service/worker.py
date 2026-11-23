# SPDX-License-Identifier: AGPL-3.0-only
"""Translate one PDF with BabelDOC. Run as a child process by app.py, one job per process.

Reads the job as JSON on stdin (so the API key never shows up in `ps`), prints progress as
JSON lines on stdout: {"type": "progress", "progress": 0-100, "stage": str} ... then either
{"type": "done", "file": path} or {"type": "error", "error": str}.
"""

import asyncio
import json
import logging
import os
import sys

# Libraries print to stdout too (rich progress, warnings), which would garble the event stream:
# keep a private copy of stdout for events and point fd 1 at stderr for everything else.
_events = os.fdopen(os.dup(1), "w", buffering=1, encoding="utf-8")
os.dup2(2, 1)
sys.stdout = sys.stderr


def emit(**event) -> None:
    _events.write(json.dumps(event, ensure_ascii=False) + "\n")


async def run(job: dict) -> None:
    import babeldoc.format.pdf.high_level as high_level
    from babeldoc.docvision.doclayout import DocLayoutModel
    from babeldoc.format.pdf.translation_config import TranslationConfig, WatermarkOutputMode
    from babeldoc.translator.translator import OpenAITranslator, set_translate_rate_limiter

    high_level.init()
    extra = {}
    if "deepseek" in job["base_url"]:
        extra["thinking"] = "disabled"  # v4 models think by default: slower, no better at translating
    translator = OpenAITranslator(
        lang_in=job["lang_in"],
        lang_out=job["lang_out"],
        model=job["model"],
        base_url=job["base_url"],
        api_key=job["api_key"],
        ignore_cache=False,
        **extra,
    )
    set_translate_rate_limiter(job["qps"])
    layout = DocLayoutModel.load_onnx()
    config = TranslationConfig(
        input_file=job["input"],
        font=None,
        pages=None,
        output_dir=job["out_dir"],
        translator=translator,
        term_extraction_translator=translator,
        doc_layout_model=layout,
        lang_in=job["lang_in"],
        lang_out=job["lang_out"],
        no_dual=not job["dual"],
        no_mono=job["dual"],
        qps=job["qps"],
        pool_max_workers=job["qps"],
        use_alternating_pages_dual=True,  # original page, then translated page (same as the browser engine)
        watermark_output_mode=WatermarkOutputMode.NoWatermark,
        report_interval=1.0,
        working_dir=job["work_dir"],
    )
    getattr(layout, "init_font_mapper", lambda _c: None)(config)
    async for event in high_level.async_translate(config):
        kind = event["type"]
        if kind in ("progress_start", "progress_update", "progress_end"):
            emit(type="progress", progress=round(event.get("overall_progress", 0), 1), stage=event["stage"])
        elif kind == "error":
            emit(type="error", error=str(event["error"]))
            return
        elif kind == "finish":
            result = event["translate_result"]
            path = result.dual_pdf_path if job["dual"] else result.mono_pdf_path
            if not path:
                emit(type="error", error="BabelDOC produced no output")
                return
            emit(type="done", file=str(path))
            return


def main() -> None:
    logging.basicConfig(level=logging.WARNING, stream=sys.stderr)
    job = json.loads(sys.stdin.read())
    try:
        asyncio.run(run(job))
    except Exception as e:  # report instead of a bare traceback so the client sees why
        emit(type="error", error=f"{type(e).__name__}: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
