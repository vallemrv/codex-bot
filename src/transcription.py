"""
Grok Speech-to-Text transcription module.
Uses X.AI STT API at https://api.x.ai/v1/stt
Pricing: $0.10/hr (REST)
"""

import os
import logging
from pathlib import Path

import httpx

logger = logging.getLogger(__name__)

MIME_TYPES: dict[str, str] = {
    ".ogg":  "audio/ogg",
    ".oga":  "audio/ogg",
    ".mp3":  "audio/mpeg",
    ".mpeg": "audio/mpeg",
    ".mpga": "audio/mpeg",
    ".wav":  "audio/wav",
    ".m4a":  "audio/mp4",
    ".mp4":  "audio/mp4",
    ".webm": "audio/webm",
    ".flac": "audio/flac",
    ".aac":  "audio/aac",
}


def is_configured() -> bool:
    return bool(os.getenv("XAI_API_KEY"))


async def transcribe(file_path: str) -> str | None:
    """
    Transcribe an audio file using Grok STT API.
    Returns the transcribed text, or None on failure.
    """
    xai_api_key = os.getenv("XAI_API_KEY")
    if not xai_api_key:
        logger.warning("XAI_API_KEY not set — transcription disabled")
        return None

    path = Path(file_path)
    if not path.exists():
        logger.error(f"Audio file not found: {file_path}")
        return None

    ext  = path.suffix.lower()
    mime = MIME_TYPES.get(ext, "audio/ogg")

    try:
        async with httpx.AsyncClient(timeout=120) as client:
            with open(file_path, "rb") as f:
                response = await client.post(
                    "https://api.x.ai/v1/stt",
                    headers={"Authorization": f"Bearer {xai_api_key}"},
                    files={"file": (path.name, f, mime)},
                )

        if response.is_success:
            text = response.json().get("text", "").strip()
            logger.info(f"Transcription OK ({len(text)} chars)")
            return text or None
        else:
            logger.error(f"Grok STT error {response.status_code}: {response.text}")
            return None

    except Exception as exc:
        logger.error(f"Transcription exception: {exc}", exc_info=True)
        return None
