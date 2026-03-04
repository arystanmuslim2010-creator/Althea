"""AI summary integration for AML alert narratives."""
from __future__ import annotations

import importlib
import json
import logging
import os
import sys
from pathlib import Path
from typing import Any, Dict, Optional, Tuple

from . import config

# Simple cache for generate_case_summary (replaces st.cache_data)
_summary_cache: Dict[str, str] = {}
_MAX_CACHE_SIZE = 500

logger = logging.getLogger(__name__)

# Optional import for Gemini AI
try:
    from google import genai

    GEMINI_AVAILABLE = True
except ImportError:
    GEMINI_AVAILABLE = False
    genai = None


def _load_api_key() -> str:
    """Load API key with priority: secrets.py -> environment variable -> disabled."""
    logger.debug("_load_api_key called")
    api_key = ""
    secrets_module = None
    module_name = "src.secrets"

    # Try secrets import via multiple strategies.
    try:
        logger.debug("Checking module cache")
        try:
            from . import secrets

            secrets_module = secrets
            logger.debug("secrets module imported successfully (relative)")
        except ImportError:
            try:
                import src.secrets as secrets

                secrets_module = secrets
                logger.debug("secrets module imported successfully (absolute)")
            except ImportError:
                try:
                    import importlib.util as _importlib_util

                    spec = _importlib_util.spec_from_file_location("secrets", Path(__file__).parent / "secrets.py")
                    if spec and spec.loader:
                        secrets = _importlib_util.module_from_spec(spec)
                        spec.loader.exec_module(secrets)
                        secrets_module = secrets
                        logger.debug("secrets module imported successfully (file)")
                except Exception:
                    logger.debug("All import methods failed")

        if secrets_module and module_name in sys.modules:
            importlib.reload(sys.modules[module_name])
            logger.debug("Module reloaded")

        if secrets_module:
            key_value = getattr(secrets_module, "GEMINI_API_KEY", "")
            logger.debug("Raw key value loaded from secrets")
            if key_value and isinstance(key_value, str):
                api_key = key_value.strip()
                logger.debug("Key after strip")
                if api_key in ("PUT_YOUR_KEY_HERE", ""):
                    api_key = ""
                    logger.debug("Key filtered out as placeholder")
    except ImportError as e:
        logger.debug("ImportError loading secrets: %s", e)
        print(f"Warning: Failed to load secrets (ImportError): {e}", file=sys.stderr)
    except Exception as e:
        logger.debug("Exception loading secrets: %s", e)
        print(f"Warning: Failed to load secrets: {e}", file=sys.stderr)

    # Fallback: direct file read
    if not api_key:
        try:
            import re as _re

            _secrets_path = Path(__file__).resolve().parent / "secrets.py"
            if _secrets_path.exists():
                _content = _secrets_path.read_text(encoding="utf-8")
                _m = _re.search(r'GEMINI_API_KEY\s*=\s*["\']([^"\']+)["\']', _content)
                if _m:
                    _fkey = _m.group(1).strip()
                    if _fkey and _fkey not in ("PUT_YOUR_KEY_HERE", "your-api-key-here", ""):
                        api_key = _fkey
        except Exception:
            pass

    # Fallback: environment
    if not api_key:
        api_key = os.environ.get("GEMINI_API_KEY", "").strip()
        logger.debug("Using environment variable fallback")

    logger.debug("Final api_key resolved; empty=%s", not bool(api_key))
    return api_key


# Load API key
GEMINI_API_KEY: str = _load_api_key()
logger.debug("Module-level initialization: GEMINI_AVAILABLE=%s", GEMINI_AVAILABLE)

# DEBUG: Print to stderr for immediate visibility and try multiple fallback methods
if not GEMINI_API_KEY:
    print("WARNING: GEMINI_API_KEY is empty after _load_api_key(), trying fallbacks...", file=sys.stderr)
    try:
        from . import secrets

        direct_key = getattr(secrets, "GEMINI_API_KEY", "")
        if direct_key and direct_key.strip() and direct_key.strip() not in ("PUT_YOUR_KEY_HERE", ""):
            GEMINI_API_KEY = direct_key.strip()
            print(f"FIXED: Loaded key via relative import, length: {len(GEMINI_API_KEY)}", file=sys.stderr)
        else:
            print(f"DEBUG: Relative import worked but key is empty/placeholder: '{direct_key}'", file=sys.stderr)
    except Exception as e:
        print(f"DEBUG: Relative import failed: {e}, trying absolute...", file=sys.stderr)
        try:
            import src.secrets as secrets_abs

            direct_key = getattr(secrets_abs, "GEMINI_API_KEY", "")
            if direct_key and direct_key.strip() and direct_key.strip() not in ("PUT_YOUR_KEY_HERE", ""):
                GEMINI_API_KEY = direct_key.strip()
                print(f"FIXED: Loaded key via absolute import, length: {len(GEMINI_API_KEY)}", file=sys.stderr)
            else:
                print(f"DEBUG: Absolute import worked but key is empty/placeholder: '{direct_key}'", file=sys.stderr)
        except Exception as e2:
            print(f"DEBUG: Absolute import also failed: {e2}, trying file import...", file=sys.stderr)
            try:
                secrets_file = Path(__file__).parent / "secrets.py"
                if secrets_file.exists():
                    with open(secrets_file, "r", encoding="utf-8") as f:
                        content = f.read()
                        import re

                        match = re.search(r'GEMINI_API_KEY\s*=\s*["\']([^"\']+)["\']', content)
                        if match:
                            file_key = match.group(1).strip()
                            if file_key and file_key not in ("PUT_YOUR_KEY_HERE", ""):
                                GEMINI_API_KEY = file_key
                                print(f"FIXED: Loaded key via file read, length: {len(GEMINI_API_KEY)}", file=sys.stderr)
                            else:
                                print(f"DEBUG: File read found key but it's placeholder: '{file_key}'", file=sys.stderr)
                        else:
                            print("DEBUG: File read couldn't find GEMINI_API_KEY in content", file=sys.stderr)
                else:
                    print(f"DEBUG: secrets.py file not found at {secrets_file}", file=sys.stderr)
            except Exception as e3:
                print(f"DEBUG: File read also failed: {e3}", file=sys.stderr)
else:
    print(f"SUCCESS: GEMINI_API_KEY loaded, length: {len(GEMINI_API_KEY)}", file=sys.stderr)


# Initialize Gemini client safely
GEMINI_INIT_ERROR: Optional[str] = None
gemini_client = None

if GEMINI_AVAILABLE and GEMINI_API_KEY:
    try:
        logger.debug("Attempting gemini_client initialization")
        gemini_client = genai.Client(api_key=GEMINI_API_KEY)
        logger.debug("gemini_client initialized successfully")
    except Exception as e:
        GEMINI_INIT_ERROR = str(e)
        gemini_client = None
        logger.debug("gemini_client initialization failed: %s", e)
else:
    logger.debug("Skipping client init")


def get_gemini_status() -> Tuple[bool, Optional[str]]:
    """Return API key presence and initialization error (if any)."""
    logger.debug("get_gemini_status called")
    global GEMINI_API_KEY, gemini_client, GEMINI_INIT_ERROR
    GEMINI_API_KEY = _load_api_key()

    if GEMINI_AVAILABLE and GEMINI_API_KEY and gemini_client is None:
        try:
            gemini_client = genai.Client(api_key=GEMINI_API_KEY)
            GEMINI_INIT_ERROR = None
        except Exception as e:
            GEMINI_INIT_ERROR = str(e)
            gemini_client = None

    result = (bool(GEMINI_API_KEY), GEMINI_INIT_ERROR)
    logger.debug("get_gemini_status returning")
    return result


def generate_case_summary(row_dict: Dict[str, Any]) -> str:
    """Generate a short analyst summary for the given alert row. Never crashes."""
    cache_key = json.dumps(row_dict, sort_keys=True, default=str)
    if cache_key in _summary_cache:
        return _summary_cache[cache_key]
    if len(_summary_cache) >= _MAX_CACHE_SIZE:
        _summary_cache.clear()

    result: str
    global GEMINI_API_KEY, gemini_client, GEMINI_INIT_ERROR
    current_key = _load_api_key()

    if current_key != GEMINI_API_KEY or (gemini_client is None and current_key):
        GEMINI_API_KEY = current_key
        if GEMINI_AVAILABLE and GEMINI_API_KEY:
            try:
                gemini_client = genai.Client(api_key=GEMINI_API_KEY)
                GEMINI_INIT_ERROR = None
            except Exception as e:
                GEMINI_INIT_ERROR = str(e)
                gemini_client = None

    if gemini_client is None:
        if not GEMINI_API_KEY:
            result = "Gemini API key not configured. Add your key to src/secrets.py or set GEMINI_API_KEY environment variable."
        elif GEMINI_INIT_ERROR:
            result = f"Gemini initialization failed: {GEMINI_INIT_ERROR}"
        else:
            result = "Gemini client not available."
    else:
        prompt = f"""
You are an AML analyst assistant.

Transaction context:
- Customer segment: {row_dict.get('segment', 'unknown')}
- Risk score: {row_dict.get('risk_score', 0):.1f}
- Behavioral deviations: {row_dict.get('Reason', 'N/A')}

Write a short 2-3 sentence analyst summary explaining:
1) Why this alert is high risk
2) What behavioral change occurred

Do NOT mention AI or models.
""".strip()
        try:
            response = gemini_client.models.generate_content(
                model=config.GEMINI_MODEL_NAME,
                contents=prompt,
            )
            result = response.text.strip() if response and hasattr(response, "text") else "AI summary returned empty response."
        except Exception as e:
            result = f"AI summary failed: {str(e)}"

    _summary_cache[cache_key] = result
    return result
