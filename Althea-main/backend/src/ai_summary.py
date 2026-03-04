"""AI summary integration for AML alert narratives."""
from __future__ import annotations

import importlib
import json
import os
import time
from pathlib import Path
from typing import Optional, Dict, Any, Tuple

from . import config

# Simple cache for generate_case_summary (replaces st.cache_data)
_summary_cache: Dict[str, str] = {}
_MAX_CACHE_SIZE = 500

# Get debug log path (workspace root/.cursor/debug.log)
# Try multiple methods to find workspace root
def _get_debug_log_path():
    """Find workspace root and return debug log path."""
    # Method 1: From file location (if in workspace)
    file_path = Path(__file__).resolve()
    # Look for workspace root by going up until we find .cursor or backend
    current = file_path.parent
    for _ in range(5):  # Max 5 levels up
        cursor_dir = current / ".cursor"
        if cursor_dir.exists() or (current / "backend").exists():
            return cursor_dir / "debug.log"
        if current.parent == current:  # Reached root
            break
        current = current.parent
    
    # Method 2: Use current working directory
    cwd = Path.cwd()
    cursor_dir = cwd / ".cursor"
    cursor_dir.mkdir(exist_ok=True)
    return cursor_dir / "debug.log"

_DEBUG_LOG_PATH = _get_debug_log_path()
_DEBUG_LOG_PATH.parent.mkdir(exist_ok=True)

def _debug_log(location: str, message: str, hypothesis_id: str, data: dict = None):
    """Helper to write debug logs safely."""
    try:
        with open(_DEBUG_LOG_PATH, "a", encoding="utf-8") as f:
            f.write(json.dumps({
                "sessionId": "debug-session",
                "runId": "run1",
                "hypothesisId": hypothesis_id,
                "location": location,
                "message": message,
                "data": data or {},
                "timestamp": int(time.time() * 1000)
            }) + "\n")
    except Exception:
        pass  # Don't crash on logging errors

# Optional import for Gemini AI
try:
    from google import genai
    GEMINI_AVAILABLE = True
except ImportError:
    GEMINI_AVAILABLE = False
    genai = None


def _load_api_key() -> str:
    """Load API key with priority: secrets.py -> environment variable -> disabled"""
    # #region agent log
    _debug_log("ai_summary.py:_load_api_key:entry", "_load_api_key called", "H1,H2,H3")
    # #endregion
    api_key = ""
    
    # Try to load from secrets.py (with reload to pick up changes)
    # Try multiple import methods
    secrets_module = None
    try:
        import sys
        # #region agent log
        module_name = 'src.secrets'
        _debug_log("ai_summary.py:_load_api_key:before_reload", "Checking module cache", "H3,H5", {"module_name": module_name, "in_modules": module_name in sys.modules})
        # #endregion
        
        # Method 1: Relative import
        try:
            from . import secrets
            secrets_module = secrets
            # #region agent log
            _debug_log("ai_summary.py:_load_api_key:import_success", "secrets module imported successfully (relative)", "H1")
            # #endregion
        except ImportError:
            # Method 2: Absolute import
            try:
                import src.secrets as secrets
                secrets_module = secrets
                # #region agent log
                _debug_log("ai_summary.py:_load_api_key:import_success", "secrets module imported successfully (absolute)", "H1")
                # #endregion
            except ImportError:
                # Method 3: Direct file import (use importlib.util alias to avoid shadowing global importlib)
                try:
                    import importlib.util as _importlib_util
                    spec = _importlib_util.spec_from_file_location("secrets", Path(__file__).parent / "secrets.py")
                    if spec and spec.loader:
                        secrets = _importlib_util.module_from_spec(spec)
                        spec.loader.exec_module(secrets)
                        secrets_module = secrets
                        # #region agent log
                        _debug_log("ai_summary.py:_load_api_key:import_success", "secrets module imported successfully (file)", "H1")
                        # #endregion
                except Exception as file_import_error:
                    # #region agent log
                    _debug_log("ai_summary.py:_load_api_key:all_imports_failed", "All import methods failed", "H1", {"file_import_error": str(file_import_error)})
                    # #endregion
        
        # Force reload to pick up changes in secrets.py
        if secrets_module and module_name in sys.modules:
            importlib.reload(sys.modules[module_name])
            # #region agent log
            _debug_log("ai_summary.py:_load_api_key:after_reload", "Module reloaded", "H5")
            # #endregion
        
        if secrets_module:
            key_value = getattr(secrets_module, "GEMINI_API_KEY", "")
            # #region agent log
            _debug_log("ai_summary.py:_load_api_key:raw_value", "Raw key value from secrets", "H2,H6", {
                "key_type": type(key_value).__name__,
                "key_length": len(str(key_value)) if key_value else 0,
                "key_preview": str(key_value)[:20] + "..." if key_value and len(str(key_value)) > 20 else str(key_value)
            })
            # #endregion
            if key_value and isinstance(key_value, str):
                api_key = key_value.strip()
                # #region agent log
                _debug_log("ai_summary.py:_load_api_key:after_strip", "Key after strip", "H2", {"api_key_length": len(api_key), "is_placeholder": api_key in ("PUT_YOUR_KEY_HERE", "")})
                # #endregion
                # Remove placeholder text
                if api_key in ("PUT_YOUR_KEY_HERE", ""):
                    api_key = ""
                    # #region agent log
                    _debug_log("ai_summary.py:_load_api_key:filtered_out", "Key filtered out as placeholder", "H2")
                    # #endregion
    except ImportError as e:
        # #region agent log
        _debug_log("ai_summary.py:_load_api_key:import_error", "ImportError caught", "H1", {"error": str(e)})
        # #endregion
        import sys
        print(f"Warning: Failed to load secrets (ImportError): {e}", file=sys.stderr)
    except Exception as e:
        # #region agent log
        _debug_log("ai_summary.py:_load_api_key:exception", "Exception loading secrets", "H1", {"error": str(e), "error_type": type(e).__name__})
        # #endregion
        # Log but don't crash
        import sys
        print(f"Warning: Failed to load secrets: {e}", file=sys.stderr)
    
    # Fallback: direct file read (most reliable, avoids all import issues)
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

    # Fallback to environment variable
    if not api_key:
        api_key = os.environ.get("GEMINI_API_KEY", "").strip()
        # #region agent log
        _debug_log("ai_summary.py:_load_api_key:env_fallback", "Using environment variable fallback", "H2", {"env_key_length": len(api_key)})
        # #endregion
    
    # #region agent log
    _debug_log("ai_summary.py:_load_api_key:return", "Final api_key value", "H2", {"api_key_length": len(api_key), "api_key_empty": not api_key})
    # #endregion
    return api_key


# Load API key
GEMINI_API_KEY: str = _load_api_key()

# #region agent log
_debug_log("ai_summary.py:module_init", "Module-level initialization", "H4", {"GEMINI_AVAILABLE": GEMINI_AVAILABLE, "GEMINI_API_KEY_length": len(GEMINI_API_KEY), "GEMINI_API_KEY_empty": not GEMINI_API_KEY, "GEMINI_API_KEY_preview": GEMINI_API_KEY[:30] + "..." if GEMINI_API_KEY and len(GEMINI_API_KEY) > 30 else GEMINI_API_KEY})
# #endregion

# DEBUG: Print to stderr for immediate visibility and try multiple fallback methods
import sys
if not GEMINI_API_KEY:
    print(f"WARNING: GEMINI_API_KEY is empty after _load_api_key(), trying fallbacks...", file=sys.stderr)
    # Fallback 1: Direct relative import
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
        # Fallback 2: Absolute import
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
            # Fallback 3: Direct file read
            try:
                secrets_file = Path(__file__).parent / "secrets.py"
                if secrets_file.exists():
                    with open(secrets_file, "r", encoding="utf-8") as f:
                        content = f.read()
                        # Simple regex to extract key value
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
                            print(f"DEBUG: File read couldn't find GEMINI_API_KEY in content", file=sys.stderr)
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
        # #region agent log
        _debug_log("ai_summary.py:client_init_attempt", "Attempting gemini_client initialization", "H4")
        # #endregion
        gemini_client = genai.Client(api_key=GEMINI_API_KEY)
        # #region agent log
        _debug_log("ai_summary.py:client_init_success", "gemini_client initialized successfully", "H4", {"client_not_none": gemini_client is not None})
        # #endregion
    except Exception as e:
        GEMINI_INIT_ERROR = str(e)
        gemini_client = None
        # #region agent log
        _debug_log("ai_summary.py:client_init_failed", "gemini_client initialization failed", "H4", {"error": str(e), "error_type": type(e).__name__})
        # #endregion
else:
    # #region agent log
    _debug_log("ai_summary.py:client_init_skipped", "Skipping client init", "H4", {"GEMINI_AVAILABLE": GEMINI_AVAILABLE, "has_key": bool(GEMINI_API_KEY)})
    # #endregion


def get_gemini_status() -> Tuple[bool, Optional[str]]:
    """Return API key presence and initialization error (if any)."""
    # #region agent log
    _debug_log("ai_summary.py:get_gemini_status:entry", "get_gemini_status called", "H7")
    # #endregion
    # Reload key in case it was updated
    global GEMINI_API_KEY, gemini_client, GEMINI_INIT_ERROR
    GEMINI_API_KEY = _load_api_key()
    
    # Reinitialize client if key is now available
    if GEMINI_AVAILABLE and GEMINI_API_KEY and gemini_client is None:
        try:
            gemini_client = genai.Client(api_key=GEMINI_API_KEY)
            GEMINI_INIT_ERROR = None
        except Exception as e:
            GEMINI_INIT_ERROR = str(e)
            gemini_client = None
    
    result = (bool(GEMINI_API_KEY), GEMINI_INIT_ERROR)
    # #region agent log
    _debug_log("ai_summary.py:get_gemini_status:return", "get_gemini_status returning", "H7", {"has_key": result[0], "init_error": result[1], "gemini_client_none": gemini_client is None})
    # #endregion
    return result


def generate_case_summary(row_dict: Dict[str, Any]) -> str:
    """Generate a short analyst summary for the given alert row. Never crashes."""
    cache_key = json.dumps(row_dict, sort_keys=True, default=str)
    if cache_key in _summary_cache:
        return _summary_cache[cache_key]
    if len(_summary_cache) >= _MAX_CACHE_SIZE:
        _summary_cache.clear()

    result: str
    # Check current API key state (in case it was updated)
    global GEMINI_API_KEY, gemini_client, GEMINI_INIT_ERROR
    current_key = _load_api_key()

    # If key changed or client is None, try to reinitialize
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

Write a short 2–3 sentence analyst summary explaining:
1) Why this alert is high risk
2) What behavioral change occurred

Do NOT mention AI or models.
""".strip()
        try:
            response = gemini_client.models.generate_content(
                model=config.GEMINI_MODEL_NAME,
                contents=prompt,
            )
            result = response.text.strip() if response and hasattr(response, 'text') else "AI summary returned empty response."
        except Exception as e:
            result = f"AI summary failed: {str(e)}"

    _summary_cache[cache_key] = result
    return result
