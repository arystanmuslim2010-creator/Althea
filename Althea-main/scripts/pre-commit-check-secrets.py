#!/usr/bin/env python3
"""Pre-commit check: fail if staged files contain API keys or secrets."""
import re
import subprocess
import sys

# Patterns that look like real API keys (avoid placeholders)
SECRET_PATTERNS = [
    (re.compile(r"AIza[0-9A-Za-z\-_]{35}"), "Google API key (AIza...)"),
    (re.compile(r"sk-[a-zA-Z0-9]{20,}"), "OpenAI-style secret key (sk-...)"),
    (re.compile(r"ghp_[a-zA-Z0-9]{36}"), "GitHub personal access token (ghp_...)"),
]

PLACEHOLDERS = ("your-api-key-here", "PUT_YOUR_KEY_HERE", "your_api_key_here", "")


def get_staged_files():
    out = subprocess.run(
        ["git", "diff", "--cached", "--name-only", "--diff-filter=ACM"],
        capture_output=True,
        text=True,
        check=True,
    )
    return (f for f in out.stdout.strip().splitlines() if f)


def main():
    found = []
    for path in get_staged_files():
        try:
            with open(path, "r", encoding="utf-8", errors="ignore") as f:
                content = f.read()
        except OSError:
            continue
        for pattern, name in SECRET_PATTERNS:
            for m in pattern.finditer(content):
                snippet = m.group(0)
                if snippet in PLACEHOLDERS:
                    continue
                found.append((path, name, snippet[:20] + "..."))
    if not found:
        return 0
    print("ERROR: Possible secrets found in staged files. Do not commit API keys.", file=sys.stderr)
    for path, name, preview in found:
        print(f"  {path}: {name} ({preview})", file=sys.stderr)
    print("Remove the key from the file or unstage it. See backend/src/SECRETS_README.md", file=sys.stderr)
    return 1


if __name__ == "__main__":
    sys.exit(main())
