# Add backend root to path for tests
import sys
from pathlib import Path
_backend = Path(__file__).resolve().parent.parent
if str(_backend) not in sys.path:
    sys.path.insert(0, str(_backend))
