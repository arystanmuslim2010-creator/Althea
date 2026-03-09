from __future__ import annotations

import os

import pandas as pd

from src import config
from src.synth_data import generate_synthetic_transactions


def main() -> None:
    n_users = 1000
    tx_per_user = 50
    df = generate_synthetic_transactions(n_users=n_users, tx_per_user=tx_per_user, cfg=config)
    out_dir = os.path.join("data")
    os.makedirs(out_dir, exist_ok=True)
    out_path = os.path.join(out_dir, "demo_synth.csv")
    df.to_csv(out_path, index=False)
    print(f"Wrote {len(df)} rows to {out_path}")


if __name__ == "__main__":
    main()
