# Download CZ / Open Asset Pricing firm-level signal values (exact published
# signal panels) for the valuation-beta analysis (4c18).
# Saves one parquet per signal to ../Data/Raw/CZ_FirmSignals/
#
# Usage: python3 4c18a_DownloadFirmSignals.py

import os
import sys
import time

import pandas as pd
import openassetpricing as oap

OUT_DIR = "../Data/Raw/CZ_FirmSignals"
os.makedirs(OUT_DIR, exist_ok=True)

# All CZ predictors from the repo's signal doc (212 signals)
doc = pd.read_csv("../Data/Raw/SignalDoc.csv")
doc = doc[doc["Cat.Signal"] == "Predictor"]
signals = sorted(doc["Acronym"].dropna().unique())
print(f"{len(signals)} predictor signals in SignalDoc")

o = oap.OpenAP()

failed = []
for i, s in enumerate(signals):
    fout = os.path.join(OUT_DIR, f"{s}.parquet")
    if os.path.exists(fout):
        continue
    try:
        t0 = time.time()
        df = o.dl_signal("pandas", [s])
        df.to_parquet(fout, index=False)
        print(f"[{i+1}/{len(signals)}] {s}: {df.shape[0]:,} rows "
              f"({time.time()-t0:.0f}s)", flush=True)
    except Exception as e:
        failed.append(s)
        print(f"[{i+1}/{len(signals)}] {s}: FAILED ({e})", flush=True)

print(f"\nDone. {len(failed)} failures: {failed}")
sys.exit(1 if failed else 0)
