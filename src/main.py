"""
TP1 — Qualité des données : Criminalité à Cambridge
Pipeline principale : profilage → audit → traitement → monitoring → cartographie.
"""
import logging
import os
import sys
import pandas as pd

# ── Configuration du logging ─────────────────────────────────────────────────
os.makedirs("logs", exist_ok=True)
logging.basicConfig(
    filename="logs/app.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    encoding="utf-8",
)
# Afficher aussi dans la console (niveau INFO)
console = logging.StreamHandler(sys.stdout)
console.setLevel(logging.WARNING)
logging.getLogger("").addHandler(console)

logger = logging.getLogger(__name__)

# ── Chemins ──────────────────────────────────────────────────────────────────
BASE_DIR      = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR      = os.path.join(BASE_DIR, "data")
OUTPUT_DIR    = os.path.join(BASE_DIR, "output")
RAW_CSV       = os.path.join(DATA_DIR, "crime_reports_broken.csv")
CLEAN_CSV     = os.path.join(OUTPUT_DIR, "crime_reports_clean.csv")
GEOJSON_PATH  = os.path.join(DATA_DIR, "BOUNDARY_CDDNeighborhoods.geojson")
MAP_HTML      = os.path.join(OUTPUT_DIR, "map.html")

os.makedirs(OUTPUT_DIR, exist_ok=True)

from profiling import run_profiling
from quality   import audit_qualite, afficher_audit
from treatment import appliquer_traitements, exporter_csv
from mapping   import produire_carte


def main():
    logger.info("========== DÉMARRAGE DU PIPELINE TP1 ==========")

    # ── Section 1 : Profilage ────────────────────────────────────────────────
    df_raw = pd.read_csv(RAW_CSV)
    run_profiling(df_raw)

    # ── Section 2 : Audit initial ────────────────────────────────────────────
    audit_initial = audit_qualite(df_raw)
    afficher_audit(audit_initial, label="AVANT NETTOYAGE")

    # ── Section 3 : Traitement ───────────────────────────────────────────────
    print(f"\n{'='*60}")
    print("TRAITEMENT — Règles de nettoyage")
    print(f"{'='*60}")
    df_clean = appliquer_traitements(df_raw)
    exporter_csv(df_clean, CLEAN_CSV)

    # ── Section 4 : Monitoring post-nettoyage ────────────────────────────────
    audit_clean = audit_qualite(df_clean)
    afficher_audit(audit_clean, label="APRÈS NETTOYAGE")

    print(f"\n{'='*60}")
    print("COMPARAISON AVANT / APRÈS")
    print(f"{'='*60}")
    comparison = pd.DataFrame({
        "Avant": audit_initial,
        "Après": audit_clean,
        "Δ": (audit_clean - audit_initial).round(2),
    })
    print(comparison.to_string())

    significatifs = comparison[comparison["Δ"].abs() >= 1.0]
    if not significatifs.empty:
        print(f"\n⚡ Indicateurs avec évolution significative (|Δ| ≥ 1%) :")
        for idx, row in significatifs.iterrows():
            direction = "↑" if row["Δ"] > 0 else "↓"
            print(f"   {direction} {idx}: {row['Avant']:.2f}% → {row['Après']:.2f}% (Δ={row['Δ']:+.2f}%)")

    # ── Section 5 : Cartographie ─────────────────────────────────────────────
    print(f"\n{'='*60}")
    print("CARTOGRAPHIE — Choroplèthe crimes par quartier")
    print(f"{'='*60}")
    produire_carte(CLEAN_CSV, GEOJSON_PATH, MAP_HTML)

    logger.info("========== PIPELINE TERMINÉ ==========")
    print(f"\n✅ Pipeline terminé. Fichiers dans output/")


if __name__ == "__main__":
    main()
