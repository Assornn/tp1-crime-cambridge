"""
Section 3 — Traitement et nettoyage des données.
"""
import logging
import pandas as pd
import numpy as np

from quality import (
    VALID_NEIGHBORHOODS,
    _parse_date,
    _extract_crime_start,
    _is_reporting_area_valid,
)

logger = logging.getLogger(__name__)


def appliquer_traitements(df_original: pd.DataFrame) -> pd.DataFrame:
    """
    Applique l'ensemble des règles de traitement sur une copie du dataframe.
    Retourne le dataframe nettoyé.
    """
    df = df_original.copy()
    initial_len = len(df)
    logger.info("Début du traitement — %d lignes initiales", initial_len)

    # ── 1. Doublons exacts ──────────────────────────────────────────────────
    # Décision : on conserve la première occurrence, les autres sont supprimées.
    # Justification : un dossier de crime ne peut pas être enregistré deux fois
    #                 avec exactement les mêmes données ; doublon = double saisie.
    n_before = len(df)
    df = df.drop_duplicates(keep="first").reset_index(drop=True)
    n_removed = n_before - len(df)
    logger.info("Doublons exacts supprimés : %d lignes", n_removed)
    print(f"  [1] Doublons exacts supprimés          : {n_removed:>5} lignes")

    # ── 2. Crime null ────────────────────────────────────────────────────────
    # Décision : suppression des lignes sans type de crime.
    # Justification : sans catégorie de crime, la ligne est inexploitable pour
    #                 tout indicateur criminel (carte, statistiques, …).
    n_before = len(df)
    df = df.dropna(subset=["Crime"]).reset_index(drop=True)
    n_removed = n_before - len(df)
    logger.info("Lignes sans crime supprimées : %d", n_removed)
    print(f"  [2] Lignes sans 'Crime' supprimées     : {n_removed:>5} lignes")

    # ── 3. Date of Report invalide ───────────────────────────────────────────
    # Décision : suppression des lignes avec date non parsable.
    # Justification : la date de signalement est une dimension d'analyse
    #                 temporelle essentielle ; on ne peut pas l'inférer.
    df["_report_dt"] = df["Date of Report"].apply(_parse_date)
    n_before = len(df)
    df = df[df["_report_dt"].notna()].reset_index(drop=True)
    n_removed = n_before - len(df)
    logger.info("Dates de report invalides supprimées : %d", n_removed)
    print(f"  [3] Dates de report invalides supp.    : {n_removed:>5} lignes")

    # ── 4. Date of Report antérieure au début de Crime Date Time ─────────────
    # Décision : suppression.
    # Justification : un crime ne peut pas être signalé avant d'avoir commencé ;
    #                 incohérence logique irréparable sans source supplémentaire.
    df["_crime_start"] = df["Crime Date Time"].apply(_extract_crime_start)
    both_valid = df["_report_dt"].notna() & df["_crime_start"].notna()
    incoherent = (df["_report_dt"] < df["_crime_start"]) & both_valid
    n_before = len(df)
    df = df[~incoherent].reset_index(drop=True)
    n_removed = n_before - len(df)
    logger.info("Incohérences temporelles supprimées : %d", n_removed)
    print(f"  [4] Incohérences temporelles supp.     : {n_removed:>5} lignes")

    # Nettoyage colonnes temporaires de travail
    df = df.drop(columns=["_report_dt", "_crime_start"])

    # ── 5. Reporting Area invalide ───────────────────────────────────────────
    # Décision : mise à NaN des valeurs invalides, puis suppression des lignes.
    # Justification : une zone négative ou non numérique est une erreur de saisie ;
    #                 on ne peut pas inférer la valeur correcte.
    invalid_area = ~df["Reporting Area"].apply(_is_reporting_area_valid)
    n_invalid = invalid_area.sum()
    df.loc[invalid_area, "Reporting Area"] = np.nan
    n_before = len(df)
    df = df.dropna(subset=["Reporting Area"]).reset_index(drop=True)
    n_removed = n_before - len(df)
    logger.info("Reporting Area invalides mis à NaN puis supprimés : %d", n_invalid)
    print(f"  [5] Reporting Area invalides supp.     : {n_removed:>5} lignes")

    # Conversion en int (maintenant sûr)
    df["Reporting Area"] = df["Reporting Area"].apply(lambda x: int(float(x)))

    # ── 6. Neighborhood invalide ─────────────────────────────────────────────
    # Décision : mise à NaN des valeurs hors référentiel, puis suppression.
    # Justification : seuls les 13 quartiers du référentiel officiel ont une
    #                 signification cartographique exploitable.
    invalid_hood = ~df["Neighborhood"].isin(VALID_NEIGHBORHOODS)
    n_invalid = invalid_hood.sum()
    df.loc[invalid_hood, "Neighborhood"] = np.nan
    n_before = len(df)
    df = df.dropna(subset=["Neighborhood"]).reset_index(drop=True)
    n_removed = n_before - len(df)
    logger.warning("Neighborhoods invalides mis à NaN puis supprimés : %d", n_invalid)
    print(f"  [6] Neighborhoods invalides supp.      : {n_removed:>5} lignes")

    # ── 7. Enrichissement : reporting_area_group ──────────────────────────────
    # Extraction du groupe de centaines (ex : 602 → 6 ; 1109 → 11)
    df["reporting_area_group"] = df["Reporting Area"] // 100

    # Vérification des valeurs aberrantes (négatif ou > 20 = invraisemblable)
    aberrant = (df["reporting_area_group"] <= 0) | (df["reporting_area_group"] > 20)
    n_aberrant = aberrant.sum()
    if n_aberrant > 0:
        logger.warning("reporting_area_group aberrants neutralisés : %d", n_aberrant)
        df.loc[aberrant, "reporting_area_group"] = np.nan
    print(f"  [7] reporting_area_group créé — aberrants neutralisés : {n_aberrant}")

    final_len = len(df)
    total_removed = initial_len - final_len
    print(f"\n  ✅ Dataset nettoyé : {final_len:,} lignes conservées "
          f"({total_removed:,} supprimées, {total_removed/initial_len*100:.1f}%)")

    logger.info("Traitement terminé — %d lignes finales (%d supprimées)",
                final_len, total_removed)
    return df


def exporter_csv(df: pd.DataFrame, path: str) -> None:
    """Exporte le dataset nettoyé en CSV."""
    df.to_csv(path, index=False)
    logger.info("Dataset exporté → %s", path)
    print(f"\n  💾 Export : {path}")
