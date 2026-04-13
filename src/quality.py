"""
Section 2 — Audit de la qualité des données.
Fonctions de calcul d'indicateurs + fonction principale d'audit.
"""
import logging
import re
import pandas as pd

logger = logging.getLogger(__name__)

VALID_NEIGHBORHOODS = {
    "Cambridgeport",
    "East Cambridge",
    "Mid-Cambridge",
    "North Cambridge",
    "Riverside",
    "Area 4",
    "West Cambridge",
    "Peabody",
    "Inman/Harrington",
    "Highlands",
    "Agassiz",
    "MIT",
    "Strawberry Hill",
}

# Seuils d'acceptation
THRESHOLDS = {
    "completude_crime": 95.0,
    "completude_neighborhood": 95.0,
    "completude_file_number": 99.0,
    "unicite_file_number": 99.0,
    "taux_doublons_exacts": 1.0,       # seuil max acceptable
    "taux_dates_invalides": 1.0,        # seuil max acceptable
    "taux_incoherences_temporelles": 1.0,
    "taux_reporting_area_non_conformes": 1.0,
}


# ── Indicateurs individuels ──────────────────────────────────────────────────

def completude(df: pd.DataFrame, col: str) -> float:
    """% de valeurs non nulles dans une colonne."""
    return round((1 - df[col].isnull().mean()) * 100, 2)


def unicite_file_number(df: pd.DataFrame) -> float:
    """% de valeurs uniques dans File Number (parmi les non-nulles)."""
    s = df["File Number"].dropna()
    if len(s) == 0:
        return 0.0
    return round(s.nunique() / len(s) * 100, 2)


def taux_doublons_exacts(df: pd.DataFrame) -> float:
    """% de lignes strictement dupliquées."""
    return round(df.duplicated().sum() / len(df) * 100, 2)


def _parse_date(s: str):
    """Tente de parser une chaîne en datetime, retourne NaT si échec."""
    try:
        return pd.to_datetime(s, format="%m/%d/%Y %I:%M:%S %p")
    except Exception:
        try:
            return pd.to_datetime(s, infer_datetime_format=True)
        except Exception:
            return pd.NaT


def taux_dates_invalides(df: pd.DataFrame) -> float:
    """% de dates non parsables dans Date of Report."""
    parsed = df["Date of Report"].apply(_parse_date)
    return round(parsed.isnull().mean() * 100, 2)


def _extract_crime_start(crime_dt_str: str):
    """Extrait la date de début depuis Crime Date Time (plage ou instant)."""
    if pd.isna(crime_dt_str):
        return pd.NaT
    # Format plage : "MM/DD/YYYY HH:MM - MM/DD/YYYY HH:MM"
    parts = str(crime_dt_str).split(" - ")
    start_str = parts[0].strip()
    try:
        return pd.to_datetime(start_str, format="%m/%d/%Y %H:%M")
    except Exception:
        try:
            return pd.to_datetime(start_str, infer_datetime_format=True)
        except Exception:
            return pd.NaT


def taux_incoherences_temporelles(df: pd.DataFrame) -> float:
    """% de lignes où Date of Report < début de Crime Date Time."""
    report = df["Date of Report"].apply(_parse_date)
    crime_start = df["Crime Date Time"].apply(_extract_crime_start)
    both_valid = report.notna() & crime_start.notna()
    incoherent = (report < crime_start) & both_valid
    return round(incoherent.sum() / len(df) * 100, 2)


def _is_reporting_area_valid(val) -> bool:
    """Vérifie qu'une Reporting Area est un entier positif."""
    if pd.isna(val):
        return False  # NaN = invalide
    try:
        num = float(str(val).strip())
        return num > 0 and num == int(num)
    except (ValueError, TypeError):
        return False


def taux_reporting_area_non_conformes(df: pd.DataFrame) -> float:
    """% de valeurs non conformes dans Reporting Area (hors NaN)."""
    # On calcule sur les lignes ayant une valeur (NaN géré séparément par complétude)
    s = df["Reporting Area"].dropna()
    if len(s) == 0:
        return 0.0
    invalid = ~s.apply(_is_reporting_area_valid)
    return round(invalid.sum() / len(df) * 100, 2)


# ── Fonction principale ──────────────────────────────────────────────────────

def audit_qualite(df: pd.DataFrame) -> pd.Series:
    """
    Calcule l'ensemble des indicateurs de qualité et retourne un pd.Series synthétique.
    """
    indicators = {
        "completude_file_number (%)":        completude(df, "File Number"),
        "completude_crime (%)":              completude(df, "Crime"),
        "completude_neighborhood (%)":       completude(df, "Neighborhood"),
        "unicite_file_number (%)":           unicite_file_number(df),
        "taux_doublons_exacts (%)":          taux_doublons_exacts(df),
        "taux_dates_invalides (%)":          taux_dates_invalides(df),
        "taux_incoherences_temporelles (%)": taux_incoherences_temporelles(df),
        "taux_reporting_area_non_conformes (%)": taux_reporting_area_non_conformes(df),
    }
    return pd.Series(indicators)


def afficher_audit(series: pd.Series, label: str = "Dataset") -> None:
    """Affiche les résultats d'audit avec comparaison aux seuils."""
    print(f"\n{'='*60}")
    print(f"AUDIT QUALITÉ — {label}")
    print(f"{'='*60}")

    seuils_max = {
        "taux_doublons_exacts (%)": THRESHOLDS["taux_doublons_exacts"],
        "taux_dates_invalides (%)": THRESHOLDS["taux_dates_invalides"],
        "taux_incoherences_temporelles (%)": THRESHOLDS["taux_incoherences_temporelles"],
        "taux_reporting_area_non_conformes (%)": THRESHOLDS["taux_reporting_area_non_conformes"],
    }
    seuils_min = {
        "completude_crime (%)": THRESHOLDS["completude_crime"],
        "completude_neighborhood (%)": THRESHOLDS["completude_neighborhood"],
        "completude_file_number (%)": THRESHOLDS["completude_file_number"],
        "unicite_file_number (%)": THRESHOLDS["unicite_file_number"],
    }

    for indicateur, valeur in series.items():
        if indicateur in seuils_min:
            seuil = seuils_min[indicateur]
            status = "✅" if valeur >= seuil else "❌"
            print(f"  {status} {indicateur:<45} {valeur:>6.2f}%  (seuil ≥ {seuil}%)")
        elif indicateur in seuils_max:
            seuil = seuils_max[indicateur]
            status = "✅" if valeur <= seuil else "❌"
            print(f"  {status} {indicateur:<45} {valeur:>6.2f}%  (seuil ≤ {seuil}%)")
        else:
            print(f"     {indicateur:<45} {valeur:>6.2f}%")
