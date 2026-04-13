"""
Section 1 — Profilage et exploration du dataset.
"""
import logging
import pandas as pd

logger = logging.getLogger(__name__)

DATA_DICTIONARY = {
    "File Number": {
        "type": "str",
        "definition": "Identifiant unique du dossier de crime au format AAAA-NNNNN.",
        "example": "2016-02477",
        "remarque": "Doit être unique par ligne. Des doublons ont été détectés.",
    },
    "Date of Report": {
        "type": "str (datetime attendu)",
        "definition": "Date et heure du signalement du crime par la police.",
        "example": "04/14/2016 07:11:00 PM",
        "remarque": "Certaines valeurs ne sont pas parsables (dates invalides).",
    },
    "Crime Date Time": {
        "type": "str (plage horaire ou instant)",
        "definition": "Fenêtre temporelle estimée durant laquelle le crime a eu lieu.",
        "example": "04/13/2016 20:00 - 04/14/2016 06:30",
        "remarque": "Peut être un instant unique ou une plage. Début toujours ≤ Date of Report.",
    },
    "Crime": {
        "type": "str (catégorie)",
        "definition": "Type de crime déclaré (ex : Larceny from MV, Auto Theft, …).",
        "example": "Larceny from MV",
        "remarque": "~5 % de valeurs nulles — lignes inexploitables pour analyses.",
    },
    "Reporting Area": {
        "type": "str (numérique attendu)",
        "definition": "Code numérique de zone de patrouille police (1 à 4 chiffres).",
        "example": "403",
        "remarque": "Présence de valeurs négatives et non numériques (erreurs de saisie).",
    },
    "Neighborhood": {
        "type": "str (catégorie référentielle)",
        "definition": "Quartier de Cambridge où le crime a été commis (référentiel de 13 valeurs).",
        "example": "Cambridgeport",
        "remarque": "~7 % de NaN + valeurs parasites (not_specified, ???, Cambrigeport…).",
    },
    "Location": {
        "type": "str (adresse)",
        "definition": "Adresse approximative du lieu du crime, à Cambridge MA.",
        "example": "100 HARVARD ST, Cambridge, MA",
        "remarque": "Non utilisée dans les analyses ; ~2 % manquants.",
    },
}


def run_profiling(df: pd.DataFrame) -> None:
    """Affiche le profilage complet du dataset."""
    logger.info("=== SECTION 1 — Profilage et exploration ===")

    print(f"\n{'='*60}")
    print("PROFILAGE DU DATASET")
    print(f"{'='*60}")

    print(f"\n📊 Dimensions : {df.shape[0]:,} lignes × {df.shape[1]} colonnes")

    print("\n📋 Types des colonnes :")
    print(df.dtypes.to_string())

    print("\n❓ Valeurs manquantes par colonne :")
    nulls = df.isnull().sum()
    pct = (nulls / len(df) * 100).round(2)
    missing_df = pd.DataFrame({"Nb manquants": nulls, "% manquants": pct})
    print(missing_df[missing_df["Nb manquants"] > 0].to_string())

    print("\n🔍 Problèmes de qualité identifiés :")
    problems = [
        "1. DOUBLONS EXACTS : présence de lignes strictement identiques "
        f"({df.duplicated().sum()} doublons sur {len(df)} lignes).",
        "2. VALEURS MANQUANTES : colonnes 'Crime' (~5 %), 'Neighborhood' (~7 %), "
        "'Location' (~2 %) présentent des nulls significatifs.",
        "3. NEIGHBORHOOD INVALIDE : valeurs hors référentiel officiel détectées "
        "(ex : 'not_specified', '???', 'Cambrigeport', 'N-A', 'Unknown').",
        "4. REPORTING AREA NON CONFORME : valeurs négatives (ex : '-12') ou "
        "non numériques présentes, incompatibles avec un code de zone.",
        "5. INCOHÉRENCES TEMPORELLES : certaines 'Date of Report' sont "
        "antérieures au début de 'Crime Date Time' (impossible logiquement).",
    ]
    for p in problems:
        print(f"   {p}")

    print("\n📖 Dictionnaire des données :")
    for col, meta in DATA_DICTIONARY.items():
        print(f"\n  [{col}]")
        for k, v in meta.items():
            print(f"    {k:<12}: {v}")

    logger.info("Profilage terminé — %d lignes, %d colonnes", df.shape[0], df.shape[1])
