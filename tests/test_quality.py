"""
Tests automatisés — indicateurs de qualité des données.
Lancer avec : pytest tests/ -v
"""
import sys
import os
import pytest
import pandas as pd
import numpy as np

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from quality import (
    completude,
    unicite_file_number,
    taux_doublons_exacts,
    taux_dates_invalides,
    taux_incoherences_temporelles,
    taux_reporting_area_non_conformes,
    audit_qualite,
    VALID_NEIGHBORHOODS,
)
from treatment import appliquer_traitements


# ── Fixtures ─────────────────────────────────────────────────────────────────

@pytest.fixture
def df_propre():
    """Dataset parfaitement propre."""
    return pd.DataFrame({
        "File Number":     ["2020-00001", "2020-00002", "2020-00003"],
        "Date of Report":  ["01/10/2020 10:00:00 AM", "02/15/2020 03:00:00 PM", "03/20/2020 08:00:00 AM"],
        "Crime Date Time": ["01/10/2020 08:00 - 01/10/2020 09:30",
                            "02/15/2020 14:00 - 02/15/2020 14:30",
                            "03/20/2020 07:00"],
        "Crime":           ["Larceny", "Auto Theft", "Vandalism"],
        "Reporting Area":  ["403.0", "201.0", "105.0"],
        "Neighborhood":    ["Cambridgeport", "MIT", "East Cambridge"],
        "Location":        ["1 Main St", "2 Main St", "3 Main St"],
    })


@pytest.fixture
def df_avec_problemes():
    """Dataset avec problèmes connus."""
    return pd.DataFrame({
        "File Number":     ["2020-00001", "2020-00001", "2020-00003", "2020-00004"],
        "Date of Report":  ["01/10/2020 10:00:00 AM", "01/10/2020 10:00:00 AM", "invalid_date", "02/20/2020 09:00:00 AM"],
        "Crime Date Time": ["01/10/2020 08:00", "01/10/2020 08:00", "03/01/2020 10:00", "02/20/2020 10:00"],
        "Crime":           ["Larceny", "Larceny", "Vandalism", None],
        "Reporting Area":  ["403.0", "403.0", "-5", "201.0"],
        "Neighborhood":    ["Cambridgeport", "Cambridgeport", "???", "MIT"],
        "Location":        [None, None, None, None],
    })


# ── Tests de complétude ───────────────────────────────────────────────────────

def test_completude_100_pct(df_propre):
    assert completude(df_propre, "Crime") == 100.0


def test_completude_avec_null(df_avec_problemes):
    # 1 valeur nulle sur 4 = 75%
    assert completude(df_avec_problemes, "Crime") == 75.0


def test_completude_colonne_vide():
    df = pd.DataFrame({"Crime": [None, None, None]})
    assert completude(df, "Crime") == 0.0


# ── Tests d'unicité ───────────────────────────────────────────────────────────

def test_unicite_100_pct(df_propre):
    assert unicite_file_number(df_propre) == 100.0


def test_unicite_avec_doublons(df_avec_problemes):
    # 3 distincts sur 4 = 75%
    assert unicite_file_number(df_avec_problemes) == 75.0


# ── Tests de doublons ─────────────────────────────────────────────────────────

def test_pas_de_doublons(df_propre):
    assert taux_doublons_exacts(df_propre) == 0.0


def test_avec_doublons(df_avec_problemes):
    # 1 doublon exact sur 4 lignes
    assert taux_doublons_exacts(df_avec_problemes) == 25.0


# ── Tests de dates invalides ──────────────────────────────────────────────────

def test_dates_valides(df_propre):
    assert taux_dates_invalides(df_propre) == 0.0


def test_dates_invalides(df_avec_problemes):
    # "invalid_date" sur 4 = 25%
    assert taux_dates_invalides(df_avec_problemes) == 25.0


# ── Tests d'incohérences temporelles ─────────────────────────────────────────

def test_pas_incoherence_temporelle(df_propre):
    assert taux_incoherences_temporelles(df_propre) == 0.0


def test_incoherence_temporelle():
    """Date of Report antérieure au début de Crime Date Time."""
    df = pd.DataFrame({
        "Date of Report":  ["01/05/2020 06:00:00 AM"],   # avant
        "Crime Date Time": ["01/05/2020 08:00 - 01/05/2020 10:00"],  # commence après
    })
    assert taux_incoherences_temporelles(df) == 100.0


# ── Tests de Reporting Area ───────────────────────────────────────────────────

def test_reporting_area_valide(df_propre):
    assert taux_reporting_area_non_conformes(df_propre) == 0.0


def test_reporting_area_invalide():
    df = pd.DataFrame({"Reporting Area": ["-5", "abc", "200.0"]})
    # 2 invalides sur 3 = 66.67%
    result = taux_reporting_area_non_conformes(df)
    assert round(result, 1) == 66.7


# ── Test de la fonction principale d'audit ────────────────────────────────────

def test_audit_qualite_retourne_series(df_propre):
    result = audit_qualite(df_propre)
    assert isinstance(result, pd.Series)
    assert len(result) >= 6


# ── Tests sur le dataset nettoyé ─────────────────────────────────────────────

def test_traitement_supprime_doublons(df_avec_problemes):
    df_clean = appliquer_traitements(df_avec_problemes)
    assert df_clean.duplicated().sum() == 0


def test_traitement_supprime_crime_null(df_avec_problemes):
    df_clean = appliquer_traitements(df_avec_problemes)
    assert df_clean["Crime"].isnull().sum() == 0


def test_traitement_neighborhood_valide(df_avec_problemes):
    df_clean = appliquer_traitements(df_avec_problemes)
    assert df_clean["Neighborhood"].dropna().isin(VALID_NEIGHBORHOODS).all()


def test_traitement_reporting_area_group_positif(df_propre):
    df_clean = appliquer_traitements(df_propre)
    assert (df_clean["reporting_area_group"].dropna() > 0).all()


def test_traitement_reporting_area_group_raisonnable(df_propre):
    df_clean = appliquer_traitements(df_propre)
    assert (df_clean["reporting_area_group"].dropna() <= 20).all()


def test_dataset_reel_charge():
    """Vérifie que le dataset brut se charge correctement."""
    base = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    path = os.path.join(base, "data", "crime_reports_broken.csv")
    if not os.path.exists(path):
        pytest.skip("Dataset non disponible")
    df = pd.read_csv(path)
    assert len(df) > 10000
    assert "File Number" in df.columns
    assert "Crime" in df.columns
    assert "Neighborhood" in df.columns
