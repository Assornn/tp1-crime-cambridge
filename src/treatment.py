"""
Section 3 — Traitement et nettoyage des données.
Logique alignée sur la correction officielle.
"""
import logging
import pandas as pd
import numpy as np

logger = logging.getLogger(__name__)

VALID_NEIGHBORHOODS = {
    "Cambridgeport", "East Cambridge", "Mid-Cambridge", "North Cambridge",
    "Riverside", "Area 4", "West Cambridge", "Peabody", "Inman/Harrington",
    "Highlands", "Agassiz", "MIT", "Strawberry Hill",
}

VALID_GROUP_MIN, VALID_GROUP_MAX = 1, 13


def parse_report_datetime(s):
    return pd.to_datetime(s, errors="coerce", format="%m/%d/%Y %I:%M:%S %p")


def extract_crime_start_datetime(s):
    start = s.astype("string").str.split(" - ").str[0].str.strip()
    dt = pd.to_datetime(start, errors="coerce", format="%m/%d/%Y %H:%M")
    return dt.fillna(pd.to_datetime(start, errors="coerce"))


def extract_reporting_area_group(series):
    num = pd.to_numeric(series, errors="coerce")
    return (num // 100).astype("Int64")


def appliquer_traitements(df_original):
    df = df_original.copy()
    initial_len = len(df)
    logger.info("Debut du traitement — %d lignes initiales", initial_len)

    # 1. Doublons sur File Number
    n_before = len(df)
    df = df.drop_duplicates(subset=["File Number"], keep="first").reset_index(drop=True)
    n_removed = n_before - len(df)
    logger.info("Doublons File Number supprimes : %d", n_removed)
    print(f"  [1] Doublons File Number supprimes     : {n_removed:>5} lignes")

    # 2. Crime null
    n_before = len(df)
    df = df.dropna(subset=["Crime"]).reset_index(drop=True)
    n_removed = n_before - len(df)
    logger.info("Lignes sans crime supprimees : %d", n_removed)
    print(f"  [2] Lignes sans Crime supprimees       : {n_removed:>5} lignes")

    # 3. Date of Report invalide -> NaN
    report_dt = parse_report_datetime(df["Date of Report"])
    n_invalid = report_dt.isna().sum()
    df.loc[report_dt.isna(), "Date of Report"] = np.nan
    print(f"  [3] Dates invalides -> NaN             : {n_invalid:>5} lignes")

    # 4. Incoherence temporelle -> NaN
    crime_dt = extract_crime_start_datetime(df["Crime Date Time"])
    report_dt = parse_report_datetime(df["Date of Report"])
    mask = report_dt.notna() & crime_dt.notna() & (report_dt < crime_dt)
    df.loc[mask, "Date of Report"] = np.nan
    print(f"  [4] Incoherences temporelles -> NaN    : {int(mask.sum()):>5} lignes")

    # 5. Imputation Date of Report depuis File Number
    mask_missing = df["Date of Report"].isna()
    year = df.loc[mask_missing, "File Number"].astype(str).str[:4]
    imputed = pd.to_datetime(year + "-12-31 23:59:59", errors="coerce")
    df.loc[mask_missing, "Date of Report"] = imputed.dt.strftime("%m/%d/%Y %I:%M:%S %p")
    print(f"  [5] Dates imputees (31/12/YYYY)        : {int(mask_missing.sum()):>5} lignes")

    # 6. Reporting Area invalide -> NaN
    area_num = pd.to_numeric(df["Reporting Area"], errors="coerce")
    n_invalid = area_num.isna().sum()
    df.loc[area_num.isna(), "Reporting Area"] = pd.NA
    df["Reporting Area"] = pd.to_numeric(df["Reporting Area"], errors="coerce")
    print(f"  [6] Reporting Area invalides -> NaN    : {n_invalid:>5} lignes")

    # 7. Neighborhood hors referentiel -> NaN
    df["Neighborhood"] = df["Neighborhood"].astype("string").str.strip()
    invalid_hood = ~df["Neighborhood"].isin(VALID_NEIGHBORHOODS)
    n_invalid = invalid_hood.sum()
    df.loc[invalid_hood, "Neighborhood"] = pd.NA
    print(f"  [7] Neighborhoods invalides -> NaN     : {n_invalid:>5} lignes")

    # 8. reporting_area_group
    df["reporting_area_group"] = extract_reporting_area_group(df["Reporting Area"])
    invalid_group = (
        df["reporting_area_group"].isna() |
        (df["reporting_area_group"] < VALID_GROUP_MIN) |
        (df["reporting_area_group"] > VALID_GROUP_MAX)
    )
    n_aberrant = int(invalid_group.sum())
    df.loc[invalid_group, "reporting_area_group"] = pd.NA
    print(f"  [8] reporting_area_group aberrants     : {n_aberrant:>5} lignes")

    # 9. Imputation Neighborhood via reporting_area_group
    group_to_neighborhood = (
        df.dropna(subset=["reporting_area_group", "Neighborhood"])
        .groupby("reporting_area_group")["Neighborhood"]
        .agg(lambda s: s.value_counts().idxmax())
        .to_dict()
    )
    mask_missing_neigh = df["Neighborhood"].isna() & df["reporting_area_group"].notna()
    df.loc[mask_missing_neigh, "Neighborhood"] = (
        df.loc[mask_missing_neigh, "reporting_area_group"].map(group_to_neighborhood)
    )
    print(f"  [9] Neighborhoods imputes              : {int(mask_missing_neigh.sum()):>5} lignes")

    final_len = len(df)
    total_removed = initial_len - final_len
    print(f"\n  Dataset final : {final_len:,} lignes ({total_removed:,} supprimees, {total_removed/initial_len*100:.1f}%)")
    return df


def exporter_csv(df, path):
    df.to_csv(path, index=False)
    logger.info("Dataset exporte -> %s", path)
    print(f"\n  Export : {path}")
