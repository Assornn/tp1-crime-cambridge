"""
Section 5 — Cartographie choroplèthe des crimes par quartier.
"""
import logging
import json
import pandas as pd
import geopandas as gpd
import folium
from folium.features import GeoJsonTooltip

logger = logging.getLogger(__name__)

# Correspondance entre les noms du référentiel CSV et les noms du GeoJSON
# (le GeoJSON utilise des noms légèrement différents)
NEIGHBORHOOD_TO_GEOJSON_NAME = {
    "Area 4":           "The Port",
    "Cambridgeport":    "Cambridgeport",
    "East Cambridge":   "East Cambridge",
    "Mid-Cambridge":    "Mid-Cambridge",
    "North Cambridge":  "North Cambridge",
    "Riverside":        "Riverside",
    "West Cambridge":   "West Cambridge",
    "Peabody":          "Neighborhood Nine",        # Peabody = Neighborhood Nine
    "Inman/Harrington": "Wellington-Harrington",    # correspondance approx.
    "Highlands":        "Cambridge Highlands",
    "Agassiz":          "Agassiz",
    "MIT":              "Area 2/MIT",
    "Strawberry Hill":  "Strawberry Hill",
}


def produire_carte(
    clean_csv: str,
    geojson_path: str,
    output_html: str,
) -> None:
    """
    Produit la carte choroplèthe et l'exporte en HTML.
    """
    logger.info("=== SECTION 5 — Cartographie ===")

    # ── Agrégation des crimes par quartier ───────────────────────────────────
    df = pd.read_csv(clean_csv)
    crimes_par_quartier = (
        df.groupby("Neighborhood")
        .size()
        .reset_index(name="nb_crimes")
    )

    # Vérification : somme == nb lignes (hors NaN Neighborhood — déjà nettoyé)
    total_agg = crimes_par_quartier["nb_crimes"].sum()
    total_df = len(df)
    assert total_agg == total_df, (
        f"Incohérence agrégation : {total_agg} ≠ {total_df}"
    )
    print(f"  ✅ Agrégation cohérente : {total_agg:,} crimes répartis sur "
          f"{len(crimes_par_quartier)} quartiers")

    # Mapping vers noms GeoJSON
    crimes_par_quartier["geojson_name"] = (
        crimes_par_quartier["Neighborhood"]
        .map(NEIGHBORHOOD_TO_GEOJSON_NAME)
    )

    # ── Chargement du GeoJSON ────────────────────────────────────────────────
    gdf = gpd.read_file(geojson_path)
    print(f"  GeoJSON chargé : {len(gdf)} entités")
    print(f"  Colonnes : {list(gdf.columns)}")

    # ── Jointure ─────────────────────────────────────────────────────────────
    merged = gdf.merge(
        crimes_par_quartier[["geojson_name", "nb_crimes", "Neighborhood"]],
        left_on="NAME",
        right_on="geojson_name",
        how="left",
    )

    # Vérification des orphelins
    orphelins_sans_crimes = merged[merged["nb_crimes"].isna()]["NAME"].tolist()
    if orphelins_sans_crimes:
        logger.warning("Quartiers GeoJSON sans crimes : %s", orphelins_sans_crimes)
        print(f"  ⚠️  Quartiers sans crimes (non mappés) : {orphelins_sans_crimes}")
    else:
        print("  ✅ Aucun quartier orphelin")

    # Remplir les NaN restants à 0
    merged["nb_crimes"] = merged["nb_crimes"].fillna(0).astype(int)

    # ── Carte Folium ─────────────────────────────────────────────────────────
    m = folium.Map(
        location=[42.373, -71.109],
        zoom_start=13,
        tiles="CartoDB positron",
    )

    choropleth = folium.Choropleth(
        geo_data=merged.__geo_interface__,
        data=merged,
        columns=["NAME", "nb_crimes"],
        key_on="feature.properties.NAME",
        fill_color="RdYlGn_r",   # vert (peu) → rouge (beaucoup)
        fill_opacity=0.75,
        line_opacity=0.4,
        legend_name="Nombre de crimes",
        highlight=True,
        name="Crimes par quartier",
    )
    choropleth.add_to(m)

    # Tooltip au survol
    tooltip_layer = folium.GeoJson(
        merged.__geo_interface__,
        name="Quartiers",
        style_function=lambda x: {
            "fillOpacity": 0,
            "color": "transparent",
            "weight": 0,
        },
        highlight_function=lambda x: {
            "weight": 2,
            "color": "#333",
            "fillOpacity": 0.1,
        },
        tooltip=GeoJsonTooltip(
            fields=["NAME", "nb_crimes"],
            aliases=["Quartier :", "Nombre de crimes :"],
            localize=True,
            sticky=False,
            labels=True,
            style=(
                "background-color: white; color: #333; "
                "font-family: Arial; font-size: 13px; padding: 6px;"
            ),
        ),
    )
    tooltip_layer.add_to(m)

    folium.LayerControl().add_to(m)

    m.save(output_html)
    logger.info("Carte exportée → %s", output_html)
    print(f"\n  🗺️  Carte exportée → {output_html}")
