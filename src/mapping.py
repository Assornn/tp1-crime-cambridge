"""
Section 5 — Cartographie choroplethe des crimes par quartier.
Jointure par reporting_area_group <-> N_HOOD du GeoJSON.
"""
import logging
import pandas as pd
import geopandas as gpd
import folium
from folium.features import GeoJsonTooltip

logger = logging.getLogger(__name__)


def produire_carte(clean_csv, geojson_path, output_html):
    logger.info("=== SECTION 5 — Cartographie ===")

    df = pd.read_csv(clean_csv)

    # Agregation par reporting_area_group (jointure cle GeoJSON)
    crimes_by_code = (
        df.dropna(subset=["reporting_area_group"])
        .groupby("reporting_area_group")
        .size()
        .reset_index(name="crime_count")
    )

    # Verification coherence
    crimes_by_neigh = (
        df.dropna(subset=["Neighborhood"])
        .groupby("Neighborhood")
        .size()
        .reset_index(name="nb_crimes")
        .sort_values("nb_crimes", ascending=False)
    )
    print(f"  Crimes par quartier (Neighborhood):")
    print(crimes_by_neigh.to_string(index=False))

    n_rows = df["Neighborhood"].notna().sum()
    n_agg  = crimes_by_neigh["nb_crimes"].sum()
    assert n_rows == n_agg, f"Incoherence : {n_rows} != {n_agg}"
    print(f"\n  Agregation coherente : {n_agg:,} crimes")

    # GeoJSON
    gdf = gpd.read_file(geojson_path)
    gdf["N_HOOD"] = pd.to_numeric(gdf["N_HOOD"], errors="coerce").astype("Int64")

    # Jointure par N_HOOD <-> reporting_area_group
    gdf_map = gdf.merge(crimes_by_code, left_on="N_HOOD", right_on="reporting_area_group", how="left")
    gdf_map["crime_count"] = gdf_map["crime_count"].fillna(0).astype(int)
    gdf_map = gdf_map.to_crs(epsg=4326)

    # Orphelins
    orphelins = gdf_map[gdf_map["crime_count"] == 0]["NAME"].tolist()
    if orphelins:
        logger.warning("Quartiers sans crimes : %s", orphelins)
        print(f"  Quartiers sans crimes : {orphelins}")
    else:
        print("  Aucun quartier orphelin")

    print("\n  Carte GeoJSON:")
    print(gdf_map[["NAME","N_HOOD","crime_count"]].sort_values("crime_count", ascending=False).to_string(index=False))

    # Carte Folium
    center = gdf_map.geometry.union_all().centroid
    m = folium.Map(location=[center.y, center.x], zoom_start=13, tiles="cartodbpositron")

    folium.Choropleth(
        geo_data=gdf_map,
        data=gdf_map,
        columns=["N_HOOD", "crime_count"],
        key_on="feature.properties.N_HOOD",
        fill_color="RdYlGn_r",
        fill_opacity=0.75,
        line_opacity=0.3,
        legend_name="Nombre de crimes par quartier",
        highlight=True,
    ).add_to(m)

    folium.GeoJson(
        gdf_map,
        tooltip=GeoJsonTooltip(
            fields=["NAME", "crime_count"],
            aliases=["Quartier :", "Crimes :"],
            localize=True,
        )
    ).add_to(m)

    m.save(output_html)
    logger.info("Carte exportee -> %s", output_html)
    print(f"\n  Carte exportee -> {output_html}")
