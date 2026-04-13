# TP1 — Audit et qualité des données
## Criminalité à Cambridge (2009–2024)

---

## Contexte et objectif

Consultant pour le service « Sécurité » de la ville de Cambridge, ce projet réalise un audit complet
de qualité des données de crimes déclarés entre 2009 et 2024. L'objectif est d'évaluer la fiabilité
du dataset, d'appliquer des règles de traitement, puis de produire une carte choroplèthe des crimes
par quartier à destination du maire.

---

## Jeu de données

| Fichier | Description |
|---|---|
| `data/crime_reports_broken.csv` | Dataset brut (~10 500 lignes, 7 colonnes) |
| `data/BOUNDARY_CDDNeighborhoods.geojson` | Polygones des quartiers de Cambridge (13 entités) |

Colonnes principales : `File Number`, `Date of Report`, `Crime Date Time`, `Crime`,
`Reporting Area`, `Neighborhood`, `Location`.

---

## Prérequis

- Python 3.10+
- pip

---

## Installation

```bash
# Créer et activer l'environnement virtuel
python -m venv .venv
.venv\Scripts\activate        # Windows
source .venv/bin/activate     # Linux/Mac

# Installer les dépendances
pip install --upgrade pip
pip install -r requirements.txt
```

---

## Exécution

```bash
# Pipeline complète (profilage → audit → nettoyage → monitoring → carte)
python src/main.py
```

Les sorties sont produites dans `output/` :
- `crime_reports_clean.csv` — dataset nettoyé
- `map.html` — carte choroplèthe interactive

Les logs sont écrits dans `logs/app.log`.

Pour lancer les tests :
```bash
pytest tests/ -v
```

---

## Indicateurs de qualité (seuils d'acceptation)

| Indicateur | Avant | Après | Seuil attendu |
|---|---|---|---|
| Complétude `Crime` | ~95 % | 100 % | ≥ 95 % |
| Complétude `Neighborhood` | ~93 % | 100 % | ≥ 95 % |
| Unicité `File Number` | ~98 % | 100 % | ≥ 99 % |
| Taux de doublons exacts | ~2 % | 0 % | < 1 % |
| Dates invalides | ~0.5 % | 0 % | < 1 % |
| Incohérences temporelles | ~1 % | 0 % | < 1 % |
| Reporting Area non conformes | ~0.3 % | 0 % | < 1 % |

*(Les valeurs exactes sont affichées à l'exécution)*

---

## Décisions de traitement

| Anomalie | Décision | Justification |
|---|---|---|
| `File Number` en doublon exact | Suppression des doublons (conserver première occurrence) | Un numéro de dossier est un identifiant unique ; les doublons indiquent une double saisie sans valeur analytique |
| `Crime` null | Suppression de la ligne | Sans type de crime, la ligne est inexploitable pour tout indicateur criminel |
| `Date of Report` invalide | Suppression de la ligne | La date de signalement est structurante ; une date non parsable empêche toute analyse temporelle |
| `Date of Report` antérieure au début de `Crime Date Time` | Suppression de la ligne | Un crime ne peut pas être signalé avant qu'il commence ; incohérence logique irréparable sans source complémentaire |
| `Reporting Area` non conforme (négatif, non numérique) | Mise à NaN puis suppression | Une zone de rapport négative ou non numérique est une erreur de saisie ; on ne peut pas inférer la valeur correcte |
| `Neighborhood` invalide (hors référentiel, placeholder) | Mise à NaN puis suppression | Seuls les quartiers du référentiel officiel ont une signification cartographique |
| `reporting_area_group` aberrant (négatif, > 20) | Mise à NaN | Valeurs dérivées d'areas invalides ; nettoyées en amont, les aberrations résiduelles sont neutralisées |

---

## Question bonus

> **Pourquoi le terme « quartier le plus dangereux » peut-il être trompeur avec un indicateur en volume brut ?**

Un volume brut de crimes ne tient pas compte de la superficie ni de la population du quartier.
Un quartier densément peuplé ou très étendu accumulera mécaniquement plus de signalements qu'un
quartier résidentiel calme, sans que le risque per capita soit supérieur. Pour comparer équitablement
les quartiers, il faudrait un taux de criminalité ramené à la population (crimes / 1 000 habitants)
ou à la superficie (crimes / km²).
