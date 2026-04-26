# 🐛 TROUBLESHOOTING - Meteo Project

## 📋 Erreurs d'Orchestration (Dagster)

### 🔧 1. Importation du module extract

#### ❌ Erreur
```
ModuleNotFoundError: No module named 'weather_extractor'
```

#### ✅ Solution
Ajouter le répertoire `extract` au `sys.path` dans les assets Dagster :

```python
import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..', 'extract'))
from weather_extractor import WeatherExtractor
```

#### 📍 Fichier concerné
- `dagster_weather/dagster_weather/assets.py`

---

### 🔧 2. Dépendances Dagster non fonctionnelles

#### ❌ Erreur
```
load_weather_to_motherduck ne reçoit pas les données de extract_weather_data
```

#### ✅ Solution
1. **Annoter le retour de l'asset extract** :
```python
def extract_weather_data(context: AssetExecutionContext) -> Output[pd.DataFrame]:
    # ... code d'extraction ...
    return Output(value=df, metadata=metadata)
```

2. **Annoter l'entrée de l'asset load** :
```python
def load_weather_to_motherduck(context: AssetExecutionContext, extract_weather_data: pd.DataFrame) -> MaterializeResult:
    # ... code de chargement ...
```

3. **Déclarer la dépendance** :
```python
@asset(
    key="load_weather_to_motherduck",
    deps="extract_weather_data"  # ou deps=["extract_weather_data"]
)
```

#### 📍 Fichiers concernés
- `dagster_weather/dagster_weather/assets.py`

---

### 🔧 3. Conflits de versions Dagster

#### ❌ Erreurs
```
ImportError: cannot import name 'assert_no_remaining_opts' from 'dagster._cli.utils'
DagsterInvalidDefinitionError: Invalid type: dagster_type must be an instance of DagsterType
```

#### ✅ Solution
Version compatible identifiée : `dagster==1.7.16`

```bash
# Downgrader si nécessaire
pip install dagster==1.7.16

# Ou upgrader vers version stable
pip install --upgrade dagster dagster-webserver
```

#### 📋 Notes sur les versions
- **1.7.16** : Version stable testée et fonctionnelle
- **Éviter 1.8.x** : Problèmes de compatibilité signalés

---

### 🔧 4. Problèmes de chemins relatifs

#### ❌ Erreur
```
FileNotFoundError: [Errno 2] No such file or directory: '..\..\data'
ERROR:weather_extractor:Failed to save data to ../../data/weather_data_20260418_031001.csv: Cannot save file into a non-existent directory: '..\..\data'
```

#### ✅ Solution
Utiliser des chemins absolus depuis les assets Dagster :

```python
# Ancienne approche (problématique)
csv_path = f"../../data/weather_data_{timestamp}.csv"

# Nouvelle approche (fonctionnelle)
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
csv_path = os.path.join(project_root, 'data', f"weather_data_{timestamp}.csv")
```

#### 📍 Fichiers concernés
- `dagster_weather/dagster_weather/assets.py`
- `extract/weather_extractor.py`

---

### 🔧 5. Erreurs de type Dagster

#### ❌ Erreur
```
DagsterTypeCheckDidNotPass: Type check failed for step output "result" - expected type "Nothing". Description: Value must be None, got a <class 'pandas.core.frame.DataFrame'>
DagsterInvariantViolationError: Error with output for op "extract_weather_data": received Output object for output 'result' which does not have an Output annotation.
```

#### ✅ Solution
1. **Importer Output** :
```python
from dagster import Output  # Ajouter aux imports
```

2. **Utiliser Output pour les assets qui transmettent des données** :
```python
return Output(value=df, metadata=metadata)
```

3. **Utiliser MaterializeResult pour les assets finaux** :
```python
return MaterializeResult(metadata=metadata)
```

---

### 🔧 6. Problèmes de métadonnées

#### ❌ Erreur
```
DagsterInvalidMetadata: Could not resolve the metadata value for "date" to a known type. Its type was <class 'pandas._libs.tslibs.timestamps.Timestamp'>.
```

#### ✅ Solution
Convertir les types de données non supportés en strings :

```python
# Ancienne approche
"date": df['date'].iloc[0]  # Timestamp pandas non supporté

# Nouvelle approche
"date": str(df['date'].iloc[0])  # String supportée
```

---

### 🔧 9. Erreurs Pylance : "(" was not closed

#### ❌ Erreur
```
message: "(" was not closed (Pylance)
```
Généralement signalé sur la ligne `return SensorResult(`.

#### ✅ Solution
Il s'agit d'un problème d'indentation des arguments à l'intérieur de la fonction. En Python, les arguments d'une fonction multi-ligne doivent être alignés. Si l'argument `cursor` est aligné avec le mot-clé `return` au lieu d'être indenté à l'intérieur des parenthèses, Pylance lève une erreur de syntaxe.

**Correction :**
```python
return SensorResult(
    run_requests=[...],
    cursor=str(...) # S'assurer que 'cursor' est bien aligné sous 'run_requests'
)
```

---

### 🔧 10. dbt docs serve : Erreurs 404 "getIcon"

#### ❌ Erreur
```
127.0.0.1 - - "GET /%7B%7B%20getIcon(item.type,%20'on')%20%7D%7D HTTP/1.1" 404 -
```

#### ✅ Solution
Il s'agit d'un problème cosmétique connu dans l'interface utilisateur de dbt docs lié au rendu des icônes. 

**Impact :** Aucun. Votre documentation et votre graphe de lignage restent parfaitement fonctionnels. Vous pouvez ignorer ces messages dans la console.

---

## 🧪 Tests et validation

### ✅ Tests de bon fonctionnement
```bash
# Test extraction seule
dagster asset materialize -f dagster_weather/dagster_weather/assets.py --select extract_weather_data

# Test chargement avec dépendance
dagster asset materialize -f dagster_weather/dagster_weather/assets.py --select extract_weather_data,load_weather_to_motherduck

# Vérification des fichiers générés
ls -la data/weather_data_*.csv
```

### 📊 Logs attendus
```
✅ extract_weather_data: Successfully extracted 3 weather records
✅ load_weather_to_motherduck: Successfully saved 3 records to /path/to/data/weather_data_YYYYMMDD_HHMMSS.csv
✅ Dependencies: extract_weather_data → load_weather_to_motherduck
```

---

## 🚀 Checklist de déploiement

### Avant de lancer en production
- [ ] Variables d'environnement configurées (`.env`)
- [ ] Clés API valides (OpenWeatherMap, MotherDuck)
- [ ] Python 3.12+ installé
- [ ] Dagster 1.7.16 installé
- [ ] Répertoire `data/` créé
- [ ] Tests locaux passants

### Commandes de validation finale
```bash
# Pipeline complet
dagster asset materialize -f dagster_weather/dagster_weather/assets.py --select extract_weather_data,load_weather_to_motherduck

# Vérification des fichiers
ls -la data/weather_data_*.csv

# UI Dagster (optionnel)
dagster dev -f dagster_weather/dagster_weather/assets.py
```

---

## 📞 Support

Si vous rencontrez d'autres erreurs :

1. **Vérifier les logs** : Messages détaillés dans la console
2. **Tester isolément** : Lancer les assets un par un
3. **Consulter ce fichier** : Solutions documentées ici
4. **Vérifier les versions** : Dagster 1.7.16 recommandé

---

**🐛 Dernière mise à jour : 2026-04-18**
**✅ Pipeline ELT opérationnel et documenté**
