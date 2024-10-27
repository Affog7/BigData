Pour planifier le planificateur basé sur l'API OpenWeatherMap qui fournit des prévisions météorologiques sur 5 jours avec un pas de 3 heures, vous devez configurer votre DAG pour qu'il s'exécute à des intervalles appropriés. Étant donné que les prévisions sont mises à jour toutes les 3 heures, vous pouvez configurer le DAG pour qu'il s'exécute toutes les 3 heures.

 https://openweathermap.org/api

 # Pipeline ETL météo avec Apache Airflow

## Aperçu

Ce projet implémente un pipeline ETL (Extract, Transform, Load) pour les données météorologiques à l'aide d'Apache Airflow, Python et Docker. Le pipeline extrait les données de prévisions météorologiques de l'API OpenWeatherMap, les transforme dans un format approprié et les charge dans une base de données PostgreSQL. Les données sont ensuite analysées et visualisées à l'aide de Jupyter Notebook.

## Caractéristiques

- **Extraction de données** : récupère les données de prévisions météorologiques à partir de l'API OpenWeatherMap.
- **Transformation des données** : traite les données brutes dans un format structuré à l'aide de Pandas.
- **Chargement de données** : insère les données transformées dans une base de données PostgreSQL à l'aide de SQLAlchemy.
- **Analyse des données** : analyse et visualise les données à l'aide de Jupyter Notebook.
- **Environnement Dockerisé** : utilise Docker pour gérer l'environnement, garantissant ainsi la cohérence et la facilité de déploiement.

## Prérequis

- Docker et Docker Compose installés sur votre machine.
- Python 3.x installé sur votre machine.
- Une clé API OpenWeatherMap.

## Installation

1. Clonez le référentiel :
 ```bash
 git clone https://github.com/your-username/weather-etl-pipeline.git
 cd météo-etl-pipeline
 ```

2. Créez un fichier `.env` dans le répertoire racine avec votre clé API OpenWeatherMap :
 ```environ.
 OPENWEATHERMAP_API_KEY=votre_api_key_here
 ```

3. Créez et démarrez les conteneurs Docker :
 ```bash
 docker-compose up --build
 ```

4. Accédez à l'interface Web Airflow à l'adresse « http://localhost:8080 ».

## Utilisation

1. **Activez le DAG** :
 - Accédez à l'interface Web Airflow.
 - Trouvez le DAG `weather_etl` et activez-le.
 - Déclenchez le DAG manuellement pour démarrer le processus ETL.

2. **Analyser les données** :
 - Ouvrez un bloc-notes Jupyter.
 - Connectez-vous à la base de données PostgreSQL en utilisant les informations d'identification fournies.
 - Utilisez l'exemple de bloc-notes pour interroger les données et créer des visualisations.

## Structure du répertoire

```
météo-etl-pipeline/
├── jours/
│ └── météo_etl.py
├── docker-compose.yml
├── .env
├── LISEZMOI.md
└── cahiers/
 └── météo_analyse.ipynb
```

## Détails du DAG

Le DAG `weather_etl.py` se compose de trois tâches principales :

1. **Extraire les données** :
 - Récupère les données de prévisions météorologiques de l'API OpenWeatherMap.
 - Utilise la bibliothèque `requests` pour effectuer l'appel API.
 - Gère les erreurs d'API et réessaye si nécessaire.

2. **Transformer les données** :
 - Traite les données brutes JSON dans un format structuré à l'aide de Pandas.
 - Extrait les champs pertinents tels que la température, l'humidité et la description météorologique.
 - Convertit les données en DataFrame.

3. **Charger les données** :
 - Insère les données transformées dans une base de données PostgreSQL à l'aide de SQLAlchemy.
 - Remplace les données existantes dans la table « météo ».

## Analyse des données

Le notebook `weather_analysis.ipynb` fournit des exemples sur la façon d'interroger les données de la base de données PostgreSQL et de créer des visualisations à l'aide de Matplotlib et Seaborn. Le cahier comprend :

- Tracé linéaire de la température au fil du temps.
- Graphique à barres de l'humidité au fil du temps.
- Nuage de points de la température par rapport à humidité.
- Diagramme circulaire des descriptions météorologiques.

## Configuration du Docker

Le fichier `docker-compose.yml` définit les services requis pour le pipeline ETL :

- **PostgreSQL** : le service de base de données.
- **Airflow Init** : initialise la base de données Airflow et crée un utilisateur.
- **Serveur Web Airflow** : l'interface Web Airflow.
- **Airflow Scheduler** : le service de planification Airflow.

Chaque service dispose d'une politique de redémarrage pour gérer les échecs avec élégance.

## Contribuer

Les contributions sont les bienvenues ! Veuillez ouvrir un problème ou soumettre une pull request si vous avez des suggestions ou des améliorations.

## Licence

Ce projet est sous licence MIT. Consultez le fichier [LICENSE](LICENSE) pour plus de détails.

## Contact

Pour toute question ou assistance, veuillez contacter [votre-email@example.com](mailto:your-email@example.com).

## Documentation API

Pour plus d'informations sur l'API OpenWeatherMap, veuillez vous référer à la [documentation officielle](https://openweathermap.org/api/forecast30).