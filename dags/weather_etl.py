# Importation des modules nécessaires d'Airflow et de Python
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import timedelta
import requests
import pandas as pd
from sqlalchemy import create_engine  # Pour la connexion et le chargement dans une base de données PostgreSQL
import pendulum
from airflow.models import Variable

# Configuration par défaut des arguments du DAG
default_args = {
    'owner': 'airflow',  # Le propriétaire du DAG
    'depends_on_past': False,  # Ne dépend pas des exécutions passées
    'start_date': pendulum.today('UTC').add(days=-1),  # Commence à "hier" pour s'assurer que le DAG s'exécute dès son lancement
    'email_on_failure': False,  # Pas d'alerte email en cas d'échec
    'email_on_retry': False,  # Pas d'alerte email en cas de retry
    'retries': 1,  # Nombre de tentatives en cas d'échec
    'retry_delay': timedelta(minutes=5),  # Intervalle entre chaque retry
}

# Définition de la liste des villes pour lesquelles nous voulons récupérer les données météo
cities = ['Paris', 'Londres', 'Berlin', 'Tokyo']

# Définition du DAG pour l'ETL météo
dag = DAG(
    'weather_etl',
    default_args=default_args,
    description='ETL pipeline pour les données météo de plusieurs villes',
    schedule_interval=timedelta(hours=3),  # Exécution toutes les 3 heures pour s'aligner avec les mises à jour de l'API
)

# Fonction d'extraction des données météo pour une ville donnée
def extract_data(city):
    api_key = Variable.get("weather_api_key")  # Clé API stockée dans les variables Airflow pour la sécurité
    url = f'https://api.openweathermap.org/data/2.5/forecast?q={city}&appid={api_key}'  
    response = requests.get(url)   
    if response.status_code == 200:  # Vérification du statut de la réponse
        data = response.json()  # Conversion en JSON si la réponse est correcte
        return data
    else:
        raise ValueError(f"Failed to fetch data for {city}: {response.status_code}")  # Gestion des erreurs si la requête échoue

# Fonction de transformation des données extraites en un DataFrame pandas
def transform_data(city, **kwargs):
    ti = kwargs['ti']  # Récupération du contexte pour l'échange de données avec XCom
    data = ti.xcom_pull(task_ids=f'extract_data_{city}')  # Extraction des données de la tâche précédente via XCom
    weather_list = data['list']  # Liste de prévisions météo par période
    rows = []
    for item in weather_list:
        row = {
            'city': city,  # Ajout de la ville pour identifier les données
            'dt': item['dt'],  # Timestamp de la prévision
            'temperature': item['main']['temp'],  # Température
            'humidity': item['main']['humidity'],  # Humidité
            'weather_description': item['weather'][0]['description'],  # Description du temps
            'dt_txt': item['dt_txt']  # Date et heure en texte
        }
        rows.append(row)
    df = pd.DataFrame(rows)  # Transformation en DataFrame
    return df.to_json()  # Renvoi du DataFrame en format JSON pour l'échange de données

# Fonction de chargement des données dans la base de données PostgreSQL
def load_data(city, **kwargs):
    ti = kwargs['ti']  # Récupération du contexte pour l'échange de données avec XCom
    data = ti.xcom_pull(task_ids=f'transform_data_{city}')  # Extraction des données de la tâche précédente
    df = pd.read_json(data)  # Conversion du JSON en DataFrame
    engine = create_engine('postgresql+psycopg2://airflow:airflow@postgres:5432/airflow')  
    df.to_sql('weather', engine, if_exists='append', index=False)  

# Définition des tâches pour chaque ville
for city in cities:
    with dag:
        extract_task = PythonOperator(
            task_id=f'extract_data_{city}',  # Tâche d'extraction des données pour la ville spécifique
            python_callable=extract_data,
            op_args=[city],  # Passe la ville en argument
        )

        transform_task = PythonOperator(
            task_id=f'transform_data_{city}',  # Tâche de transformation des données pour la ville
            python_callable=transform_data,
            op_args=[city],  # Passe la ville en argument
            provide_context=True,  # Nécessaire pour utiliser XCom
        )

        load_task = PythonOperator(
            task_id=f'load_data_{city}',  # Tâche de chargement des données pour la ville
            python_callable=load_data,
            op_args=[city],  # Passe la ville en argument
            provide_context=True,  # Nécessaire pour utiliser XCom
        )

        # Orchestration des tâches pour chaque ville : extraction -> transformation -> chargement
        extract_task >> transform_task >> load_task
