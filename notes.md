# Ressources GTFS et GTFS RT

https://transport.data.gouv.fr/datasets/donnees-statiques-et-dynamiques-du-reseau-de-transport-lignes-dazur

## Données GTFS statiques

- routes.txt = Informations génrales sur la ligne (noms, couleur, débuts, fins ...) 
- shapes.txt = Définition d'un parcours. Ex: allez-retour d'une `route` (Station A -> Station B) != (Station B -> Station A). Permet aussi d'exprimer les différents terminus d'une même `route`.
- trips.txt = Trajets en fonction de la `route` et de la `shape` et des horaires. Un `trip` = une ligne horaire.
- stops.txt = Liste des arrêts avec latitude et longitude
- stop_times.txt = Liste des horaires selon les `stops` et `trips`

## GTFS RT

### R&cupérer et exploiter les données
https://gtfs.org/documentation/realtime/

Exemples de code python:
- https://gtfs.org/documentation/realtime/language-bindings/python/
- https://github.com/plannerstack/gtfsrt-example-client  

Documentation et references:
- https://gtfs.org/documentation/realtime/proto/

Données mise-à-jour tous les x minutes

### Donneés:
- trip_update 
    - trip = `trip` concerné 
    - stop_time_update = date d'arrivé/départ prévue

```python
trip_update {
  trip {
    trip_id: "6416517-33_A_47_33S2_16:35-SETP2025-33-L-Ma-J-V-36"
    route_id: "33"
    direction_id: 1
  }
  stop_time_update {
    stop_sequence: 0
    arrival {
    }
    departure {
      time: 1756737300
    }
    stop_id: "1426"
  }
}
``` 

- vehicle = Donnes la localisation d'un véhicule

```python
vehicle {
  trip {
    trip_id: "6428348-60_A_50_6003_14:50-PROJET2025-60-Semaine-08"
    route_id: "chouette:Line:4a8dc505-72e7-4b72-b186-a5b46e94fb82:LOC"
  }
  position {
    latitude: 43.6923866
    longitude: 7.24417925
    bearing: 163
  }
  timestamp: 1756730896
  stop_id: "2997"
  vehicle {
    id: "TCA73932"
  }
}
```

# Airflow

**DAG**
Directed Acyclic Graph
Collection de tâches qui s'enchaîne = workflow
Composer de:
- Schedule: Quand
- Tasks: Quoi
- Tasks dependencies: Ordre d'éxecutions des tâches
- Callbacks: éxecuter quand le `workflow` termine son éxecution

**Tasks**
Un traitement
https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/tasks.html

**Operators**
Template de taches prédefinies. Utiles pour les tâches fréquement utilisés.
ex: BashOperator
https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/operators.html

**Sensors**
Type d'`Operator` qui attend un événement externe 
https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/sensors.html

**Scheduler**
Surveille toutes les `Dags` et les `Tasks`. Execute les `Tasks` quand leurs prérequis sont complétés

**Executor**
Lance les `Tasks`. Ils sont interchangeablent.
https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/executor/index.html

**Metastore**
Permet de stocker les données liés d'airflow lui même tel que les `Tasks`, les `Dags` 
**XCom**
Cross-comunication
Permets aux `Tasks`de s'échanger des données entre elles

```python
# pushes data in any_serializable_value into xcom with key "identifier as string"
task_instance.xcom_push(key="identifier as a string", value=any_serializable_value)
# pulls the xcom variable with key "identifier as string" that was pushed from within task-1
task_instance.xcom_pull(key="identifier as string", task_ids="task-1")

```



TODO: 
- logs
- xcom
- Ecrire dans un fichier
- lire un fichier
- duckdb