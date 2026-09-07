import requests
import json
from sqlalchemy import create_engine, text  
from datetime import timedelta   #ESTABLECER TIEMPO DE REPETICIÓN DE DAG


#LIBRERIAS AIRFLOW NECESARIAS
from airflow.decorators import task, dag
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook #ESTABLECER CONEXION CON API
from airflow.providers.postgres.hooks.postgres import PostgresHook #ESTABLECER CONEXION CON BASE DE DATOS POSTGRE
import pendulum


#FUNCION INSERTAR DATOS EN TABLA
def charge_table (connection, stmt, param) :
    
    try:
        connection.execute(stmt, param)
        connection.commit()
    except :
        raise #SE RELANZA LA EXCEPCION PARA QUE AIRFLOW MARQUE LAS TASK COMO FALLIDA

#SE ESTABLECE LA CONFIGURACION DE LA DAG
@dag(
    dag_id = 'football_api',
    start_date = pendulum.datetime (2026, 9, 5, tz="UTC"),
    schedule = timedelta(minutes=30), #FRECUENCIA ESCOGIDA PARA MANTENER LOS DATOS ACTUALIZADOS SIN SER EXCESIVO
    catchup = False
)

#DEFINIR LAS DIFERENTES TAREAS  
def football_dag() :

    #TAREA DE EXTRACCIÓN DE DATOS CON CONEXIÓN A LA API MEDIANTE TOKEN (CONEXION GUARDADA EN EL APARTADO CONEXIONES DE LA INTERFAZ AIRFLOW)
    @task
    def extract_data() :

        connection = BaseHook.get_connection('token')
        base_url = connection.host
        token = connection.extra_dejson
        print(token)
        token = token.get('X-Auth-Token')
        dt_raw = requests.get(f"{base_url}",
                         headers={"X-Auth-Token": token}
                        )
        dt_raw = dt_raw.json() #SE PASAN LOS DATOS OBTENIDOS A UN ARCHIVO JSON
        return dt_raw
    data = extract_data() #SE GUARDAN LOS DATOS EN LA VARIABLE "DATA"

    
    @task
    def clean_data(dt_raw) :
        dt_list = []
        for match in dt_raw["matches"] :
            if match["homeTeam"]["id"] is not None and match["awayTeam"]["id"] is not None and match["competition"]["id"] is not None and match["season"]["id"] is not None  :
                dt_list.append(match)
        dt = {"matches" : dt_list}
        return dt
    clean = clean_data(data)

    #TAREAS DE MODELACION E INSERCCIÓN DE DATOS EN LAS TABLA
    @task
    def areas_table(dt) :

        param_area = []
        for match in dt["matches"] :
            df_area = {
            "area_id" : match["area"]["id"],
            "area_name" : match["area"]["name"],
            "area_code" : match["area"]["code"]
            }
            param_area.append(df_area)

        stmt_area = text("INSERT INTO areas (area_id, area_name, area_code) VALUES (:area_id, :area_name, :area_code) " \
                    "ON CONFLICT (area_id) DO NOTHING")
        hook = PostgresHook(postgres_conn_id='Football_api')
        engine = hook.get_sqlalchemy_engine()
        with engine.connect() as connection:
            connection.execute(stmt_area, param_area)
            connection.commit()
    areas = areas_table(clean)

    #TAREAS DE MODELACION E INSERCCIÓN DE DATOS EN LAS TABLA
    @task
    def competition_table(dt) :
        param_competition = []
        for match in dt["matches"] :
            df_competition = {
            "competition_id" : match["competition"]["id"],
            "competition_name" : match["competition"]["name"],
            "competition_code" : match["competition"]["code"],
            "competition_type" : match["competition"]["type"]
            }
            param_competition.append(df_competition)

        stmt_competition = text("INSERT INTO competitions (competition_id, competition_name, competition_code, competition_type) " \
                                "VALUES (:competition_id, :competition_name, :competition_code, :competition_type)" \
                                "ON CONFLICT (competition_id) DO NOTHING")
        hook = PostgresHook(postgres_conn_id='Football_api')
        engine = hook.get_sqlalchemy_engine()
        with engine.connect() as connection:
            connection.execute(stmt_competition, param_competition)
            connection.commit()
    competitions =competition_table(clean)

    #TAREAS DE MODELACION E INSERCCIÓN DE DATOS EN LAS TABLA
    @task
    def season_table(dt) :
        param_season = []
        for match in dt["matches"] :
            df_season = {
            "season_id" : match["season"]["id"],
            "season_startDate" : match["season"]["startDate"],
            "season_endDate" : match["season"]["endDate"],
            "season_currentMatchday" : match["season"]["currentMatchday"],
            "season_winner" : match["season"]["winner"]
            }
            param_season.append(df_season)

        #SE ACTUALIZAN LOS DATOS PARA SABER EN QUE JORANADA VA CADA LIGA
        stmt_season = text("INSERT INTO seasons (season_id, season_startDate, season_endDate, season_currentMatchday, season_winner) " \
                            "VALUES (:season_id, :season_startDate, :season_endDate, :season_currentMatchday, :season_winner) " \
                            "ON CONFLICT (season_id) DO UPDATE SET season_currentMatchday=EXCLUDED.season_currentMatchday") 

        hook = PostgresHook(postgres_conn_id='Football_api')
        engine = hook.get_sqlalchemy_engine()
        with engine.connect() as connection:
            connection.execute(stmt_season, param_season)
            connection.commit()
    seasons = season_table(clean)

    #TAREAS DE MODELACION E INSERCCIÓN DE DATOS EN LAS TABLA
    @task 
    def teams_table(dt) :
        param_teams = []
        for match in dt["matches"] :
            df_home_team = {
            "team_id" : match["homeTeam"]["id"],
            "team_name" : match["homeTeam"]["name"],
            "team_shortName" : match["homeTeam"]["shortName"],
            "team_tla" : match["homeTeam"]["tla"]
            }
            df_away_team = {
                "team_id" : match["awayTeam"]["id"],
                "team_name" : match["awayTeam"]["name"],
                "team_shortName" : match["awayTeam"]["shortName"],
                "team_tla" : match["awayTeam"]["tla"]
            }
            param_teams.append(df_home_team)
            param_teams.append(df_away_team)

        stmt_teams = text("INSERT INTO teams (team_id, team_name, team_shortName, team_tla) " \
                        "VALUES (:team_id, :team_name, :team_shortName, :team_tla)" \
                        "ON CONFLICT (team_id) DO NOTHING")

        hook = PostgresHook(postgres_conn_id='Football_api')
        engine = hook.get_sqlalchemy_engine()
        with engine.connect() as connection:
            connection.execute(stmt_teams, param_teams)
            connection.commit()
    teams = teams_table(clean)

    #TAREAS DE MODELACION E INSERCCIÓN DE DATOS EN LAS TABLA
    @task 
    def matches_table(dt) :
        param_match = []
        for match in dt["matches"] :  
            df_matches = {
            "match_id" : match["id"],
            "match_utcDate" : match["utcDate"], 
            "match_status" : match["status"],
            "match_matchday" : match["matchday"],
            "match_stage" : match["stage"],
            "match_group" : match["group"],
            "match_lastUpdated" : match["lastUpdated"],
            "match_winner" : match["score"]["winner"],
            "match_duration" : match["score"]["duration"],
            "match_fulltime_home" : match["score"]["fullTime"]["home"],
            "match_fulltime_away" : match["score"]["fullTime"]["away"],
            "match_halftime_home" : match["score"]["halfTime"]["home"],
            "match_halftime_away" : match["score"]["halfTime"]["away"],
            "home_team_id" : match["homeTeam"]["id"],
            "away_team_id" : match["awayTeam"]["id"],
            "area_id" : match["area"]["id"],
            "competition_id" : match["competition"]["id"],
            "season_id" : match["season"]["id"]
            }
            param_match.append(df_matches)

        #SE UTILIZA DO UPDATE SER PARA ACTUALIZAR LOS DATOS DE LOS PARTIDOS (LOS ID NO SE MODIFICAN PARA MANTENER LA INTEGRIDAD DE LAS FK)
        stmt_match = text("INSERT INTO matches (match_id, match_utcDate, match_status, match_matchday, match_stage, match_group, match_lastUpdated, match_winner, match_duration, " \
                            "match_fulltime_home, match_fulltime_away, match_halftime_home, match_halftime_away, home_team_id, away_team_id, area_id, competition_id, season_id)" \
                            "VALUES (:match_id, :match_utcDate, :match_status, :match_matchday, :match_stage, :match_group, :match_lastUpdated, :match_winner, :match_duration, " \
                            ":match_fulltime_home, :match_fulltime_away, :match_halftime_home, :match_halftime_away, :home_team_id, :away_team_id, :area_id, :competition_id, :season_id)" \
                            "ON CONFLICT (match_id) DO UPDATE SET match_status=EXCLUDED.match_status, match_winner=EXCLUDED.match_winner, match_duration=EXCLUDED.match_duration, " \
                            "match_fulltime_home=EXCLUDED.match_fulltime_home, match_fulltime_away=EXCLUDED.match_fulltime_away, match_halftime_home=EXCLUDED.match_halftime_home, match_halftime_away=EXCLUDED.match_halftime_away, match_lastUpdated=EXCLUDED.match_lastUpdated")

        hook = PostgresHook(postgres_conn_id='Football_api')
        engine = hook.get_sqlalchemy_engine()
        with engine.connect() as connection:
            connection.execute(stmt_match, param_match)
            connection.commit()
    matches = matches_table(clean)
    #ORDEN DE EJECUCIÓN DE LAS TAREAS
    data >> clean >> [areas, competitions, seasons, teams] >> matches 

football_dag() 