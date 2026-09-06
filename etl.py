import pandas as pd
import numpy as np
import requests
import json
from sqlalchemy import create_engine, text

import os 
from dotenv import load_dotenv


load_dotenv()
token = os.getenv('token')
user = os.getenv('user')

session = requests.Session()
session.headers.update({
    'X-Auth-Token' : token
})

import logging #IMPORTAR LOG DE AVISO

#ESTABLECER LA CONFIGURACION DE LOS AVISOS
logging.basicConfig(filename = 'etl_consultasfootballmatches.log', level = logging.INFO, format = '%(asctime)s %(levelname)s %(message)s')

#INSERTAR CREDENCIALES
load_dotenv()
user = os.getenv('user')
password = os.getenv('password')
ip = os.getenv('ip')
port = os.getenv('port')
database = os.getenv('database')

engine= create_engine('postgresql://' + user + ':' + password + '@' + ip + ':' + port + '/' + database)

def charge_table (connection, stmt, param) :
    
    try:
        connection.execute(stmt, param)
        connection.commit()
        logging.info("Load complete.")
    except Exception as e:
        logging.error(f"Error al insertar: {e}.")


with engine.connect() as connection :
    logging.info("Starting pipeline...")

    connection.execute(text('''
        CREATE TABLE IF NOT EXISTS areas(
        area_id INT NOT NULL,
        area_name VARCHAR(250),
        area_code VARCHAR(5),

        PRIMARY KEY (area_id)  
        )
    '''))

    connection.execute(text('''
        CREATE TABLE IF NOT EXISTS competitions(
        competition_id INT NOT NULL,
        competition_name VARCHAR(250),
        competition_code VARCHAR(5),
        competition_type VARCHAR(100),

        PRIMARY KEY (competition_id)
        )
    '''))

    connection.execute(text('''
        CREATE TABLE IF NOT EXISTS seasons(
        season_id INT NOT NULL,
        season_startDate DATE,
        season_endDate DATE,
        season_currentMatchday INT,
        season_winner VARCHAR(250),

        PRIMARY KEY (season_id)
        )
    '''))

    connection.execute(text('''
        CREATE TABLE IF NOT EXISTS teams(
        team_id INT NOT NULL,
        team_name VARCHAR(250),
        team_shortname VARCHAR(20),
        team_tla VARCHAR(3),

        PRIMARY KEY (team_id)
        )
    '''))

    connection.execute(text('''
        CREATE TABLE IF NOT EXISTS matches(
        match_id INT NOT NULL,
        match_utcDate TIMESTAMP,
        match_status VARCHAR(100),
        match_matchday INT,
        match_stage VARCHAR(100),
        match_group VARCHAR(50),
        match_lastUpdated TIMESTAMP,
        match_winner VARCHAR(250),
        match_duration VARCHAR(250),
        match_fulltime_home INT,
        match_fulltime_away INT,
        match_halftime_home INT,
        match_halftime_away INT,
        home_team_id INT NOT NULL,
        away_team_id INT NOT NULL,
        area_id INT NOT NULL,
        competition_id INT NOT NULL,
        season_id INT NOT NULL,

        PRIMARY KEY (match_id),

        FOREIGN KEY (home_team_id) REFERENCES teams(team_id),
        FOREIGN KEY (away_team_id) REFERENCES teams(team_id),
        FOREIGN KEY (area_id) REFERENCES areas(area_id),
        FOREIGN KEY (competition_id) REFERENCES competitions(competition_id),
        FOREIGN KEY (season_id) REFERENCES seasons(season_id)
        )
    '''))

    connection.commit()

    params = {'dateFrom': '2026-08-27', 'dateTo' : '2026-08-31'}
    dt = session.get("https://api.football-data.org/v4/matches", params = params)
    dt = dt.json()


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

    charge_table (connection, stmt_area, param_area)

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

    charge_table (connection, stmt_competition, param_competition)

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

    stmt_season = text("INSERT INTO seasons (season_id, season_startDate, season_endDate, season_currentMatchday, season_winner) " \
                        "VALUES (:season_id, :season_startDate, :season_endDate, :season_currentMatchday, :season_winner) " \
                        "ON CONFLICT (season_id) DO NOTHING")

    charge_table (connection, stmt_season, param_season)

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

    charge_table (connection, stmt_teams, param_teams)


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

    stmt_match = text("INSERT INTO matches (match_id, match_utcDate, match_status, match_matchday, match_stage, match_group, match_lastUpdated, match_winner, match_duration, " \
                        "match_fulltime_home, match_fulltime_away, match_halftime_home, match_halftime_away, home_team_id, away_team_id, area_id, competition_id, season_id)" \
                        "VALUES (:match_id, :match_utcDate, :match_status, :match_matchday, :match_stage, :match_group, :match_lastUpdated, :match_winner, :match_duration, " \
                        ":match_fulltime_home, :match_fulltime_away, :match_halftime_home, :match_halftime_away, :home_team_id, :away_team_id, :area_id, :competition_id, :season_id)" \
                        "ON CONFLICT (match_id) DO NOTHING")

    charge_table (connection, stmt_match, param_match)