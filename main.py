from flask import Flask, render_template,request, jsonify
from flask_cors import CORS, cross_origin
import pymysql
import json
import os

db_user = os.environ.get('CLOUD_SQL_USERNAME')
db_password = os.environ.get('CLOUD_SQL_PASSWORD')
db_name = os.environ.get('CLOUD_SQL_DATABASE_NAME')
db_connection_name = os.environ.get('CLOUD_SQL_CONNECTION_NAME')

sql_shot_detail_query_all_shots = "SELECT PLAYER_NAME, LOC_X, LOC_Y,SHOT_MADE_FLAG,EVENT_TYPE,ACTION_TYPE,SHOT_ZONE_BASIC,SHOT_DISTANCE FROM ShotChartDetails WHERE PERIOD IN ({0}) AND PLAYER_ID IN ({1}) AND SHOT_ZONE_BASIC IN ({2}) AND MINUTES_REMAINING BETWEEN {3};"
sql_league_avg_query = "select SHOT_ZONE_BASIC, round(SUM(SHOT_MADE_FLAG = 1) /COUNT(SHOT_ZONE_BASIC ) * 100) AS LEAGUE_AVG FROM ShotChartDetails WHERE PERIOD IN ({0})AND SHOT_ZONE_BASIC IN ({1}) AND MINUTES_REMAINING BETWEEN {2} GROUP BY SHOT_ZONE_BASIC;"
sql_subject_avg  = "select SHOT_ZONE_BASIC, round(SUM(SHOT_MADE_FLAG = 1) /COUNT(SHOT_ZONE_BASIC ) * 100) AS PLAYER_AVG FROM ShotChartDetails WHERE PLAYER_ID IN ({0}) AND  PERIOD IN ({1})AND SHOT_ZONE_BASIC IN ({2}) AND MINUTES_REMAINING BETWEEN {3} GROUP BY SHOT_ZONE_BASIC;"

app = Flask(__name__)
cors = CORS(app)
app.config['CORS_HEADERS'] = 'Content-Type'

@app.route('/teamDetails', methods=['GET'])
@cross_origin()
def teamDetailsApi():
    team_name = request.args.get('team_name')
    team_data = []
    cnx = open_connection()
    with cnx.cursor() as cursor:
        cursor.execute("SELECT DISTINCT(PLAYER_ID),PLAYER_NAME FROM ShotChartDetails WHERE TEAM_NAME = %s", [team_name])
        headers = [head[0] for head in cursor.description]
        results = cursor.fetchall()
        for player in results:
            team_data.append(dict(zip(headers,player)))
    cnx.close()
    return json.dumps(team_data)

@app.route('/team/shotChart', methods=['GET'])
@cross_origin()
def teamShotChartInfo():
    subject = request.args.get('subject')
    shotType = request.args.get('shotType')
    qtrs = request.args.get('qtr')
    timeInterval = request.args.get('timeInterval')

    shot_data = []
    league_avg = []
    subject_avg = []

    cnx = open_connection()
    with cnx.cursor() as cursor:
        cursor.execute(sql_shot_detail_query_all_shots.format(qtrs,subject,shotType,timeInterval))
        headers = [head[0] for head in cursor.description]
        results = cursor.fetchall()
        for shot in results:
            shot_data.append(dict(zip(headers,shot)))

        cursor.execute(sql_league_avg_query.format(qtrs,shotType,timeInterval))
        headers = [head[0] for head in cursor.description]
        results = cursor.fetchall()
        for stat in results:
            lst = list(stat)
            lst[1] = int(lst[1])
            tuple(lst)
            league_avg.append(dict(zip(headers,tuple(lst))))

        cursor.execute(sql_subject_avg.format(subject,qtrs,shotType,timeInterval))
        headers = [head[0] for head in cursor.description]
        results = cursor.fetchall()
        for stat in results:
            lst = list(stat)
            lst[1] = int(lst[1])
            tuple(lst)
            subject_avg.append(dict(zip(headers,tuple(lst))))
    cnx.close()
    return json.dumps({"shot_data" :shot_data, "league_avg": league_avg, "subject_avg": subject_avg})

if __name__ == '__main__':
    app.run(host='127.0.0.1', port=8080, debug=True)


def open_connection():
    if os.environ.get('GAE_ENV') == 'standard': 
        unix_socket = '/cloudsql/{}'.format(db_connection_name)
        cnx = pymysql.connect(user=db_user, password=db_password, unix_socket=unix_socket, db=db_name)
    return cnx