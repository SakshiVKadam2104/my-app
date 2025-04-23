from flask import Flask, jsonify, request
import sqlite3

app = Flask(__name__)
DATABASE_NAME = 'dashboard.db'

def get_db_connection():
    conn = sqlite3.connect(DATABASE_NAME)
    conn.row_factory = sqlite3.Row  # To access columns by name
    return conn

def close_db_connection(conn):
    if conn:
        conn.close()

@app.route('/api/data', methods=['GET'])
def get_dashboard_data():
    conn = get_db_connection()
    cur = conn.cursor()

    query = "SELECT intensity, likelihood, relevance, strftime('%Y', published) AS year, country, topic, region, city, sector, pestle, source FROM insights WHERE 1=1"
    filters = {}
    if request.args.get('end_year'):
        query += " AND end_year = ?"
        filters['end_year'] = request.args.get('end_year')
    if request.args.get('topic'):
        query += " AND topic = ?"
        filters['topic'] = request.args.get('topic')
    if request.args.get('sector'):
        query += " AND sector = ?"
        filters['sector'] = request.args.get('sector')
    if request.args.get('region'):
        query += " AND region = ?"
        filters['region'] = request.args.get('region')
    if request.args.get('pestle'):
        query += " AND pestle = ?"
        filters['pestle'] = request.args.get('pestle')
    if request.args.get('source'):
        query += " AND source = ?"
        filters['source'] = request.args.get('source')
    if request.args.get('country'):
        query += " AND country = ?"
        filters['country'] = request.args.get('country')
    if request.args.get('city'):
        query += " AND city = ?"
        filters['city'] = request.args.get('city')

    cur.execute(query, tuple(filters.values()))
    data = [dict(row) for row in cur.fetchall()]
    cur.close()
    close_db_connection(conn)
    return jsonify(data)

@app.route('/api/filters', methods=['GET'])
def get_filter_options():
    conn = get_db_connection()
    cur = conn.cursor()

    filter_options = {}
    filters_to_fetch = ['end_year', 'topic', 'sector', 'region', 'pestle', 'source', 'country', 'city']
    for f in filters_to_fetch:
        query = f"SELECT DISTINCT {f} FROM insights WHERE {f} IS NOT NULL AND {f} != '' ORDER BY {f}"
        cur.execute(query)
        values = [row[0] for row in cur.fetchall()]
        filter_options[f] = values

    cur.close()
    close_db_connection(conn)
    return jsonify(filter_options)

if __name__ == '__main__':
    app.run(debug=True)