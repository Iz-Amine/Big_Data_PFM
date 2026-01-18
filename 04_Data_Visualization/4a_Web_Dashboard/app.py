from flask import Flask, render_template, send_from_directory
import os

# Configuration des chemins
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
TEMPLATE_DIR = os.path.join(BASE_DIR, 'templates')
STATIC_DIR = os.path.join(BASE_DIR, 'static')

# Création de l'application Flask
app = Flask(__name__, template_folder=TEMPLATE_DIR, static_folder=STATIC_DIR)

# Route pour la page d'accueil (Le Dashboard)
@app.route('/')
def dashboard():
    return render_template('dashboard.html')

# API : Route spéciale pour que le site puisse télécharger les JSON générés par Spark
@app.route('/api/data/<filename>')
def get_json_data(filename):
    # Sécurité : on ne sert que les fichiers du dossier 'data'
    data_dir = os.path.join(STATIC_DIR, 'data')
    return send_from_directory(data_dir, filename)

# Nouvelles routes pour les visualisations manquantes
@app.route('/api/quartiles')
def get_quartiles():
    data_dir = os.path.join(STATIC_DIR, 'data')
    return send_from_directory(data_dir, 'quartiles_distribution.json')

@app.route('/api/top-authors')
def get_top_authors():
    data_dir = os.path.join(STATIC_DIR, 'data')
    return send_from_directory(data_dir, 'top_authors.json')

@app.route('/api/top-laboratories')
def get_top_labs():
    data_dir = os.path.join(STATIC_DIR, 'data')
    return send_from_directory(data_dir, 'top_laboratories.json')

if __name__ == '__main__':
    print("🚀 Serveur lancé ! Oouvrez : http://127.0.0.1:5000")
    app.run(debug=True, port=5000)