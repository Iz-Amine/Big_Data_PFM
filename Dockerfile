# Utiliser une image Python légère
FROM python:3.12-slim

# Répertoire de travail
WORKDIR /app

# Copier les dépendances et installer
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copier tout le projet
COPY . .

# Commande par défaut (facultatif, pour garder le conteneur en vie ou lancer un script)
CMD ["/bin/bash"]
