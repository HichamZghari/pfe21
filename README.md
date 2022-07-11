# Informations générales
Ce dépot est l'objet du travail de Aissam Chia, Laila Zoubari et Mathieu Faugeras sur la technologie Snowflake.
Supervisé par Christophe Desrousseaux et Thierno Madiariou Diallo.

Pour être sûre d'avoir les bonnes versions de pasckages merci de faire la commande suivante :

pip install -r .\requirements.txt

# Migration Snowflake

Dans le dossier migration_SF vous retrouverez :

main.py -> le main qui devra être appelé pour la migration


loader_MySQL.py      |
loader_Postgre.py    |
loader_SQL_Server.py | -> les loaders qui sont appelés dans main.py

migration_MySQL.py      |
migration_Postgre.py    |
migration_SQL_Server.py | -> Les fonctions pour la migration

utils.py -> Fichier contenant les fonctions génériques


Pour lancer la migration veuiller remplir le fichier config.ini et faire la commande suivante à la racine du git :

python .\migration_SF\main.py nom_SGBD




# Market place

Dans le dossier Market_Place vous retrouverez :

main_market_place.py -> le main pour charger une source de données

utils_market_place.py -> le fichier regroupant toutes les fonctions du main

test.config -> fichier servant à se connecter à snowsql

snowflake.ini -> fichier contenant les informations de connection à sf

ini_files -> le dossier contenant tous les fichiers de description des sources de données


Pour lancer la migration veuiller remplir le fichier snowflake.ini et test.config et faire la commande suivante dans le dossier market_place : python .\main_market_place.py .\ini_files\nom_du_fichier.ini 

par exemple : python .\main_market_place.py .\ini_files\ACCIDENT_ROUTIER_VEHICULES.ini

