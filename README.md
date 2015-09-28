# hadoop-sandbox
Projet "bac à sable" pour expérimenter Hadoop.

Informations utiles :
 * Mockup contient une classe qui permet de remplir la base avec de fausses données.
 * Scratchpad contient des classes principales d'expérimentation.
 * Les tests ne sont encore que des ébauches.
  
Comment démarrer :
 * Cloner le projet et l'importer dans un IDE.
 * Lancer mvn install -DskipTests
 * Définir la variable d'environnement HADOOP_HOME dans la configuration du projet.
 * Créer les tables dans HBase (voir "Strucutre HBase" ci-dessous).
 * Adapter hbase-site.xml à la configuration locale.
 * Définir ENTRY_COUNT dans Mock1FillStations puis lancer la classe pour générer des données.
 * Lancer Main puis vérifier les logs et la table 'results'.

# Strucutre HBase

create_namespace 'meteo'

create 'meteo:stations', 'coordinates', 'parameters'

create 'meteo:results', 'results'
