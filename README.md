# KG Creator
KG(Knowledge Graph) Creator is an API that constructs nodes and their relations dynamically.

## Tech Stack
* This project is developed using python.
* For persistent storage(database), the project uses neo4J, a graph database, to store nodes and their relations.
* To write Cypher( graph query language)  queries in python, the project uses query builder to build queries effectively.

## Installation
* Install the latest version of python in your machine.
* Install cymple, query builder package, using command ```pip install cymple```
* Download neo4j desktop version in your OS, and setup username and password (check requirements file for more information).
* Install neo4j driver using command ```pip install neo4j```. This package provides neo4j driver to interact with a Neo4j instance through a python application.
* In Constants.py file update the username and password with your neo4j credentials.