from neo4j import GraphDatabase

import Constants


class GraphRepo:
    def __init__(self):
        self.driver = GraphDatabase.driver(Constants.URI, auth=(Constants.username, Constants.password))

    def execute_query(self, query):
        records, summary, keys = self.driver.execute_query(query)
        self.close_connections()
        return records, summary, keys

    def check_connectivity(self):
        self.driver.verify_connectivity()

    def close_connections(self):
        self.driver.close()
