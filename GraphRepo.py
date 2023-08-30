from neo4j import GraphDatabase

import Constants


class GraphRepo:
    driver = GraphDatabase.driver(Constants.URI, auth=(Constants.USERNAME, Constants.PASSWORD))

    @classmethod
    def execute_query(cls, query, routing_control='w'):
        records, summary, keys = cls.driver.execute_query(query_=query, routing_=routing_control)
        return records, summary, keys

    @classmethod
    def check_connectivity(cls):
        cls.driver.verify_connectivity()

    @classmethod
    def close_connections(cls):
        cls.driver.close()
