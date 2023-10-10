from neo4j import GraphDatabase

import Constants


class GraphRepo:
    driver = GraphDatabase.driver(Constants.URI, auth=('neo4j', 'kgopu1998'))

    @classmethod
    def execute_query(cls, query):
        records, summary, keys = cls.driver.execute_query(query_=query)
        return records, summary, keys

    @classmethod
    def check_connectivity(cls):
        cls.driver.verify_connectivity()

    @classmethod
    def close_connections(cls):
        cls.driver.close()
