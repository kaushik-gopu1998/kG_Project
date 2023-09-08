import os
import unittest
from neo4j import GraphDatabase
from main import get_label, get_subject_predict_label, parse_relation
import Constants
import RemoteGraphConstruction
from main import extract_data_from_csv
from concurrent.futures import ThreadPoolExecutor
from concurrent.futures import as_completed


class MyTestCase(unittest.TestCase):

    def test_entity_creation(self):
        """
        creates arbitrary size of threads(k=10) and distributes each input instance to different thread and performs the insertion asynchronously.
        :return:
        """
        file_path = Constants.TEST_ENTITY_FILE
        file_name = os.path.basename(file_path).split(Constants.DOT)[0]
        file_name_split = file_name.split(Constants.UNDERSCORE)
        entity_rows = extract_data_from_csv(file_path)
        label = get_label(file_name_split)
        with GraphDatabase.driver(Constants.URI, auth=(Constants.USERNAME, Constants.PASSWORD)) as driver:
            with ThreadPoolExecutor(10) as executor:  # creating a pool of threads with size 10
                futures = [executor.submit(RemoteGraphConstruction.insert_entity_instance, driver, label, entity) for
                           entity in entity_rows]  # executor is responsible for submitting input to different threads.
                for future in as_completed(futures):
                    self.assertEqual(future.result(), True)

    def test_relation_creation(self):
        """
        creates arbitrary size of threads(k=10) and distributes each input instance to different thread and performs the insertion asynchronously.
        :return:
        """
        file_path = Constants.TEST_REL_FILE
        entity_rows = extract_data_from_csv(file_path)
        file_name = os.path.basename(file_path).split(Constants.DOT)[0]
        subject_label, predict_label = get_subject_predict_label(file_name)
        with GraphDatabase.driver(Constants.URI, auth=(Constants.USERNAME, Constants.PASSWORD)) as driver:
            with ThreadPoolExecutor(10) as executor:  # creating a pool of threads with size 10
                futures = [executor.submit(RemoteGraphConstruction.insert_relationship_instance, driver, subject_label,
                                           predict_label, parse_relation(rel_entity)) for
                           rel_entity in
                           entity_rows]  # executor is responsible for submitting input to different threads.
                for future in as_completed(futures):
                    self.assertEqual(future.result(), True)


if __name__ == '__main__':
    unittest.main()
