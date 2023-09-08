# This is a sample Python script.
from threading import Lock

import Constants
import csv
from cymple import QueryBuilder
import os
import logging

from GraphRepo import GraphRepo


def insert_relationship_instance(graph_db_driver, subject_entity_name, predict_entity_name, rel_instance_dictionary):
    try:
        with graph_db_driver.session(database='neo4j') as session:
            for rel_instance in rel_instance_dictionary:
                result = session.execute_write(insert_relation_tx, subject_entity_name, predict_entity_name,
                                               rel_instance)
                return result
    except Exception as ex:
        logging.exception('exception, %s, occurred while creating relationship' % ex)
        return False


def insert_relation_tx(tx, subject_label, predict_label, relation):
    is_valid = check_relationship(relation, subject_label, predict_label)
    if not is_valid:
        raise Exception('RelationCannotBeFormed')
    lock = Lock()
    with lock:
        try:
            create_relation_query = create_relationship_helper(subject_label, predict_label, relation)
            tx.run(create_relation_query)
            return True
        except Exception as ex:
            logging.exception('exception, %s, occurred while creating relationship' % ex)
            return False


def insert_entity_instance(graph_db_driver, entity_name, entity_instance_dictionary):
    try:
        with graph_db_driver.session(database='neo4j') as session:
            for entity in entity_instance_dictionary:
                result = session.execute_write(insert_entity_tx, entity_name, entity)
                return result
    except Exception as ex:
        logging.exception('exception, %s, occurred while creating node' % ex)
        return False


def insert_entity_tx(tx, entity_name, entity):
    qb = QueryBuilder()
    id_match_query = qb.match().node(labels=entity_name, ref_name='n',
                                     properties={Constants.ID: str(entity[Constants.ID])}).return_literal(
        'n')
    result = tx.run(id_match_query)
    record = result.single()
    if record is not None:
        raise Exception("Node ID with given label is already exists in database")
    lock = Lock()
    with lock:
        create_node_query = qb.create().node(labels=entity_name, ref_name='n', properties=entity).return_literal('n')
        result = tx.run(create_node_query)
        record = result.single()
        if record is not None:
            return True
        else:
            return False


def extract_data_from_csv(path):
    rows = []
    with open(path, newline='') as csv_file:
        data = csv.DictReader(csv_file)
        for row in data:
            rows.append(row)
    return rows


def is_id_exists(node_id, node_label):
    qb = QueryBuilder()
    query = qb.match().node(labels=node_label, ref_name='n', properties={Constants.ID: str(node_id)}).return_literal(
        'n')
    try:
        records, summary, keys = GraphRepo.execute_query(query, routing_control=Constants.READ)
        return len(records) > 0
    except Exception as ex:
        logging.exception("exception, %s, occurred while checking the existence of node" % ex)


def create_node(node_label, entity_rows):
    for entity in entity_rows:
        if not is_id_exists(entity[Constants.ID], node_label):
            qb = QueryBuilder()
            query = qb.create().node(labels=label, properties=entity)
            logging.info("executing query %s" % query)
            try:
                GraphRepo.execute_query(query, routing_control=Constants.WRITE)
            except Exception as e:
                logging.exception("An error, %s, occurred while creating a node." % e)


def create_relationship_helper(l_label, r_label, rel_entity):
    qb = QueryBuilder()
    query_match_left = qb.match() \
        .node(labels=l_label, ref_name='l') \
        .where('l.' + Constants.ID, Constants.EQUALS, rel_entity[Constants.SUBJECT])
    query_match_right = qb.match() \
        .node(labels=r_label, ref_name='r') \
        .where('r.' + Constants.ID, Constants.EQUALS, rel_entity[Constants.PREDICT])
    query_create_rel = qb.create() \
        .node(ref_name='l') \
        .related_to(ref_name='rel', label=rel_entity[Constants.NAME]) \
        .node(ref_name='r')
    final_query = query_match_left + query_match_right + query_create_rel;
    return final_query


def get_label(name_split):
    name_split = [name.capitalize() for name in name_split]
    return '_'.join(name_split[1:])


def parse_relation(relation_row):
    relation = {}
    for key, value in relation_row.items():
        split_key = key.split(Constants.DOLLAR)
        if split_key[0] == Constants.SUBJECT:
            relation[Constants.SUBJECT] = value
        elif split_key[0] == Constants.PREDICT:
            relation[Constants.PREDICT] = value
        elif split_key[1] == Constants.NAME:
            relation[Constants.NAME] = str(value).upper()
        elif split_key[1] == Constants.TYPE:
            relation[str(split_key[2]).upper()] = value
    return relation


def is_functional_satisfies(node, label, relation):
    properties = {Constants.ID: node}
    qb = QueryBuilder()
    query = qb.match().node(labels=label, ref_name='s', properties=properties) \
        .related_to(label=relation, ref_name='r') \
        .node(labels=label, ref_name='p').return_literal('p')
    try:
        records, summary, keys = GraphRepo.execute_query(query, routing_control=Constants.READ)
    except Exception as ex:
        logging.exception("an exception, %s, occurred while executing the query" % ex)
        return False
    return len(records) == 0


def check_relationship(relation, subject_label, predict_label):
    if not is_id_exists(relation[Constants.SUBJECT], subject_label):
        return False
    if not is_id_exists(relation[Constants.PREDICT], predict_label):
        return False
    if relation[Constants.FUNCTIONAL] == '1' and not is_functional_satisfies(relation[Constants.SUBJECT], subject_label,
                                                                             relation[Constants.NAME]):
        return False
    if relation[Constants.INVERSE_FUNCTIONAL] == '1' and not is_functional_satisfies(relation[Constants.PREDICT],
                                                                                     predict_label,
                                                                                     relation[Constants.NAME]):
        return False
    return True


def create_relationship(relation_rows, subject_label, predict_label):
    success = 0
    failure = 0
    for relation in relation_rows:
        parsed_relation = parse_relation(relation)
        can_form_relationship = check_relationship(parsed_relation, subject_label, predict_label)
        if not can_form_relationship:
            logging.info(
                " either subject_id %s or object_id %s doesn't exist in database" % (subject_label, predict_label))
            failure += 1
        else:
            try:
                query = create_relationship_helper(subject_label, predict_label, parsed_relation)
                GraphRepo.execute_query(query, routing_control=Constants.WRITE)
            except Exception as ex:
                logging.exception(
                    "exception  %s occurred while creating relationship between %s and %s" % (
                        ex, subject_label, predict_label))
                failure += 1
            else:
                success += 1
    return success, failure


def get_subject_predict_label(file_name):
    split_name = file_name.split(Constants.HYPHEN)
    labels = []
    for name in split_name:
        if str(name).startswith(Constants.ENTITY):
            labels.append(get_label(name.split(Constants.UNDERSCORE)))
    return labels[0], labels[1]


if __name__ == '__main__':
    file_path = input('Enter the path of the file- do not enclose path in single/double quotes: ')
    file_name = os.path.basename(file_path)
    file_name, extension = file_name.split(Constants.DOT)
    try:
        if str(file_name).startswith(Constants.ENTITY):
            entity_rows = extract_data_from_csv(str(file_path))
            file_name_split = file_name.split(Constants.UNDERSCORE)
            label = get_label(file_name_split)
            create_node(label, entity_rows)
        elif str(file_name).startswith(Constants.RELATION):
            relation_rows = extract_data_from_csv(str(file_path))
            print(relation_rows)
            subject_label, predict_label = get_subject_predict_label(file_name)
            success, failure = create_relationship(relation_rows, subject_label, predict_label)
            print("relationship status : success = " + str(success) + " failure = " + str(failure))
        else:
            print("invalid file format")
    except Exception as ex:
        logging.exception('an error occurred while constructing graph')
        GraphRepo.close_connections()
    else:
        GraphRepo.close_connections()
