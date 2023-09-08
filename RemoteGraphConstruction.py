from main import check_relationship
from main import create_relationship_helper
from cymple import QueryBuilder
import logging
import Constants


def insert_entity_instance(graph_db_driver, entity_name, entity_instance):
    """
    Creates entity instance and stores into the database
    :param graph_db_driver: fully configured remote/local database connection
    :param entity_name: label( identifier to a Node)
    :param entity_instance: a key-value map/dictionary consists of properties of the node.
    :return:
    """
    try:
        with graph_db_driver.session(database='neo4j') as session:  # a session is lightweight operation.
            result = session.execute_write(insert_entity_tx, entity_name, entity_instance)
            return result
    except Exception as ex:
        logging.exception('exception, %s, occurred while creating node' % ex)
        return False


def insert_entity_tx(tx, entity_name, entity):
    """

    :param tx: transaction is an atomic operation which means will either be executed as whole or not at all
    :param entity_name: label name
    :param entity: key-value map (properties of node).
    :return: True, if node creation is successful.
    """
    qb = QueryBuilder()
    id_match_query = qb.match().node(labels=entity_name, ref_name='n',
                                     properties={Constants.ID: str(entity[Constants.ID])}).return_literal(
        'n')
    result = tx.run(id_match_query)
    record = result.single()
    if record is not None:
        raise Exception("Node ID with given label is already exists in database")
    create_node_query = qb.create().node(labels=entity_name, ref_name='n', properties=entity).return_literal(
        'n')
    result = tx.run(create_node_query)
    record = result.single()
    if record is not None:
        return True
    else:
        return False


def insert_relationship_instance(graph_db_driver, subject_entity_name, predict_entity_name,
                                 rel_instance):
    """

    :param graph_db_driver: fully configured remote/local database connection
    :param subject_entity_name: label name of the subject node
    :param predict_entity_name: label name of the predict/object node.
    :param rel_instance: The list of key-value pairs present in the map:
                         1 Subject ID: id of the subject
                         2 Name: label name of the relationship
                         3 Functional: characteristic of the relationship.The value of this key is either 0(Not Functional) or 1(functional)
                         4 Inverse Functional: characteristic of the relationship.The value of this key is either 0(Not Inverse) or 1(Inverse)
                         5 Transitive: characteristic of the relationship.The value of this key is either 0(Not Transitive) or 1(Transitive)
                         6 Symmetric: characteristic of the relationship.The value of this key is either 0(Not Symmetric) or 1(Symmetric)
                         7 Asymmetric: characteristic of the relationship.The value of this key is either 0(Not Asymmetric) or 1(Asymmetric)
                         8 Reflexive:  characteristic of the relationship.The value of this key is either 0(Not Reflexive) or 1(Reflexive)
                         9 Irreflexive: characteristic of the relationship.The value of this key is either 0(Not Irreflexive) or 1(Irreflexive)
                         10 Predict ID: id of the predict/object
    :return: True, if relationship creation is successful
    """
    try:
        print(rel_instance[Constants.SUBJECT])
        with graph_db_driver.session(database='neo4j') as session:
            result = session.execute_write(insert_relation_tx, subject_entity_name, predict_entity_name,
                                           rel_instance)
            return result
    except Exception as ex:
        logging.exception('exception, %s, occurred while creating relationship' % ex)
        return False


def insert_relation_tx(tx, subject_label, predict_label, relation):
    """

    :param tx: transaction is an atomic operation which means will either be executed as whole or not at all
    :param subject_label: label name of the subject node
    :param predict_label: label name of the predict/object node.
    :param relation: properties of the relation
    :return: True, if relationship creation is successful
    """
    is_valid = check_relationship(relation, subject_label,
                                  predict_label)  # validates existence of subject and predict in the database and  satisfiability of characteristic
    if not is_valid:
        raise Exception('RelationCannotBeFormed')
    try:
        create_relation_query = create_relationship_helper(subject_label, predict_label,
                                                           relation)  # this helper function builds the query for inserting relationship
        tx.run(create_relation_query)
        return True
    except Exception as ex:
        logging.exception('exception, %s, occurred while creating relationship' % ex)
        return False
