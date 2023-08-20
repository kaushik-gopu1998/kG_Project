# This is a sample Python script.
import Constants
import csv
from cymple import QueryBuilder

from GraphRepo import GraphRepo


# extracting data from csv file
def extract_data_from_csv(path):
    rows = []
    with open(path, newline='') as csv_file:
        data = csv.DictReader(csv_file)
        for row in data:
            rows.append(row)
    return rows


def extract_left_right_relationship_info(data):
    left_entity_info = {}
    right_entity_info = {}
    relationship_props = {}
    for key, value in data.items():
        split_key = key.split(Constants.SEPARATOR)
        entity_type = split_key[0]
        if entity_type == Constants.LEFT_ENTITY:
            left_entity_info[split_key[2]] = value
            left_entity_info[Constants.LABEL] = split_key[1]
        elif entity_type == Constants.RIGHT_ENTITY:
            right_entity_info[split_key[2]] = value
            right_entity_info[Constants.LABEL] = split_key[1]
        elif entity_type == Constants.RELATIONSHIP:
            attribute = split_key[2] if len(split_key) == 3 else split_key[1]
            relationship_props[attribute] = value
    return left_entity_info, right_entity_info, relationship_props


def is_id_exists(node_id, label):
    print(node_id+" "+label)
    graph_repo = GraphRepo()
    qb = QueryBuilder()
    query = qb.match().node(labels=label, ref_name='n', properties={Constants.ID: str(node_id)}).return_literal('n')
    records = graph_repo.execute_query(query)
    return True


def create_node(entity):
    graph_repo = GraphRepo()
    label = entity[Constants.LABEL]
    entity.pop(Constants.LABEL, None)
    node_id = entity[Constants.ID]
    exists = is_id_exists(node_id, label)
    if not exists:
        qb = QueryBuilder()
        query = qb.create().node(labels=label, properties=entity)
        try:
            print('executing query...')
            graph_repo.execute_query(query)
        except Exception as e:
            print("an error occurred while creating a node"+ e)


def construct_kg(l_entity, r_entity, rel_entity):
    l_label = l_entity[Constants.LABEL]
    r_label = r_entity[Constants.LABEL]
    create_node(l_entity)
    create_node(r_entity)
    qb = QueryBuilder()
    query_match_left = qb.match() \
        .node(labels=l_label, ref_name='l') \
        .where('l.'+Constants.ID, Constants.EQUALS, l_entity[Constants.ID])
    query_match_right = qb.match() \
        .node(labels=r_label, ref_name='r') \
        .where('r.'+Constants.ID, Constants.EQUALS, r_entity[Constants.ID])
    query_create_rel = qb.create() \
        .node(ref_name='l') \
        .related_to(ref_name='rel', label=rel_entity['name']) \
        .node(ref_name='r')
    final_query = query_match_left + query_match_right + query_create_rel;
    graph_repo = GraphRepo()
    try:
        graph_repo.execute_query(final_query)
    except Exception as e:
        print("an error occurred while creating relationship."+e)


if __name__ == '__main__':
    formatted_data = extract_data_from_csv(Constants.FILE_PATH)
    for each_row in formatted_data:
        left_entity, right_entity, relationship_entity = extract_left_right_relationship_info(each_row)
        construct_kg(left_entity, right_entity, relationship_entity)
        break
