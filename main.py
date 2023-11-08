# This is a sample Python script.
from threading import Lock

import Constants
import csv
from cymple import QueryBuilder
import os
import logging
import re
from GraphRepo import GraphRepo


def extract_data_from_csv(path):
    rows = []
    with open(path, newline='', errors='ignore') as csv_file:
        data = csv.DictReader(csv_file)
        for row in data:
            rows.append(row)
    return rows


def is_id_exists(node_id, node_label):
    qb = QueryBuilder()
    query = qb.match().node(labels=node_label, ref_name='n', properties={Constants.ID: str(node_id)}).return_literal(
        'n')
    try:
        records, summary, keys = GraphRepo.execute_query(query)
        return len(records) > 0
    except Exception as ex:
        logging.exception("exception, %s, occurred while checking the existence of node" % ex)


def create_single_node(node_label, props):
    if not is_id_exists(props[Constants.ID], node_label):
        qb = QueryBuilder()
        query = qb.create().node(labels=node_label, properties=props)
        try:
            GraphRepo.execute_query(query)
        except Exception as e:
            logging.exception("An error, %s, occurred while creating a node." % e)


def create_node(node_label, entity_rows):
    for entity in entity_rows:
        if not is_id_exists(entity[Constants.ID], node_label):
            qb = QueryBuilder()
            query = qb.create().node(labels=label, properties=entity)
            logging.info("executing query %s" % query)
            try:
                GraphRepo.execute_query(query)
            except Exception as e:
                logging.exception("An error, %s, occurred while creating a node." % e)


def create_relationship_query(l_label, r_label, rel_entity):
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
        records, summary, keys = GraphRepo.execute_query(query)
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
                query = create_relationship_query(subject_label, predict_label, parsed_relation)
                GraphRepo.execute_query(query)
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


def get_post_rect(username, post_data):
    for rec in post_data:
        if rec[Constants.USERNAME] == username:
            return rec
    return None


def insert_student_feedback(feedback_file):
    feedback_entities = extract_data_from_csv(feedback_file)
    for entity_row in feedback_entities:
        qb = QueryBuilder()
        query = qb.create().node(labels=Constants.KNOWLEDGE_TICKET, properties=entity_row)
        try:
            GraphRepo.execute_query(query)
        except ex:
            logging.exception("an exception, %s, occurred while executing the query" % ex)


def get_answer(data):
    for key, val in data.items():
        if str(key).startswith('question'):
            return val
    return ''


def get_post_data(student_id, exit_data):
    for data in exit_data:
        if data[Constants.ID] == student_id:
            return data
    return None


def parse(ans):
    ans = re.sub(' ', '', ans).lower()
    ans = re.sub('\n', '', ans).lower()
    ans = re.sub('\t', '', ans).lower()
    return ans


def get_score(student_answer, correct_options):
    student_answer = parse(student_answer)
    correct = 0
    miss_hits = 0
    for correct_option in correct_options:
        if correct_option == '' or correct_option is None:
            continue
        correct_option = parse(correct_option)
        if correct_option in student_answer:
            correct += 1
        else:
            miss_hits += 1
    total_co = len(correct_options)
    if correct == 0:
        return 0.0
    if float(correct) / float(total_co) >= 0.1 and miss_hits > 0:
        return (100 * float(correct) / float(total_co)) - 10
    if miss_hits == 0:
        return 100 * float(correct) / float(total_co)
    if float(correct) / float(total_co) < 0.1 and miss_hits > 0:
        return 0.0
    return float(correct) / float(total_co)


def get_week_and_checkpoint(entry_file_name):
    list = entry_file_name.split(" ")
    week = ''
    checkpoint = ''
    for name in list:
        if str(name).startswith("W"):
            week = name
        if str(name).lower() == 'entry' or str(name).lower() == 'exit':
            checkpoint = name
    return str(week).lower(), str(checkpoint).lower()


def get_correct_options(answers_data, entry_file_name):
    week, checkpoint = get_week_and_checkpoint(entry_file_name)
    if week == '' or checkpoint == '':
        return None
    options = []
    for data in answers_data:
        title = data[Constants.TICKET_TITLE]
        if week in str(title).lower():  # ask professor whether entry and exit questions will be same or not
            for key, val in data.items():
                if Constants.ANSWER in str(key).lower() and val != '' and val is not None:
                    options.append(val)
            return options
    return None


def get_abs_gain(pre_score, post_score):
    return post_score - pre_score


def get_norm_gain(pre_score, post_score, norm_type):
    if norm_type == Constants.NORM_ONE and 100 - pre_score != 0:
        return float(post_score - pre_score) / float(100 - pre_score)
    elif norm_type == Constants.NORM_TWO and pre_score != 0:
        return float(post_score - pre_score) / float(pre_score)
    return 0.0


def get_sym_gain(pre_score, post_score, sym_type):
    if sym_type == Constants.SYM_TWO and pre_score != 0 and post_score != 0:
        return float(post_score - pre_score) / float(pre_score + post_score)
    return 0.0


def get_wt_gain(pre_score, post_score):
    return (post_score - pre_score) * pre_score / Constants.MUE


def get_student_props(student_id, student_name):
    # first_name, last_name = student_name.split(" ")
    student_dict = {Constants.ID: student_id, Constants.NAME: student_name}
    return student_dict


def create_student_learn_gain_rel(student_props, learn_gain_props):
    create_single_node('Student', student_props)
    qb = QueryBuilder()
    query_left = qb.match() \
        .node(labels='Student', ref_name='l').where('l.' + Constants.ID, Constants.EQUALS, student_props[Constants.ID])
    query_right = qb.create() \
        .node(labels='Learn_Gain', ref_name='r', properties=learn_gain_props)
    query_create_rel = qb.create() \
        .node(ref_name='l') \
        .related_to(ref_name='rel', label='HAS_LEARN_GAIN') \
        .node(ref_name='r')
    final_query = query_left + query_right + query_create_rel
    try:
        GraphRepo.execute_query(query=final_query)
    except:
        logging.error('an error occurred while inserting student-learn gain')


def insert_learning_gain(entry_file, exit_file, answers_for_tickets):
    entry_data = extract_data_from_csv(entry_file)  # parse entry data
    exit_data = extract_data_from_csv(exit_file)  # parse exit data
    answers_data = extract_data_from_csv(answers_for_tickets)  # parse answers data
    entry_file_name = get_file_name(entry_file)
    exit_file_name = get_file_name(exit_file)
    for pre_data in entry_data:
        student_id = pre_data[Constants.ID]  # extract student id
        student_name = pre_data[Constants.NAME]  # extract student name
        pre_answer = get_answer(pre_data)  # get pre answers given by student
        post_data = get_post_data(student_id, exit_data)  # get post answers given by student
        if post_data is None:  # if there is no post data for student, learn gain cannot be computed
            continue
        post_answer = get_answer(post_data)  # get post answers given by student
        # ask professor if entry and exit questions will be same or not
        correct_options = get_correct_options(answers_data,
                                              entry_file_name)  # extract all correct options for the given question
        if correct_options is None:
            print("answers file does not contains correct options info for this week")
        pre_score = float(get_score(pre_answer, correct_options))  # compute pre score
        post_score = float(get_score(post_answer, correct_options))  # compute post score
        abs_gain = get_abs_gain(pre_score, post_score)  # compute abs
        norm_gain_one = get_norm_gain(pre_score, post_score, Constants.NORM_ONE)  # compute norm 1
        norm_gain_two = get_norm_gain(pre_score, post_score, Constants.NORM_TWO)  # compute norm 2
        sym_gain_two = get_sym_gain(pre_score, post_score, Constants.SYM_ONE)  # compute sym 2
        wt_gain = get_wt_gain(pre_score, post_score)  # compute weight gain
        learn_gain_props = {}
        en_week, en_checkpoint = get_week_and_checkpoint(entry_file_name)  # extracting entry id from file name
        ex_week, ex_checkpoint = get_week_and_checkpoint(exit_file_name)  # extracting exit id from file name
        en_id = en_week + " " + en_checkpoint  # creating entry id
        ex_id = ex_week + " " + ex_checkpoint  # creating exit id
        learn_gain_props[Constants.ENTRY_ID] = en_id  # properties of learn gain
        learn_gain_props[Constants.EXIT_ID] = ex_id  # properties of learn gain
        learn_gain_props[Constants.ABS_GAIN] = abs_gain  # properties of learn gain
        learn_gain_props[Constants.NORM_GAIN_ONE] = norm_gain_one  # properties of learn gain
        learn_gain_props[Constants.NORM_GAIN_TWO] = norm_gain_two  # properties of learn gain
        learn_gain_props[Constants.SYM_GAIN_TWO] = sym_gain_two  # properties of learn gain
        learn_gain_props[Constants.WT_GAIN] = wt_gain  # properties of learn gain
        student_props = get_student_props(student_id, student_name)  # get student properties such as name, id
        try:
            GraphRepo.check_connectivity()  # check database connection
        except Exception as ex:
            logging.exception(ex)
        else:
            create_student_learn_gain_rel(student_props,
                                          learn_gain_props)  # create student , learn gain and relation between student and learn gain


def get_post_score(exit_data, id):
    for post_data in exit_data:
        if post_data[Constants.ID] == id:
            return post_data[Constants.POINTS]
    return -1.0


def create_ticket_id(entry_file_name):
    # entryticketsessionw6a format
    name_split = entry_file_name.split(" ")
    ticket_id = ''
    for name in name_split:
        if str(name).lower() == 'exit' or str(name).lower() == 'entry':
            ticket_id += str(name).lower()
        if str(name).lower() == 'ticket':
            ticket_id += str(name).lower()
        if str(name).lower() == 'session':
            ticket_id += str(name).lower()
        if str(name).lower().startswith('w'):
            ticket_id += str(name).lower()
    return ticket_id


def get_all_questions(data):
    questions = {}
    for key, val in data.items():
        if str(key).startswith('question'):
            arr = key.split(':')
            questions[arr[1]] = {Constants.ID: arr[1], Constants.QUESTION: arr[2]}
    return questions


def get_assesment_id(file_name):
    split = file_name.split('-')
    print(split)
    for sp in split:
        if str(sp).lower().startswith(Constants.QUIZ):
            return str(sp).lower()
    return ""


def get_course_instance(data):
    for key, val in data.items():
        if key == Constants.SECTION:
            return val


def create_relation_instance_assessment_question(instance_props, assessment_props, all_questions):
    qb = QueryBuilder()
    query_instance = qb.match() \
        .node(labels=Constants.COURSE_INSTANCE, ref_name='i').where('i.' + Constants.ID, Constants.EQUALS,
                                                                    instance_props[Constants.ID])
    query_assessment = qb.match() \
        .node(labels=Constants.ASSESSMENT, ref_name='a').where('a.' + Constants.ID, Constants.EQUALS,
                                                               assessment_props[Constants.ID])
    query_create_rel = qb.create() \
        .node(ref_name='i') \
        .related_to(ref_name='rel', label='Has_Assessment_Question') \
        .node(ref_name='a')
    final_query = query_instance + query_assessment + query_create_rel
    try:
        GraphRepo.execute_query(query=final_query)
    except:
        logging.error('an error occurred while inserting student-learn gain')
    else:
        for key, question_props in all_questions.items():
            query_instance = qb.match() \
                .node(labels=Constants.COURSE_INSTANCE, ref_name='i').where('i.' + Constants.ID, Constants.EQUALS,
                                                                            instance_props[Constants.ID])
            query_question = qb.create() \
                .node(labels=Constants.QUESTION, ref_name='q', properties=question_props)

            query_create_rel = qb.create() \
                .node(ref_name='i') \
                .related_to(ref_name='rel', label='Has_Assessment_Question') \
                .node(ref_name='q')
            final_query = query_instance + query_question + query_create_rel
            try:
                GraphRepo.execute_query(query=final_query)
            except:
                print('question : ' + key + ' not inserted')


def create_relation_instance_student(instance_props, student_props):
    create_single_node('Student', student_props)
    qb = QueryBuilder()
    query_instance = qb.match() \
        .node(labels=Constants.COURSE_INSTANCE, ref_name='i', properties=instance_props).where('i.' + Constants.ID,
                                                                                               Constants.EQUALS,
                                                                                               instance_props[
                                                                                                   Constants.ID])
    query_student = qb.match() \
        .node(labels='Student', ref_name='s').where('s.' + Constants.ID, Constants.EQUALS,
                                                    student_props[Constants.ID])
    query_create_rel_1 = qb.create() \
        .node(ref_name='s') \
        .related_to(ref_name='rel1', label='enrolled_in') \
        .node(ref_name='i')
    query_create_rel_2 = qb.create() \
        .node(ref_name='i') \
        .related_to(ref_name='rel2', label='enrolled_by') \
        .node(ref_name='s')
    final_query = query_student + query_instance + query_create_rel_1 + query_create_rel_2
    try:
        GraphRepo.execute_query(query=final_query)
    except Exception as ex:
        print(ex)


def insert_course_instance_assessment_question(assessment_file):
    name = os.path.basename(assessment_file)
    name, ext = name.split(Constants.DOT)
    assessment_id = get_assesment_id(name)
    assessment_data = extract_data_from_csv(assessment_file)
    print('assessment_id = ' + assessment_id)
    all_questions = {}
    for data in assessment_data:
        all_questions = get_all_questions(data)
        course_instance = get_course_instance(data)
        break
    print(all_questions)
    print('instance :' + course_instance + ' creating....')
    instance_props = {Constants.ID: course_instance}
    create_single_node(Constants.COURSE_INSTANCE, instance_props)
    print('creating instances for questions.....')
    for key, val in all_questions.items():
        question_props = val
        print('question props...')
        print(question_props)
        create_single_node(Constants.QUESTION, question_props)
    print('Creating instances for assessment')
    assessment_props = {Constants.ID: assessment_id}
    create_single_node(Constants.ASSESSMENT, assessment_props)
    create_relation_instance_assessment_question(instance_props, assessment_props, all_questions)
    for data in assessment_data:
        print('************extracting student information*************')
        student_name = data['name']
        student_id = data['id']
        student_props = {Constants.ID: student_id, Constants.NAME: student_name}
        section_instance = data[Constants.COURSE]
        score = data[Constants.SCORE]
        print('student_name :' + student_name)
        print('student_id: ' + student_id)
        print('section_instance: ' + section_instance)
        print("score: " + score)
        create_relation_instance_student(instance_props, student_props)


def assessment_input():
    ass_file = input('enter assessment file')
    insert_course_instance_assessment_question(ass_file)


def create_student_learn_gain_kg_ticket_rel(student_props, learn_gain_props, entry_id, exit_id):
    create_single_node('Student', student_props)
    if not is_id_exists(entry_id, 'Knowledge_Ticket'):
        print('entry id does not exists in database')
        return
    qb = QueryBuilder()
    # get student node
    query_node_1 = qb.match() \
        .node(labels='Student', ref_name='s') \
        .where('s.' + Constants.ID, Constants.EQUALS,
               student_props[Constants.ID])
    # get outcome node
    query_node_3 = qb.match() \
        .node(labels='Knowledge_Ticket', ref_name='en') \
        .where('en.' + Constants.ID, Constants.EQUALS,
               entry_id)
    # create relation: knowledge_ticket->session<->learning_outcome
    query_node_4 = qb.create() \
        .node(ref_name='s') \
        .related_to(ref_name='rel', label='HAS_LEARN_GAIN') \
        .node(ref_name='l') \
        .related_to(ref_name="rel2", label='BELONGS_TO_TICKET') \
        .node(ref_name='en')
    # create learn gain node
    query_node_2 = qb.create() \
        .node(labels='Learn_Gain', ref_name='l', properties=learn_gain_props)
    final_query = query_node_1 + query_node_3 + query_node_2 + query_node_4
    try:
        # run query
        GraphRepo.execute_query(final_query)
    except Exception as e:
        logging.exception("An error, %s, occurred while creating a node." % e)


def insert_learning_gain_new(entry_file, exit_file):
    entry_data = extract_data_from_csv(entry_file)  # parse entry data
    exit_data = extract_data_from_csv(exit_file)  # parse exit data
    entry_file_name = get_file_name(entry_file)
    exit_file_name = get_file_name(exit_file)
    for pre_data in entry_data:
        student_id = pre_data[Constants.ID]  # extract student id
        student_name = pre_data[Constants.NAME]  # extract student name
        pre_score = float(pre_data[Constants.POINTS])  # extract pre score
        post_score = float(get_post_score(exit_data, student_id))  # extract post score
        if post_score == -1.0:  # this means student doesn't participate in post survey quiz
            continue
        print("pre score")
        print(pre_score)
        print("post score")
        print(post_score)
        abs_gain = get_abs_gain(pre_score, post_score)  # compute abs
        norm_gain_one = get_norm_gain(pre_score, post_score, Constants.NORM_ONE)  # compute norm 1
        norm_gain_two = get_norm_gain(pre_score, post_score, Constants.NORM_TWO)  # compute norm 2
        sym_gain_two = get_sym_gain(pre_score, post_score, Constants.SYM_ONE)  # compute sym 2
        wt_gain = get_wt_gain(pre_score, post_score)  # compute weight gain
        learn_gain_props = {Constants.ABS_GAIN: abs_gain, Constants.NORM_GAIN_ONE: norm_gain_one,
                            Constants.NORM_GAIN_TWO: norm_gain_two, Constants.SYM_GAIN_TWO: sym_gain_two,
                            Constants.WT_GAIN: wt_gain}
        student_props = get_student_props(student_id, student_name)  # get student properties such as name, id
        entry_id = create_ticket_id(entry_file_name)
        exit_id = create_ticket_id(exit_file_name)
        print('exit id = ' + exit_id)
        print('entry id = ' + entry_id)
        try:
            GraphRepo.check_connectivity()  # check database connection
        except Exception as ex:
            logging.exception(ex)
        else:
            create_student_learn_gain_kg_ticket_rel(student_props,
                                                    learn_gain_props, entry_id,
                                                    exit_id)  # create student , learn gain and relation between student and learn gain


def create_session_id(ticket, ticket_title):
    session_id = ""
    session = ticket.get(Constants.Session, "")
    week = ticket.get(Constants.WEEK, -1)
    if str(ticket_title).startswith(Constants.ENTRY):
        session_id = Constants.ENTRY + "-" + week + session  # ex: entry_5A
    elif str(ticket_title).startswith(Constants.EXIT):
        session_id = Constants.EXIT + "-" + week + session
    return session_id


def create_relation_ticket_and_session_and_outcome_question(ticket, session_props, outcome_props, question_props):
    qb = QueryBuilder()
    # get ticket node
    query_node_1 = qb.match() \
        .node(labels='Knowledge_Ticket', ref_name='k') \
        .where('k.' + Constants.ID, Constants.EQUALS,
               ticket[Constants.TICKET_TITLE])
    # get session node
    query_node_2 = qb.match() \
        .node(labels='Session', ref_name='s') \
        .where('s.' + Constants.ID, Constants.EQUALS, session_props[Constants.ID])
    # get outcome node
    query_node_3 = qb.match() \
        .node(labels='Learning_Outcome', ref_name='o') \
        .where('o.' + Constants.ID, Constants.EQUALS,
               outcome_props[Constants.ID])
    # get question node
    query_node_4 = qb.match() \
        .node(labels='Question', ref_name='q') \
        .where('q.' + Constants.ID, Constants.EQUALS,
               question_props[Constants.ID])
    # create relation: knowledge_ticket->session<->learning_outcome
    query_node_5 = qb.create() \
        .node(ref_name='k') \
        .related_to(ref_name='rel', label='BELONGS_TO') \
        .node(ref_name='s') \
        .related_to(ref_name="rel2", label='HAS_OUTCOME') \
        .node(ref_name='o') \
        .related_to(ref_name='rel3', label='IS_ASSESSED_BY') \
        .node(ref_name='q') \
        .related_to(ref_name='rel4', label='ASSESSES') \
        .node(ref_name='o')
    final_query = query_node_1 + query_node_2 + query_node_3 + query_node_4 + query_node_5
    try:
        # run query
        GraphRepo.execute_query(final_query)
    except Exception as e:
        logging.exception("An error, %s, occurred while creating a node." % e)


def parse_ticket(ticket):
    for key, val in ticket.items():
        ticket[key] = parse(val)


def filter_ticket(ticket):
    ticket.pop(Constants.OUTCOMES)
    ticket_copy = dict(ticket).copy();
    for key, val in ticket_copy.items():
        if str(key).startswith("Q"):
            ticket.pop(key)


def insert_ticket_session_and_outcomes(ticket_file):
    tickets_data = extract_data_from_csv(ticket_file)
    for ticket in tickets_data:
        parse_ticket(ticket)
        ticket_title = ticket[Constants.TICKET_TITLE]
        session_id = create_session_id(ticket, ticket_title)
        outcomes = ticket[Constants.OUTCOMES]
        ticket[Constants.ID] = ticket_title
        session_props = {Constants.ID: session_id}  # session node props
        outcome_props = {Constants.OUTCOMES: outcomes, Constants.ID: session_id}  # outcome node props
        question_props = {Constants.ID: session_id, Constants.QUESTION: ticket[Constants.QUESTION]}
        filter_ticket(ticket)  # remove unwanted data such as outcomes and questions
        print("session props : %s" % session_props)
        print("ticket props : %s" % ticket)
        print("outcome props : %s" % outcome_props)
        print("question props : %s" % question_props)
        print(
            "*******************************************************************end of one record***************************************************************************" + "\n")
        create_single_node('Knowledge_Ticket', ticket)  # creating node for ticket
        create_single_node('Session', session_props)  # creating node for session
        create_single_node('Learning_Outcome', outcome_props)  # creating node for outcome
        create_single_node(Constants.QUESTION, question_props)  # creating node for question
        create_relation_ticket_and_session_and_outcome_question(ticket, session_props, outcome_props, question_props)
def get_file_name(file_path):
    file_name = os.path.basename(file_path)
    file_name, extension = file_name.split(Constants.DOT)
    return file_name

def get_feedback(course_id, student_id):

def input_learning_gain():
    entry_file = input('enter input file')
    exit_file = input('enter exit file')
    answers_for_tickets = input('enter answers for tickets')
    insert_learning_gain(str(entry_file), str(exit_file), str(answers_for_tickets))


def input_answers_for_tickets():
    ans_file = input('enter input file')
    insert_ticket_session_and_outcomes(ans_file)


def input_new_learn_gain():
    entry_file = input('entry file')
    exit_file = input('exit_file')
    insert_learning_gain_new(entry_file, exit_file)


if __name__ == '__main__':
    # input_answers_for_tickets()
    # input_new_learn_gain()
    # assessment_input()
    exit(0)
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
