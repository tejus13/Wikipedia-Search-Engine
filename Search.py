import sys
from Stemmer import Stemmer
import csv
import Preprocess as pre
import re
import operator
import pickle
import time
import os

ps = Stemmer('porter')


def preprocess_query(stop_words, query):
    """
    Preprocess the query and return the processed query words
    :param query: raw query
    :return: processed query
    """
    table = str.maketrans(' ', ' ', '!"\'()*+,;<>[\\]^`{|}~=%&_#?')
    query_tokens = query.lower().translate(table).split()
    cleaned_query_tokens = [ps.stemWord(word) for word in query_tokens if
                            word not in stop_words and pre.is_ascii(word) and len(word) > 2]
    return cleaned_query_tokens


def read_secondary_index(secondary_index_root):
    """
    Load the secondary index file from disk to memory into a dictionary
    :param secondary_index_root: Path where the secondary index file is present
    """
    with open(secondary_index_root + "/" + "secondary_index.pickle", 'rb') as handle:
        secondary_index_map = pickle.load(handle)

    actual_secondary_index = {}
    secondary_index = list(secondary_index_map.keys())
    secondary_index = sorted(secondary_index, key=lambda x: int(x))
    for i in range(len(secondary_index)):
        if i+1 < len(secondary_index):
            actual_secondary_index[secondary_index[i] + '-' + secondary_index[i+1]] = secondary_index_map[secondary_index[i]]
        else:
            actual_secondary_index[secondary_index[i] + '-' + "999999999999999"] = secondary_index_map[secondary_index[i]]
    return actual_secondary_index


def read_secondary_index_page(page_title_secondary_root):
    """
    Load the secondary index file from disk to memory into a dictionary
    :param secondary_index_root: Path where the secondary index file is present
    """
    with open(page_title_secondary_root + "/" + "page_title_secondary.pickle", 'rb') as handle:
        secondary_index_map = pickle.load(handle)

    actual_secondary_index = {}
    secondary_index = list(secondary_index_map.keys())
    secondary_index = sorted(secondary_index, key=lambda x: int(x))
    for i in range(len(secondary_index)):
        if i + 1 < len(secondary_index):
            actual_secondary_index[secondary_index[i] + '-' + secondary_index[i + 1]] = secondary_index_map[
                secondary_index[i]]
        else:
            actual_secondary_index[secondary_index[i] + '-' + "999999999999999"] = secondary_index_map[
                secondary_index[i]]
    return actual_secondary_index


def get_offset_file_secondary_index(secondary_index_map, term_id):
    """
    Gets the primary index offset file name based on the term id
    :param secondary_index_map: Map containing the secondary index
    :param term_id: term_id of the current term
    :return: offset file name
    """
    for key, value in secondary_index_map.items():
        term_range = key.split("-")
        if int(term_range[0]) <= int(term_id.strip()) < int(term_range[1]):
            return value


def read_offset_file_and_get_posting_list(primary_index_offset_file, primary_index_root, term_id):
    """
    Read offset file, get the position of the position list and then get the posting list itself
    :param primary_index_offset_file: Name of the file containing the offset of the primary index file
    :param term_id: term_id of the current term
    :return: posting list
    """
    offset_map = {}
    offset_file_name = primary_index_offset_file.split("/")[-1]
    file_no = offset_file_name.split("_")[-1]

    with open(primary_index_offset_file, 'r') as offset_file:
        lines = offset_file.readlines()
        for offset_line in lines:
            offset_value = offset_line.split(":")
            offset_map[offset_value[0]] = offset_value[1]

    pos_of_posting_list = offset_map[str(term_id)]
    posting_list = ""
    with open(primary_index_root + "/" + "Primary_Index_" + file_no) as primary_index_file:
        primary_index_file.seek(int(pos_of_posting_list), 0)
        target_line = primary_index_file.readline()
        posting_list = target_line.split(":")[1]

    return posting_list


def find_intersection_and_rank(term_posting_map, query_mode):
    """
    Find the tfidf score of each and then find intersection of all and return results in sorted manner
    :param term_posting_map: Map of a term and its corresponding posting list
    :return: ranked list of results
    """
    map_of_tfidf_maps = {}
    reg = re.compile("[a-z]")
    for term, posting_list in term_posting_map.items():
        posting_items = posting_list.split("|")[:-1]
        idf_score = posting_list.split("|")[-1]
        split_lists = [re.split(reg, item) for item in posting_items]
        doc_ids = [item[0] for item in split_lists]
        frequencies = [item[1] for item in split_lists if len(item) > 1]
        tfidf_list = map(lambda x: round(int(x) * float(idf_score), 3), frequencies)
        doc_id_tfidf_map = dict(zip(doc_ids, tfidf_list))
        map_of_tfidf_maps[term] = doc_id_tfidf_map

    list_of_sets = []
    for term, tfidf_map in map_of_tfidf_maps.items():
        list_of_sets.append(set(list(tfidf_map.keys())))

    intersection_set = set()
    if list_of_sets:
        intersection_set = set.intersection(*list_of_sets)
    ranking_list = []

    for doc_id in intersection_set:
        total_tfidf_score = 0
        for term, tfidf_map in map_of_tfidf_maps.items():
            total_tfidf_score += tfidf_map[doc_id]
        ranking_list.append((doc_id, total_tfidf_score))

    ranking_list = sorted(ranking_list, key=operator.itemgetter(1), reverse=True)
    if query_mode == "field" or query_mode == "normal":
        return ranking_list[:10]
    elif query_mode == "boolean":
        return ranking_list


def read_offset_and_get_categorized_posting_list(primary_index_offset_file, primary_index_root, term_id, category):
    """
    Read the offset file and get the posting list which only contains postings which have the required category
    :param primary_index_offset_file: Name of primary index offset file
    :param primary_index_root: Path to the directory containing the offset files
    :param term_id: term_id of the current term
    :return: Posting list having only the required postings
    """
    offset_map = {}
    offset_file_name = primary_index_offset_file.split("/")[-1]
    file_no = offset_file_name.split("_")[-1]

    with open(primary_index_offset_file, 'r') as offset_file:
        lines = offset_file.readlines()
        for offset_line in lines:
            offset_value = offset_line.split(":")
            offset_map[int(offset_value[0].strip())] = offset_value[1]

    pos_of_posting_list = offset_map[int(term_id.strip())]
    posting_list = ""
    with open(primary_index_root + "/" + "Primary_Index_" + file_no) as primary_index_file:
        primary_index_file.seek(int(pos_of_posting_list), 0)
        target_line = primary_index_file.readline()
        posting_list = target_line.split(":")[1]
        split_postings = posting_list.split(":")
        required_postings = []
        for posting in split_postings:
            if category in posting:
                required_postings.append(posting)
        required_postings.append(split_postings[-1])

    return "|".join(required_postings)


def get_actual_term_id_file(term_id_secondary_map, term):
    """
    Getting the corresponding term_id_map_file containing the term_id of the current term
    :param term_id_secondary_map: secondary index map for term id
    :param term: term_id of the current term
    :return: term_id_map_file containing the required term
    """
    secondary_index = list(term_id_secondary_map.items())
    found = 0
    for i in range(len(secondary_index)-1):
        if secondary_index[i][0] <= term < secondary_index[i+1][0]:
            found = 1
            return secondary_index[i][1]
    if not found:
        return list(term_id_secondary_map.items())[-1][1]


def get_document_titles(sorted_results, page_title_map):
    """
    Gets the title of the document given the page_id from the results
    :param page_title_map: Map of the page_id and the title of the document
    :param sorted_results: List of doc_ids whose titles are needed
    :return: List of titles
    """
    title_list = []
    for page_id, score in sorted_results:
        title_list.append(page_title_map[page_id.strip()])

    return title_list


def get_intersection_across_fields(sorted_category_results):
    """
    For field queries, this function calculates the intersection of results across fields and returns the intersection
    :param sorted_category_results: results sorted based on the tfidf score
    :return: intersection of all the results
    """
    map_of_maps = {}
    for category, tuples in sorted_category_results.items():
        map_of_maps[category] = dict(tuples)

    list_of_sets = []
    for category, tfidf_map in map_of_maps.items():
        list_of_sets.append(set(list(tfidf_map.keys())))

    intersection_set = set()
    if list_of_sets:
        intersection_set = set.intersection(*list_of_sets)

    ranking_list = []

    for doc_id in intersection_set:
        total_score = 0
        for term, tfidf_map in map_of_maps.items():
            total_score += tfidf_map[doc_id]
        ranking_list.append((doc_id, total_score))

    ranking_list = sorted(ranking_list, key=operator.itemgetter(1), reverse=True)
    return ranking_list[:10]


def and_operation(argument1, argument2):
    """
    Performs the AND operation on given two lists of doc_ids and posting_lists
    :param argument1: Tuple list of doc_id and posting_list
    :param argument2: Tuple list of doc_id and posting_list
    :return: Logical AND of the two lists
    """
    doc_ids1 = set(list(argument1.keys()))
    doc_ids2 = set(list(argument2.keys()))

    intersection_set = set.intersection(doc_ids1, doc_ids2)

    results = []
    for item in intersection_set:
        if item in argument1 and item in argument2:
            results.append((item, argument1[item] + argument2[item]))

    return results


def or_operation(argument1, argument2):
    """
    Performs the OR operation on given two lists of doc_ids and posting_lists
    :param argument1: Tuple list of doc_id and posting_list
    :param argument2: Tuple list of doc_id and posting_list
    :return: Logical OR of the two lists
    """
    doc_ids1 = set(list(argument1.keys()))
    doc_ids2 = set(list(argument2.keys()))

    union_set = set.union(doc_ids1, doc_ids2)

    results = []
    for item in union_set:
        tfidf_score = 0
        if item in argument1:
            tfidf_score += argument1[item]
        if item in argument2:
            tfidf_score += argument2[item]
        results.append([item, tfidf_score])

    return results


def not_operation(argument1, argument2):
    """
    Performs the NOT operation on given two lists of doc_ids and posting_lists, i.e. All elements that are in
    argument1 and not in argument2
    :param argument1: Tuple list of doc_id and posting_list
    :param argument2: Tuple list of doc_id and posting_list
    :return: Set Difference of the two lists (argument1 - argument2)
    """
    doc_ids1 = set(list(argument1.keys()))
    doc_ids2 = set(list(argument2.keys()))

    difference_set = doc_ids1.difference(doc_ids2)

    results = []
    for item in difference_set:
        if item in argument1:
            results.append([item, argument1[item]])

    return results


def perform_boolean_operations(sorted_query_results, operators):
    """
    Performs the boolean operations on the given query terms
    :param term_tuples: List of tuples where each item is (term, term_id)
    :param term_posting_map: map of each term_id and its corresponding posting list
    :param operators: List of boolean operations to be performed
    :return: Resultant list containing the result of preforming all the boolean operations
    """
    # The first two items are processed first with the first boolean operation
    # and results are stored in temp_results
    current_operator = operators[0]
    argument_one_list = dict(sorted_query_results[0][1])
    argument_two_list = dict(sorted_query_results[1][1])

    temp_results = []
    if current_operator == "AND":
        temp_results = and_operation(argument_one_list, argument_two_list)
    elif current_operator == "OR":
        temp_results = or_operation(argument_one_list, argument_two_list)
    elif current_operator == "NOT":
        temp_results = not_operation(argument_one_list, argument_two_list)

    if len(sorted_query_results) <= 2:
        return temp_results
    # Remaining boolean operations are performed on the rest of the list
    for i in range(1, len(sorted_query_results)):
        current_operator = operators[i-1]
        if current_operator == "AND":
            temp_results = and_operation(dict(temp_results), dict(sorted_query_results[i][1]))
        elif current_operator == "OR":
            temp_results = or_operation(dict(temp_results), dict(sorted_query_results[i][1]))
        elif current_operator == "NOT":
            temp_results = not_operation(dict(temp_results), dict(sorted_query_results[i][1]))

    return temp_results


def load_full_map(page_title_map_root):
    page_map_files = os.listdir(page_title_map_root)
    page_map_files = [file for file in page_map_files if file.endswith(".txt")]

    page_title_map = {}
    for file in page_map_files:
        with open(page_title_map_root + "/" + file, 'r', encoding='utf-8') as page_file:
            lines = page_file.readlines()
            for line in lines:
                line = line.split(":", 1)
                page_title_map[line[0]] = line[1].strip()

    return page_title_map


if __name__ == "__main__":
    stop_words = []
    with open('new_words.csv', 'r') as stop_file:
        reader = csv.reader(stop_file)
        stop_words = list(reader)
    stop_words = set(stop_words[0])

    term_map_root_secondary = "../../Files/TermIdMapSecondary"
    page_title_map_root = "../../Files/PageTitleMap"
    secondary_index_root = "../../Files/BigSecondary"
    primary_index_offset_root = "../../Files/BigPrimaryOffset"
    primary_index_root = "../../Files/BigPrimary"

    with open(term_map_root_secondary + "/" + "term_id_secondary.pickle", 'rb') as handle:
        term_id_map_secondary = pickle.load(handle)

    secondary_index_map = read_secondary_index(secondary_index_root)

    page_title_map_secondary = read_secondary_index_page(page_title_map_root)

    page_title_map = load_full_map(page_title_map_root)
    print(len(page_title_map))
    while True:
        # query_mode = input("Enter query mode: ")
        query = input("Enter the query: ")
        query_time = time.time()

        if ":" in query:
            query_mode = "field"
            # Query: "b:sachin tendulkar c:sports i:2003 t:World cup"
            print("Query Mode: ", query_mode)
            query_fields = re.findall("[a-z]:", query)
            query_fields = [category.replace(":", "") for category in query_fields]
            query_terms = re.split("[a-z]:", query)
            query_terms = [term.strip() for term in query_terms if len(term) > 1]

            query_map = {}
            # Get the term_ids of all the terms corresponding to each category
            for category, terms in list(zip(query_fields, query_terms)):
                processed_terms = preprocess_query(stop_words, terms)
                processed_term_ids = []
                for term in processed_terms:
                    term_id_file = get_actual_term_id_file(term_id_map_secondary, term)
                    with open(term_id_file, 'rb') as handle:
                        term_id_map = pickle.load(handle)
                        try:
                            processed_term_ids.append(term_id_map[term])
                        except KeyError:
                            continue
                query_map[category] = processed_term_ids

            term_posting_map = {}
            sorted_category_results = {}
            for category, term_ids in query_map.items():
                # Get the positing lists for each term_id
                for term_id in term_ids:
                    primary_index_offset_file = get_offset_file_secondary_index(secondary_index_map, term_id)
                    posting_list = read_offset_and_get_categorized_posting_list(primary_index_offset_file,
                                                                                primary_index_root, term_id, category)
                    term_posting_map[term_id] = posting_list
                # Find their intersection
                sorted_category_results[category] = find_intersection_and_rank(term_posting_map, query_mode)

            # Find the intersection of the results of all the categories
            sorted_results = get_intersection_across_fields(sorted_category_results)
            title_list = get_document_titles(sorted_results[:10], page_title_map)
            print("Time To Fetch Results: ", time.time() - query_time)
            print("Search Results")
            for i, title in enumerate(title_list):
                print(str(i+1) + ") " + title)
            print("****************************************************************")

        elif "AND" in query or "OR" in query or "NOT" in query:
            query_mode = "boolean"
            # Query: sachin AND tendulkar OR dhoni
            print("Query Mode: ", query_mode)
            query_terms = re.split("AND|OR|NOT", query)

            operators = [term for term in query.split() if term == "AND" or term == "OR" or term == "NOT"]

            query_list_map = {}
            for query in query_terms:
                processed_query = preprocess_query(stop_words, query)
                # Get all the term_ids of the processed query terms
                processed_term_ids = []
                for term in processed_query:
                    term_id_file = get_actual_term_id_file(term_id_map_secondary, term)
                    with open(term_id_file, 'rb') as handle:
                        term_id_map = pickle.load(handle)
                        try:
                            processed_term_ids.append(term_id_map[term])
                        except KeyError:
                            continue
                query_list_map[" ".join(query)] = processed_term_ids

            # Get their corresponding posting lists
            sorted_query_results = []
            for query, processed_term_ids in query_list_map.items():
                # Get the positing lists for each term_id
                term_posting_map = {}
                for term_id in processed_term_ids:
                    primary_index_offset_file = get_offset_file_secondary_index(secondary_index_map, term_id)
                    posting_list = read_offset_file_and_get_posting_list(primary_index_offset_file, primary_index_root,
                                                                         int(term_id.strip()))
                    term_posting_map[term_id] = posting_list
                # Find their intersection
                sorted_query_results.append([query, find_intersection_and_rank(term_posting_map, query_mode)])

            boolean_results = perform_boolean_operations(sorted_query_results, operators)
            sorted_results = sorted(boolean_results, key=operator.itemgetter(1), reverse=True)
            title_list = get_document_titles(sorted_results[:10], page_title_map)
            print("Time To Fetch Results: ", time.time() - query_time)
            print("Search Results")
            for i, title in enumerate(title_list):
                print(str(i+1) + ") " + title)
            print("****************************************************************")

        else:
            query_mode = "normal"
            print("Query Mode: ", query_mode)
            processed_query = preprocess_query(stop_words, query)
            processed_term_ids = []

            # Get the term_ids for the processed terms
            for term in processed_query:
                term_id_file = get_actual_term_id_file(term_id_map_secondary, term)
                with open(term_id_file, 'rb') as handle:
                    term_id_map = pickle.load(handle)
                    try:
                        processed_term_ids.append(term_id_map[term])
                    except KeyError:
                        print("Term not present")
                        continue

            # Getting the posting lists for each processed term_id
            term_posting_map = {}

            for term_id in processed_term_ids:
                primary_index_offset_file = get_offset_file_secondary_index(secondary_index_map, term_id)
                posting_list = read_offset_file_and_get_posting_list(primary_index_offset_file, primary_index_root,
                                                                     int(term_id.strip()))
                term_posting_map[term_id] = posting_list

            sorted_results = find_intersection_and_rank(term_posting_map, query_mode)
            title_list = get_document_titles(sorted_results[:10], page_title_map)
            print("Time To Fetch Results: ", time.time() - query_time)
            print("Search Results")
            for i, title in enumerate(title_list):
                print(str(i+1) + ") " + title)
            print("****************************************************************")
