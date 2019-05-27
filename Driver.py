import MultiwayMerge as mm
import Preprocess as pre
import MultiLevelIndexing as mli
import TermHandling as th
import time
import re
import csv
import sys
import os
import math
import pickle


def write_index_to_file(inverted_index, output_path):
    """
    Write full inverted index to file
    :param inverted_index: full inverted index
    """
    word_delim = ":"
    page_delim = "|"

    inverted_index = sorted(inverted_index.items())

    with open(output_path, "w+") as index_file:
        for term_id, page_map in inverted_index:
            new_line = str(term_id) + word_delim
            for page_id, freq_map in page_map.items():
                new_line += page_id + "f"
                total_freq = sum(freq_map.values())
                new_line += str(total_freq)
                for category, freq in freq_map.items():
                    new_line += category + str(freq)
                new_line += page_delim
            new_line += "\n"
            index_file.write(new_line)
            index_file.flush()


def write_page_title_map_to_file(page_title_root, page_title_map):
    """
    Function that writes page_id and title map to file
    :param secondary_index_root:
    :param term_id_map:
    """
    with open(page_title_root + "/" + "page_title_map.pickle", 'wb') as page_title_file:
        pickle.dump(page_title_map, page_title_file, protocol=pickle.HIGHEST_PROTOCOL)


def delete_the_big_index_file(merged_files_root):
    """
    Delete the big single index file
    :param merged_files_root:
    """
    big_file = os.listdir(merged_files_root)[0]
    os.remove(merged_files_root + "/" + big_file)


def get_total_terms(term_map_root):
    term_counter = 0
    files = os.listdir(term_map_root)

    files = [file for file in files if file.endswith("txt")]

    for file in files:
        term_counter += sum(1 for line in open(term_map_root + "/" + file, 'r'))

    return term_counter


if __name__ == "__main__":

    input_path = sys.argv[1]

    path_to_xml = input_path

    print("Started Processing")
    start = time.time()

    stop_words = []
    with open('new_words.csv', 'r') as stop_file:
        reader = csv.reader(stop_file)
        stop_words = list(reader)
    stop_words = set(stop_words[0])

    regex_to_remove_all = re.compile(r"\[\[[Ff]?ile(.*?)\]\]|{\|(.*?)\|}|{{[vV]?[cC]ite(.*?)}}|\<(.*?)\>|={3,}",
                                     flags=re.DOTALL)
    index_root = "../../Files/SmallIndex"
    page_title_root = "../../Files/SmallPageTitleMap"
    page_threshold = 5000
    file_threshold = 100

    processed_data_map, page_data, total_length, inverted_index_json, term_id_map, num_pages, page_title_map = \
        pre.get_wikipedia_dump(path_to_xml, index_root, stop_words, regex_to_remove_all, page_threshold, file_threshold, page_title_root)
    print("Number of Pages: ", num_pages)
    print("Total Indexing Time", time.time() - start)

    primary_index_root = "../../Files/SmallPrimary"
    term_id_root = "../../Files/SmallTermIdMap"
    term_id_merged_root = "../../Files/SmallTermIdMapMerged"
    term_id_secondary_root = "../../Files/SmallTermIdMapSecondary"
    print("Writing Term Id map")
    th.write_term_id_map_to_file(term_id_root, term_id_merged_root, term_id_secondary_root, term_id_map)

    print("Writing Page Title map")

    write_page_title_map_to_file(page_title_root, page_title_map)

    # num_pages = 17640866
    number_of_terms = get_total_terms(term_id_merged_root)
    print("Number of Terms: ", number_of_terms)
    merge_time = time.time()
    merged_files_root = "../../Files/SmallMerge"
    merge_chunk_size = 2000

    print("Started Merge")
    mm.perform_multiway_merge(index_root, merged_files_root, merge_chunk_size)
    print("Merge Time: ", time.time() - merge_time)

    split_time = time.time()
    number_of_primary_files = 400

    print("Started Split")
    mm.split_index_for_retrieval(merged_files_root, primary_index_root, number_of_terms, number_of_primary_files, num_pages)
    print("Split Time: ", time.time() - split_time)

    primary_index_offset_root = "../../Files/SmallPrimaryOffset"
    secondary_index_map = mli.create_primary_offset_index(primary_index_offset_root, primary_index_root)

    secondary_index_root = "../../Files/SmallSecondary"
    mli.write_secondary_index(secondary_index_root, secondary_index_map)
    delete_the_big_index_file(merged_files_root)








