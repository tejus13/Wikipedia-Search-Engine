import heapq
from functools import total_ordering
from contextlib import ExitStack
import os
import math


@total_ordering
class TermHeap(object):
    def __init__(self, term_id, posting_list, file_pointer):
        self.term_id = term_id
        self.posting_list = posting_list
        self.file_pointer = file_pointer

    def __eq__(self, other):
        if not isinstance(other, type(self)):
            return NotImplemented
        return self.term_id == other.term_id

    def __lt__(self, other):
        if not isinstance(other, type(self)):
            return NotImplemented
        return int(self.term_id) < int(other.term_id)


def write_term_merged_index_file(merged_index, terms):
    """
    Writes the updated merged line into the merged file.
    :param merged_index: File Pointer of the merged index file
    :param terms: List of combined terms and their posting lists
    """
    word_delim = ":"

    new_line = ""
    new_line += terms[0].term_id + word_delim

    posting_list = ""
    for term in terms:
        posting_list += term.posting_list[:-1]

    new_line += posting_list + "\n"
    merged_index.write(new_line)
    merged_index.flush()


def perform_multiway_merge_handler(index_root, merge_root, index_files_list, merge_chunk_size, level):
    """
    Perform the Multiway merge for the list for files currently present in the directory
    :param root: Path of the folder where the files will be written
    :param index_files_list: List of index files
    :return:
    """
    word_delim = ":"

    chunk = 0
    chunk_counter = 0

    while chunk < len(index_files_list):
        with ExitStack() as stack:
            files = [stack.enter_context(open(index_root + "/" + file))
                     for file in index_files_list[chunk: chunk + merge_chunk_size]]
            cur_lines = [file.readline() for file in files]
            term_ids = [line.split(word_delim)[0] for line in cur_lines]
            posting_lists = [line.split(word_delim)[1] for line in cur_lines]

            term_objects_heap = [TermHeap(term, posting_list, file)
                                 for term, posting_list, file in zip(term_ids, posting_lists, files)]

            file_name = merge_root + '/Merged_Index_' + str(chunk_counter) + "_Level" + str(level) + '.txt'
            with open(file_name, 'w+') as merged_index:
                heapq.heapify(term_objects_heap)
                while term_objects_heap:
                    terms = []
                    while True:
                        least_term = heapq.heappop(term_objects_heap)
                        terms.append(least_term)
                        if not term_objects_heap or least_term != term_objects_heap[0]:
                            break

                    write_term_merged_index_file(merged_index, terms)

                    for term in terms:
                        new_line = term.file_pointer.readline()
                        if ":" in new_line:
                            new_term_id = new_line.split(word_delim)[0]
                            new_posting_list = new_line.split(word_delim)[1]
                            new_file_pointer = term.file_pointer
                            new_heap_node = TermHeap(new_term_id, new_posting_list, new_file_pointer)
                            heapq.heappush(term_objects_heap, new_heap_node)

        chunk += merge_chunk_size
        chunk_counter += 1


def delete_old_level_files(level_to_delete, merged_files_root):
    """
    Delete all the files of the old level because
    :param level_to_delete:
    :param merged_files_root:
    """
    current_files = os.listdir(merged_files_root)
    for file in current_files:
        file_name = file.rsplit('.', 1)[0]
        if file_name.endswith("Level" + str(level_to_delete)):
            os.remove(merged_files_root + "/" + file)


def perform_multiway_merge(index_root, merged_files_root, merge_chunk_size):
    """
    Performs Multiway merge of all the index files in a given directory
    :param index_root: Path to directory with all index files
    :param merged_files_root: Path to directory where merged index file should be created
    :param merge_chunk_size: Number of files to merge at once
    """
    level = 0
    index_files_list = os.listdir(index_root)
    perform_multiway_merge_handler(index_root, merged_files_root, index_files_list, merge_chunk_size, level)
    print("Level 0 Done")

    if len(os.listdir(merged_files_root)) > 1:
        while True:
            level += 1
            level_to_delete = level - 1
            index_files_list = os.listdir(merged_files_root)
            perform_multiway_merge_handler(merged_files_root, merged_files_root, index_files_list, merge_chunk_size, level)
            delete_old_level_files(level_to_delete, merged_files_root)
            print("Level", level, "Done")
            if len(os.listdir(merged_files_root)) == 1:
                break


def split_index_for_retrieval(merged_files_root, primary_index_root, term_id_map_length, number_of_primary_files, num_pages):
    """
    Split the merged index into smaller files so that we can access them faster for retrieval
    :param merged_files_root: Path to directory containing the merged index file
    :param primary_index_root: Path to directory where the secondary index files will be created
    :param term_id_map: Map of every term and its term-id.
    """
    one_big_merged_file = os.listdir(merged_files_root)[0]

    with open(merged_files_root + "/" + one_big_merged_file, 'r') as merged_file:
        total_lengths = term_id_map_length
        each_file_length = total_lengths / (number_of_primary_files * 100)
        file_count = 0
        while file_count < number_of_primary_files:
            primary_index_file_name = primary_index_root + "/" + "Primary_Index_" + str(file_count) + ".txt"
            with open(primary_index_file_name, 'w+') as primary_file:
                line_count = 0
                while line_count < each_file_length:
                    line = merged_file.readline()
                    if len(line) > 1:
                        posting_list = line.split(":")[1]
                        document_freq = posting_list.count("|")
                        idf = math.log(num_pages, math.e) * 1.0 / document_freq
                        primary_file.write(line[:-1] + str(round(idf, 3)) + "\n")
                        primary_file.flush()
                        # os.fsync(primary_file)
                    line_count += 1
            file_count += 1
            if file_count == 100:
                each_file_length = total_lengths / (number_of_primary_files * 10)
            if file_count == 200:
                each_file_length = total_lengths / number_of_primary_files
