import os
import heapq
from contextlib import ExitStack
from functools import total_ordering
import pickle


@total_ordering
class TermMapHeap(object):
    def __init__(self, term, term_id, file_pointer):
        self.term = term
        self.term_id = term_id
        self.file_pointer = file_pointer

    def __eq__(self, other):
        if not isinstance(other, type(self)):
            return NotImplemented
        return self.term == other.term

    def __lt__(self, other):
        if not isinstance(other, type(self)):
            return NotImplemented
        return str(self.term) < str(other.term)


def merge_sorted_term_id_files(term_id_root, term_id_sorted_root):
    file_counter = 0
    term_delim = ":"

    term_map_files = os.listdir(term_id_root)

    with ExitStack() as stack:
        files = [stack.enter_context(open(term_id_root + "/" + file)) for file in term_map_files]
        cur_lines = [file.readline() for file in files]
        terms_list = [line.split(term_delim)[0] for line in cur_lines]
        term_id_list = [line.split(term_delim)[1] for line in cur_lines]

        term_objects_heap = [TermMapHeap(term, term_id, file) for term, term_id, file in zip(terms_list, term_id_list, files)]

        file_name = term_id_sorted_root + '/Merged_Term_Map_' + str(file_counter) + '.txt'
        merged_file = open(file_name, 'w+')
        heapq.heapify(term_objects_heap)

        term_map = {}
        term_secondary_map = {}

        while term_objects_heap:
            least_term = heapq.heappop(term_objects_heap)
            if os.stat(file_name).st_size < 2000000:
                term_map[least_term.term] = least_term.term_id
                merged_file.write(least_term.term + term_delim + least_term.term_id)
            else:
                pickle_file_name = term_id_sorted_root + "/" + "Merged_Term_Map_" + str(file_counter) + ".pickle"
                with open(file_name, 'r') as f:
                    first_line = f.readline()
                    term_secondary_map[first_line.split(":")[0]] = pickle_file_name
                with open(term_id_sorted_root + "/" + pickle_file_name, 'wb') as term_id_file:
                    pickle.dump(term_map, term_id_file, protocol=pickle.HIGHEST_PROTOCOL)
                file_counter += 1
                file_name = term_id_sorted_root + '/Merged_Term_Map_' + str(file_counter) + '.txt'
                merged_file.close()
                term_map.clear()
                merged_file = open(file_name, 'w+')

            new_line = least_term.file_pointer.readline()
            if ":" in new_line:
                new_term = new_line.split(term_delim)[0]
                new_term_id = new_line.split(term_delim)[1]
                new_file_pointer = least_term.file_pointer
                new_heap_node = TermMapHeap(new_term, new_term_id, new_file_pointer)
                heapq.heappush(term_objects_heap, new_heap_node)
        pickle_file_name = term_id_sorted_root + "/" + "Merged_Term_Map_" + str(file_counter) + ".pickle"
        with open(term_id_sorted_root + "/" + pickle_file_name, 'wb') as term_id_file:
            pickle.dump(term_map, term_id_file, protocol=pickle.HIGHEST_PROTOCOL)
        merged_file.close()

        return term_secondary_map


def make_secondary_index_and_pickle(term_id_secondary_map, term_id_secondary_root):
    pickle_file_name = "term_id_secondary.pickle"
    with open(term_id_secondary_root + "/" + pickle_file_name, 'wb') as term_id_secondary_file:
        pickle.dump(term_id_secondary_map, term_id_secondary_file, protocol=pickle.HIGHEST_PROTOCOL)

    secondary_file_name = "term_id_secondary.txt"
    with open(term_id_secondary_root + "/" + secondary_file_name, 'w+') as term_id_secondary_file:
        for term, file in term_id_secondary_map.items():
            term_id_secondary_file.write(term + ":" + file + "\n")
            term_id_secondary_file.flush()


def write_term_id_map_to_file(term_id_root, term_id_sorted_root, term_id_secondary_root, term_id_map):
    """
    Function that writes term id map to file
    :param secondary_index_root:
    :param term_id_map:
    """
    file_counter = 0
    filename = term_id_root + "/" + "term_id_map_" + str(file_counter) + ".txt"
    term_id_file = open(filename, 'w+')

    for term, term_id in term_id_map.items():
        if os.stat(filename).st_size < 1000000:
            term_id_file.write(term + ":" + str(term_id) + "\n")
            term_id_file.flush()
        else:
            with open(term_id_root + "/" + "term_id_map_" + str(file_counter) + ".txt") as f:
                sorted_file = sorted(f)
            with open(term_id_root + "/" + "term_id_map_" + str(file_counter) + ".txt", 'w') as f:
                f.writelines(sorted_file)
            file_counter += 1
            filename = term_id_root + "/" + "term_id_map_" + str(file_counter) + ".txt"
            term_id_file.close()
            term_id_file = open(filename, 'w+')
    term_id_file.close()

    # map_files = os.listdir(term_id_root)
    # for file in map_files:
    #     with open(term_id_root + "/" + file) as f:
    #         sorted_file = sorted(f)
    #     with open(term_id_root + "/" + file, 'w') as f:
    #         f.writelines(sorted_file)

    print("Sorted Individual Files")
    print("Now Merging them")
    term_secondary_map = merge_sorted_term_id_files(term_id_root, term_id_sorted_root)

    make_secondary_index_and_pickle(term_secondary_map, term_id_secondary_root)
