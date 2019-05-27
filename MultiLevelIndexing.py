import os
import pickle


def write_secondary_index(secondary_index_root, secondary_index_map):
    """
    Write the tertiary index to file
    :param secondary_index_root:
    :param secondary_index_map:
    """
    with open(secondary_index_root + "/" + "secondary_index.pickle", 'wb') as secondary_index_file:
        pickle.dump(secondary_index_map, secondary_index_file, protocol=pickle.HIGHEST_PROTOCOL)
    with open(secondary_index_root + "/" + "secondary_index.txt", 'w+') as secondary_index_file:
        for key, value in secondary_index_map.items():
            secondary_index_file.write(key + ":" + value + "\n")
            secondary_index_file.flush()


def create_primary_offset_index(primary_index_offset_root, primary_index_root):
    """

    :param primary_index_offset_root:
    :param primary_index_root:
    :return:
    """
    primary_index_files = os.listdir(primary_index_root)

    secondary_index_map = {}

    for primary_index_file_name in primary_index_files:
        primary_index_offset_file_name = primary_index_offset_root + "/" + "Primary_Index_Offset_" + primary_index_file_name.split("_")[-1]
        with open(primary_index_offset_file_name, 'w+') as primary_index_offset_file:
            cur_line_length = 0
            with open(primary_index_root + "/" + primary_index_file_name, 'r') as primary_index_file:
                while True:
                    if primary_index_offset_file.tell() == 0:
                        cur_line = primary_index_file.readline()
                        term_id_start = cur_line.split(":")[0]
                        posting_list_length = len(cur_line) + 1
                        new_line = term_id_start + ":" + str(cur_line_length) + "\n"
                        primary_index_offset_file.write(new_line)
                        primary_index_offset_file.flush()
                        # os.fsync(primary_index_offset_file)
                        cur_line_length += posting_list_length
                        secondary_index_map[term_id_start] = primary_index_offset_file_name
                    else:
                        cur_line = primary_index_file.readline()
                        if not cur_line:
                            break
                        term_id = cur_line.split(":")[0]
                        posting_list_length = len(cur_line) + 1
                        new_line = term_id + ":" + str(cur_line_length) + "\n"
                        primary_index_offset_file.write(new_line)
                        primary_index_offset_file.flush()
                        # os.fsync(primary_index_offset_file)
                        cur_line_length += posting_list_length

    return secondary_index_map

