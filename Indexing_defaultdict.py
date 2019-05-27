

def make_index_for_page(processed_data_map, page_data, inverted_index_primary, term_id_map, max_val):
    """
    Function which will make the Inverted Index for each word of the processed_data_map
    :param processed_data_map: Map with processed data divided into categories
    :param page_data: Map with details about the current page
    :return: Inverted Index
    """
    # TODO : Read existing index from file and then insert accordingly
    # global inverted_index_primary, full_length
    category_map = {
        "Title": 't',
        "Infobox": 'i',
        "References": 'r',
        "Category": 'c',
        "External links": 'e',
        "Body": 'b'
    }

    for category, tokens in processed_data_map.items():
        for token in tokens:
            if len(token) > 2:
                if token not in term_id_map:
                    term_id_map[token] = len(term_id_map)
                inverted_index_primary[term_id_map[token]][page_data["Page_id"]][category_map[category]] += 1

    return inverted_index_primary, term_id_map, max_val
