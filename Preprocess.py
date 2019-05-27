import xml.etree.ElementTree as et
from lxml import etree
import re
import Driver as dr
import Indexing_defaultdict as id_defdict
from collections import defaultdict
from Stemmer import Stemmer
import pickle

ps = Stemmer('porter')


def extract_text_of_subheadings(heading_text_map, article_text, compiled_regex):
    """
    Gets the data of the each division(subheading) and stores them separately
    :param article_text: Raw article text
    :return: Text divided as per their subheadings
    """
    text_for_info = article_text.replace("\n", "\\n")

    # Regex to extract all the Infobox text from the full text
    info_text = re.findall(r"{{Infobox.*\\n}}", text_for_info)

    # Regex to remove CSS, HTML
    article_text = re.sub(compiled_regex, " ", article_text)

    # Regex to extract all the subheadings inside the text field
    headings = re.findall(r"\n={2}[^=].*==", article_text)

    # Regex to extract all the text below the subheadings from text
    heading_text = re.split(r"\n={2}[^=].*==", article_text)
    heading_text_map["Infobox"] = info_text

    # Loop to store only the references, categories and external links field+text separately
    # Remaining text is put into the body category
    body_list = []
    for i, head in enumerate(headings):
        if "references" in head.lower():
            heading_text_map["References"] = heading_text[i+1]
        elif "external links" in head.lower():
            heading_text_map["External links"] = heading_text[i+1].split("\n\n[[", 1)[0]
            category_text = ""
            if "}}\n\n[[Category" in heading_text[i+1]:
                category_text = heading_text[i+1].split("}}\n\n[[", 1)[1].rsplit("]]", 1)[0]
            elif "}}\n[[Category" in heading_text[i+1]:
                category_text = heading_text[i + 1].split("}}\n[[", 1)[1].rsplit("]]", 1)[0]
            heading_text_map["Category"] = category_text
        else:
            body_list.append(heading_text[i+1])
    heading_text_map["Body"] = body_list
    return heading_text_map


def is_ascii(word):
    """
    Checks if word is ascii or not
    :param word: token
    :return: Boolean
    """
    valid = True
    try:
        word = word.encode('ascii')
    except UnicodeEncodeError:
        valid = False
    return valid


def perform_preprocessing(categorized_text, stop_words):
    """
    Perform standard preprocessing techniques like tokenizing, stemming and stop word removal
    :param categorized_text: Input text map containing all the text from one wiki page
    :return: Same map with the processed text attached to each subheading
    """

    table = str.maketrans(' ', ' ', '!"\'()*+,;<>[\\]^`{|}~:=%&_#?')
    '''
    Here preprocessing is done for each subheading of text separately - 
    (1) Tokenization
    (2) Stemming
    (3) Stop Word Removal
    After Processing they are put back into a similar map with subheadings.
    '''
    for category, text in categorized_text.items():
        if isinstance(text, list):
            text = " ".join(text)
        if category == "Title" and text:
            tokens = text.lower().translate(table).split()
            cleaned_tokens = [ps.stemWord(word) for word in tokens if word not in stop_words and is_ascii(word) and len(word) > 2]
            # cleaned_tokens = [ps.stem(word) for word in tokens if
            #                   word not in stop_words and is_ascii(word) and len(word) > 2]
            categorized_text["Title"] = cleaned_tokens
        if category == "Infobox" and text:
            text = " ".join(text)
            text = text.replace("\\n", " ")
            tokens = text.lower().translate(table).split()
            cleaned_tokens = [ps.stemWord(word) for word in tokens if word not in stop_words and is_ascii(word) and len(word) > 2]
            # cleaned_tokens = [ps.stem(word) for word in tokens if
            #                   word not in stop_words and is_ascii(word) and len(word) > 2]
            categorized_text["Infobox"] = cleaned_tokens
        if category == "References" and text:
            tokens = text.lower().translate(table).split()
            cleaned_tokens = [ps.stemWord(word) for word in tokens if word not in stop_words and is_ascii(word) and len(word) > 2]
            # cleaned_tokens = [ps.stem(word) for word in tokens if
            #                   word not in stop_words and is_ascii(word) and len(word) > 2]
            categorized_text["References"] = cleaned_tokens
        if category == "External links" and text:
            tokens = text.lower().translate(table).split()
            cleaned_tokens = [ps.stemWord(word) for word in tokens if word not in stop_words and is_ascii(word) and len(word) > 2]
            # cleaned_tokens = [ps.stem(word) for word in tokens if
            #                   word not in stop_words and is_ascii(word) and len(word) > 2]
            categorized_text["External links"] = cleaned_tokens
        if category == "Category" and text:
            tokens = text.lower().translate(table).split()
            cleaned_tokens = [ps.stemWord(word) for word in tokens if word not in stop_words and is_ascii(word) and len(word) > 2]
            # cleaned_tokens = [ps.stem(word) for word in tokens if
            #                   word not in stop_words and is_ascii(word) and len(word) > 2]
            categorized_text["Category"] = cleaned_tokens
        if category == "Body" and text:
            if isinstance(text, list):
                text = " ".join(text)
            tokens = text.lower().translate(table).split()
            cleaned_tokens = [ps.stemWord(word) for word in tokens if word not in stop_words and is_ascii(word) and len(word) > 2]
            # cleaned_tokens = [ps.stem(word) for word in tokens if
            #                   word not in stop_words and is_ascii(word) and len(word) > 2]
            categorized_text["Body"] = cleaned_tokens
    return categorized_text


def get_wikipedia_dump(path_to_xml, index_root, stop_words, compiled_regex, page_threshold, file_threshold, page_title_root):
    """
    Function that reads the data from the xml file and stores it in a JSON structure.
    :param path_to_xml: Path of the input xml file
    :return: JSON structure with the data from the xml file
    """
    path = []

    page_data = {}
    heading_text_map = {}
    term_id_map = {}
    processed_text_map = {}
    inverted_index_primary = defaultdict(lambda: defaultdict(lambda: defaultdict(int)))
    page_title_map = {}

    total_length = 0
    max_val = 0

    page_data["Document_name"] = path_to_xml

    total_pages = 0
    num_pages = 0
    file_no = 0
    page_counter = 0
    page_file_name = page_title_root + "/" + "page_title_map_" + str(file_no) + ".txt"
    page_file_pickle = page_title_root + "/" + "page_title_map_" + str(file_no) + ".pickle"
    page_file = open(page_file_name, 'w+', encoding='utf-8')
    page_secondary_map = {}

    for event, elem in etree.iterparse(path_to_xml, events=("start", "end")):
        elem.tag = elem.tag[elem.tag.find('}')+1:]
        if event == 'start':
            path.append(elem.tag)
            if elem.tag == 'page':
                page_data = {}
                heading_text_map = {}
                processed_text_map = {}
        elif event == 'end':
            if path[-1] == "title":
                heading_text_map["Title"] = elem.text
            elif 'id' == path[-1] and \
                 'page' == path[-2]:
                page_id = elem.text
                page_data["Page_id"] = page_id
                page_title_map[int(page_id)] = heading_text_map["Title"]
                if page_counter < 1000000:
                    page_file.write(page_id + ":" + heading_text_map["Title"] + "\n")
                    page_file.flush()
                else:
                    page_file.close()
                    with open(page_file_name, 'r', encoding='utf-8') as f:
                        line = f.readline()
                        page_id = line.split(":")[0]
                        page_secondary_map[page_id] = page_file_name
                    with open(page_file_pickle, 'wb') as page_title_file:
                        pickle.dump(page_title_map, page_title_file, protocol=pickle.HIGHEST_PROTOCOL)
                    page_title_map.clear()
                    file_no += 1
                    page_counter = 0
                    page_file_name = page_title_root + "/" + "page_title_map_" + str(file_no) + ".txt"
                    page_file_pickle = page_title_root + "/" + "page_title_map_" + str(file_no) + ".pickle"
                    page_file = open(page_file_name, 'w+', encoding='utf-8')
                page_counter += 1

            elif path[-1] == "text":
                pass
                # article_text = elem.text
                # if article_text:
                #     heading_text_map = extract_text_of_subheadings(heading_text_map, article_text, compiled_regex)
            elif path[-1] == "page":
                pass
                # if num_pages < page_threshold:
                #     processed_text_map = perform_preprocessing(heading_text_map, stop_words)
                #     inverted_index_primary, term_id_map, max_val = \
                #         id_defdict.make_index_for_page(processed_text_map, page_data, inverted_index_primary, term_id_map, max_val)
                #     num_pages += 1
                #     total_pages += 1
                # else:
                #     file_no += 1
                #     output_path = index_root + "/inverted_index_new_" + str(file_no) + ".txt"
                #     dr.write_index_to_file(inverted_index_primary, output_path)
                #     inverted_index_primary.clear()
                #     num_pages = 0
                #     print("File " + str(file_no) + " Done")
                    # if file_no == file_threshold:
                    #     break
                elem.clear()
            path.pop()
    page_file.close()

    with open(page_file_pickle, 'wb') as page_title_file:
        pickle.dump(page_title_map, page_title_file, protocol=pickle.HIGHEST_PROTOCOL)
    with open(page_title_root + "/" + "page_title_secondary.pickle", 'wb') as page_title_file:
        pickle.dump(page_secondary_map, page_title_file, protocol=pickle.HIGHEST_PROTOCOL)
    with open(page_title_root + "/" + "page_title_secondary.txt", 'w+') as file:
        for page_id, file_name in page_secondary_map.items():
            file.write(page_id + ":" + file_name + "\n")
    # file_no += 1
    # output_path = index_root + "/inverted_index_new_" + str(file_no) + ".txt"
    # dr.write_index_to_file(inverted_index_primary, output_path)
    # inverted_index_primary.clear()
    # print("File " + str(file_no) + " Done")

    return processed_text_map, page_data, total_length, inverted_index_primary, term_id_map, total_pages, page_title_map


