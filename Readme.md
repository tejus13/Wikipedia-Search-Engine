# Wikipedia Search Engine

This is a search engine built on the full corpus of wikipedia (~60GB). 
The link to the dataset can be found here
ftp://10.4.17.131/Datasets/IRE_Monsoon_2017/WikiSearch/

## Performance -
### For Queries of - 
1. less than **3** words, time to fetch results is **< 1s**
2. between **3 and 7** words, time to fetch results is **Around 2-3s**

## Code Files - 
1. **Search.py** - Main file containing all the code for Query Processing
2. **Driver.py** - Main file which runs the code for Indexing
3. **Preprocess.py** - File containing all functions related to XML parsing and text preprocessing.
4. **MultiwayMerge.py** - File with functions related to k-way mergesort algorithm.
5. **MultiLevelIndexing.py** - File containing functions related to making offset files and secondary index.
6. **Indexing_defaultdict.py** - File which performs the actual indexing
7. **TermHandling.py** - File with functions which split the term-term_id map into small files and sorts and performs external merge on these files. Also makes a secondary index to access this map.

## Execution of Code
### Prerequisits - 
#### Required Directories
1. **Index** - Initial index gets created here
2. **IndexMerge** - They get merged here
3. **PrimaryIndex** - Merged file is split into smaller files here
4. **PrimaryOffset** - Offset files for these merged files are made here
5. **SecondaryIndex** - Secondary index file for these offset files are made here
6. **PageTitleMap** - Files containing page_id-title map are made here
7. **TermIdMap** - Small files of the term-term_id map are made here
8. **TermIdMerge** - These small files are merged here and split into many files
9. **TermIdMapSecondary** - Secondary index for these files are present here.

#### Required Files
1. **stop_words.csv** - A csv file containing all the stop words in the current directory of the code
2. **full_wiki.xml** - The XML file containing the full data of wikipedia

### Execution -  
Run **Search.py** - An infinite loop runs expecting queries.

### Types of Queries - 
1. **Field query** - Assuming that fields are small letters(b, i, c, t, r, e) followed by colon and the fields are space separated.
“b:sachin i:2003 c:sports”

2. **Boolean query** - Assuming that the boolean operators are given in capitals (AND, OR, NOT) and remaining words are space separated.
“Sachin AND Dhoni NOT Kohli” 

3. **Normal query** - Any sequence of words that doesn’t satisfy the above conditions is considered a normal query.
    “Sachin Tendulkar”
