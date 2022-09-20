from asyncore import file_dispatcher
import markdown
import os
import itertools
from pprint import pprint
import pandas as pd
pd.option_context('display.max_rows', None,
                       'display.max_columns', None,
                       'display.precision', 3)

filepath = r"C:\Users\hallj\ons-spark\ons-spark"
old_string = "https://spark.apache.org/docs/latest/api/python/reference/api/"
new_string = "https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/"

#other strings to change
#old_string = "https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql.html#functions"
#new_string = "https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/functions.html"


#test_file = r"C:\Users\hallj\ons-spark\ons-spark\test_update_links\spark-concepts\cache.md"


def get_files(filepath, extensions):
    file_list = list()
    files = os.listdir(filepath)
    for x in files:
        if x.endswith(extensions):
            file_list.append(os.path.join(filepath, x))
        if os.path.isdir(os.path.join(filepath, x)):
            file_list = file_list + [get_files(os.path.join(filepath, x), extensions)]
    file_list = list(filter(None, file_list))
    
    #unpack sub-lists of files into one single list
    file_list_flat = list()
    for i in file_list:
        if type(i) is list:
            for item in i:
                file_list_flat.append(item)
        else:
            file_list_flat.append(i)

    return file_list_flat                     



def count_replacements(filepath, extensions, old_string, new_string):
    file_list = get_files(filepath, extensions)
    file_count = int()
    cols = ['filename', 'num_occurences']
    data = list()
    for x in file_list:
        with open(x, 'r', encoding = 'utf8') as search_file:
            old_string_count = search_file.read().count(old_string)
            print(f'Filename: {x}, occurences: {old_string_count}')
            data.append([x, old_string_count])
        if old_string_count > 0:
            file_count += 1
    print(f"Number of files to change: {file_count}")
    summary = pd.DataFrame(data, columns = cols)
    return summary



def change_string(filename, old_string, new_string):
        with open(filename, 'r', encoding = 'utf8') as search_file:
            x = search_file.read()
            if old_string not in x:
                print(f'{old_string} not found in {filename}')
                return
        with open(filename, 'w', encoding = 'utf8') as file_to_change:
            print(f'Changing {old_string} to {new_string} in {filename}')
            x = x.replace(old_string, new_string)
            file_to_change.write(x)

#change_string(test_file, old_string, new_string)


def find_and_replace(filepath, extensions, old_string, new_string):

    file_list = get_files(filepath, extensions)
    for x in file_list:
        print(f'opening {x}')
        change_string(x, old_string, new_string)


count_replacements(filepath, (".md", ".ipynb"), old_string, new_string)

find_and_replace(filepath, (".md", ".ipynb"), old_string, new_string)

count_replacements(filepath, (".md", ".ipynb"), old_string, new_string)