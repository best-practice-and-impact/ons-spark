import os
import pandas as pd
pd.option_context('display.max_rows', None,
                       'display.max_columns', None,
                       'display.precision', 3)

filepath = r"..\ons-spark\ons-spark"
old_string = ""
new_string = ""
extensions = ('.md', '.ipynb')


class FindReplace():

    def __init__(self, filepath, extensions, old_string, new_string):
        self.filepath = filepath
        self.extensions = extensions
        self.old_string = old_string
        self.new_string = new_string

    def _get_files(self):
        file_list = list()
        files = os.listdir(self.filepath)
        for root, dirs, files in os.walk(self.filepath):
            for x in files:
                if x.endswith(self.extensions):
                    file_list.append(os.path.join(root, x))

        return file_list    


    def count_replacements(self):
            file_list = self._get_files()
            file_count = int()
            cols = ['filename', 'num_occurences']
            data = list()
            for x in file_list:
                with open(x, 'r', encoding = 'utf8') as search_file:
                    old_string_count = search_file.read().count(self.old_string)
                    #print(f'Filename: {x}, occurences: {old_string_count}')
                    data.append([x, old_string_count])
                if old_string_count > 0:
                    file_count += 1
            print(f"Number of files to change: {file_count}")
            summary = pd.DataFrame(data, columns = cols)
            return summary



    def _change_string(self, filename):
            with open(filename, 'r', encoding = 'utf8') as search_file:
                x = search_file.read()
                if old_string not in x:
                    print(f'{self.old_string} not found in {filename}')
                    search_file.close()
            with open(filename, 'w', encoding = 'utf8') as file_to_change:
                print(f'Changing {self.old_string} to {self.new_string} in {filename}')
                x = x.replace(self.old_string, self.new_string)
                file_to_change.write(x)
                file_to_change.close()


    def find_and_replace(self):

        file_list = self._get_files()
        for x in file_list:
            print(f'opening {x}')
            self._change_string(x)

   

docs = FindReplace(filepath, extensions, old_string, new_string)
docs.count_replacements()
docs.find_and_replace()
docs.count_replacements()







        