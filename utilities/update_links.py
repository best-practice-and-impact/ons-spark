import os
import pandas as pd
pd.option_context('display.max_rows', None,
                  'display.max_columns', None,
                  'display.precision', 3)


class FindReplace():

    ''' 
    Class containing functions to find, count, and replace strings. 
    Use wrapper function search_and_replace
    '''

    def __init__(self, filepath, extensions, old_string, new_string):
        self.filepath = filepath
        self.extensions = extensions
        self.old_string = old_string
        self.new_string = new_string


    def _get_files(self):
        '''
        Gets a list of all files from filepath directory which match extensions
        '''

        file_list = list()
        files = os.listdir(self.filepath)
        for root, dirs, files in os.walk(self.filepath):
            for x in files:
                if x.endswith(self.extensions):
                    file_list.append(os.path.join(root, x))

        return file_list    


    def count_replacements(self):
        '''
        Counts number of instances of the old string in each file.
        Returns a pandas df with filepath and number of occurences.
        '''
        file_list = self._get_files()
        file_count = int()
        cols = ['filename', 'num_occurences']
        data = list()
        for x in file_list:
            with open(x, 'r', encoding = 'utf8') as search_file:
                old_string_count = search_file.read().count(self.old_string)
                print(f'Filename: {x}, occurences: {old_string_count}')
                data.append([x, old_string_count])
            if old_string_count > 0:
                file_count += 1
        print(f"Number of files to change: {file_count}")
        summary = pd.DataFrame(data, columns = cols)
        return summary



    def _change_string(self, filename):
        '''
        Changes string from old_string to new_string
        '''
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
        '''
        Loops through each file from get_files() and uses change_string() on it.
        '''
        file_list = self._get_files()
        for x in file_list:
            #print(f'opening {x}')
            self._change_string(x)



def search_and_replace(filepath, extensions, old_string, new_string):  
    '''
    Wrapper function.
    Counts number of files, and occurences within these files of the old_string.
    Replaces old_string with new_string in all files.
    Recounts the number of files, and occurences of old_string. This should now be 0.

    Args:
        filepath (string): filepath to directory you want to search through.
        extensions (tuple containing multiple strings): extensions of the files you want to change.
        old_string (string): string to search for which requires changing.
        new_string (string): string you want to change to.

    Example:
        filepath = r"..\ons-spark\ons-spark"
        old_string = "sprak"
        new_string = "spark"
        extensions = ('.md', '.ipynb')

        search_and_replace(filepath, extensions, old_string, new_string)

        '''

    docs = FindReplace(filepath, extensions, old_string, new_string)
    print(docs.count_replacements())
    docs.find_and_replace()
    print(docs.count_replacements())









        