import json
import nbformat as nbf

filepath = r"C:\Users\hallj\ons-spark\test_update_links\spark-concepts\cache.md"

def is_markdown(cell_type, cell_source):
    """
    Determine if a cell is pure markdown without R code
    """
    if cell_type == "markdown" and cell_source[:4] != "```r":
        return True
    else:
        return False




# def change_font_size(notebook, font_size):
#     with open(notebook, 'r+', encoding='utf8') as open_file:
#         f = json.loads(open_file.read())
#         for cell in f['cells']:
#             if cell == "markdown":
#                 cell.seek(0,0)
#                 cell.write(f'<font size = {font_size}>')
#                 cell.seek(0,2)
#                 cell.write('</font>')
#         open_file.close()



def change_font_size(notebook, font_size):
    with open(notebook, 'r+', encoding='utf8') as open_file:
        content = open_file.read()
        text = (f"<font size = {font_size}>")
        open_file.seek(0,0)
        open_file.write(text.rstrip('\r\n') + '\n' + content)
        print(f'changing font size to {font_size}')
        open_file.seek(0,2)
        open_file.write('\n' + '</font>')
    open_file.close()


def delete_existing_font(notebook):
    with open(notebook, 'r+', encoding = 'utf8') as f:
        line = next(f) # grab first line
        old = '<font size =*>'
        new = '               ' # padded with spaces to make same length as old 
        f.seek(0) # move file pointer to beginning of file
        f.write(line.replace(old, new))
        f.seek(0,2)

        

delete_existing_font(filepath)

change_font_size(filepath, '12')



        firstline = open_file.readline()
        if firstline.startswith('<font'):
            data = open_file.read().splitlines(True)
            open_file.truncate(0)
            open_file.writelines(data[1:])
            open_file.close()
    

    
def delete_existing_font(notebook):
    with open(notebook, 'r', encoding='utf8') as open_file:
        new_font = 14
        content = open_file.readlines()
        content[0] = content[0].replace('<font size = *>', f'<font size = {new_font}>')
    with open(notebook, 'w', encoding = 'utf8') as fout:
        for line in content:
            fout.write(line)
    fout.close()