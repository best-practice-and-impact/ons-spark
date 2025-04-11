# Script to write the codeblocks from a markdown document into Python and R files

import os
import re
import nbformat

# Python code block pattern:  ```{code-tab} py* ```
# R code block pattern: ```{code-tab} r R * ```

# Base path
base_path = "/home/cdsw/ons-spark/ons-spark/"

# Book page name
bpname = "r-udfs"

# Book folder name
fname = "ancillary-topics"

# There should be no reason to edit below this point
# Path for markdown file
path_md = base_path + fname + "/" + bpname + ".md"

# Output paths
path_o = base_path + "raw-notebooks/" + bpname + "/"

if(os.path.exists(path_o) == False):
   os.mkdir(path_o)

path_o_py = path_o + bpname + ".ipynb"
path_o_r = path_o + "r_input.R"

# Read in markdown document

with open(path_md, encoding="utf8") as f:
  md_doc = f.read()

#### Extract Python code blocks and write to notebook

py_regex = r"(?<=```{code-tab} py)[\s\S]+?(?=```)"

py_blocks = re.findall(py_regex, md_doc)

notebook = nbformat.v4.new_notebook()

cells = []

for block in py_blocks:
    cell = nbformat.v4.new_code_cell(block)
    cells.append(cell)

notebook['cells'] = cells

nbformat.write(notebook, path_o_py)

#### Extract R code blocks and write to R file

r_regex = r"(?<=```{code-tab} r R)[\s\S]+?(?=```)"

r_blocks = re.findall(r_regex, md_doc)

with open(path_o_r, "w") as outfile:
    outfile.write('\n'.join(str(block) for block in r_blocks))