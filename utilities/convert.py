from notebook_converter import markdown_from_notebook
# Group: Which chapter does this work belong in once converted 
group = "spark-concepts"

# What is the name of the folder the .ipynb is located in 
folder = "groups-not-loops"

# What is the name of the .ipynb folder
page = "groups-not-loops"

# The path to the folder named above (can be relative or absolute path)
base_path = "ons-spark/"

# No changes are needed below this comment 
out_path = base_path + group
in_path = base_path+"raw-notebooks/"+folder
nb_maker = (markdown_from_notebook(in_path + "/" + page + ".ipynb",
                                   out_path + "/" + page + ".md",
                                   in_path + "/r_input.R",
                                   in_path + "/outputs.csv",
                                   show_warnings=False,
                                   output_type="tabs")
)
