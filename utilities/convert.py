from notebook_converter import markdown_from_notebook

group = "spark-concepts"
folder = "groups-not-loops"
page = "groups-not-loops"
base_path = "/home/cdsw/ons-spark/ons-spark/"
out_path = base_path + group

in_path = base_path+"raw-notebooks/"+folder

nb_maker = (markdown_from_notebook(in_path + "/" + page + ".ipynb",
                                   out_path + "/" + page + ".md",
                                   in_path + "/r_input.R",
                                   in_path + "/outputs.csv",
                                   show_warnings=False,
                                   output_type="tabs")
)





