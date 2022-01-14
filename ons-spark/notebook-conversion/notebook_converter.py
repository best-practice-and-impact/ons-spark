import os
import subprocess
import pandas as pd
import nbformat as nbf
from copy import deepcopy

class MarkdownFromNotebook():
    """
    Class to generate Markdown from a source Jupter Notebook
    
    Use the wrapper markdown_from_notebook function
    
    In development - yet to be unit tested and will be added to while the book
        is being developed further
    """
    def __init__(self, input_path):
        source_nb = nbf.read(input_path, nbf.NO_CONVERT)
        self.source_nb = source_nb
        self.nb = deepcopy(source_nb)
        self.assign_cell_no()
        self.tidy_python_cell_outputs()
        self.extract_r()

    def _remove_r_path(self, r_path):
        """
        Remove the R code file, if it exists
        """
        if os.path.exists(r_path):
            os.remove(r_path)
    
    def assign_cell_no(self):
        """
        Assign cell numbers to each cell in the notebook
        """
        for cell_no, cell in enumerate(self.nb["cells"]):
            cell["cell_no"] = cell_no        
        
    def extract_r(self):
        """
        Extract the code from the R cells
        """
        r_cells = [cell for cell in self.nb["cells"]
                        if cell["cell_type"] == "markdown"
                        and cell["source"][:4] == "```r"
                        and cell["source"][-3:] == "```"]
        
        for cell in r_cells:
            r_code = cell["source"][4:-3] # currently not checking
            self.nb.cells[cell["cell_no"] - 1]["r_code"] = r_code
            # need to check that previous cell is python
        
    def run_r(self, r_path):
        """
        Run the R code and attach results to Python cells
        
        The code is saved in r_path and ran with Rscript
        """
        self._remove_r_path(r_path)
        
        r_cells = [cell for cell in self.nb["cells"]
                        if "r_code" in cell]
        
        for cell in r_cells:
            r_code = cell["r_code"]
            with open(r_path, "a") as f:
                f.write("".join(r_code))
            raw_r_output = subprocess.check_output(f"Rscript {r_path}",
                                                   shell=True,
                                                   stderr=subprocess.DEVNULL)
            cell["raw_r_output"] = raw_r_output
        
        prev_r_output = []
        for cell in r_cells:
            r_output = cell["raw_r_output"].decode().split("[1]")
            cell["r_output"] = r_output[len(prev_r_output):]
            prev_r_output = r_output

    def tidy_python_cell_output(self, cell):
        """
        Translates the Python outputs to Markdown
        """
        if "outputs" in cell:
            output = []
            for cell_output in cell["outputs"]:
                if cell_output["output_type"] == "stream":
                    output.append(cell_output["text"])
                        
                    if output[-1][-2:] == "\n\n":
                        output[-1] = output[-1][:-1]

            for cell_outputs in cell["outputs"]:
                if cell_outputs["output_type"] == "execute_result":
                    output.append(cell_outputs["data"]["text/plain"])
                    output.append("\n")
            
            return "".join(output)
    
    def tidy_python_cell_outputs(self):
        """
        Run tidy_python_cell_output for every Python cell
        """
        python_cells = [cell for cell in self.nb["cells"]
                        if cell["cell_type"] == "code"]
        
        for cell in python_cells:
            cell["tidy_python_output"] = self.tidy_python_cell_output(cell)
    
    def create_markdown_cell(self, cell):
        """
        Translates Jupyter notebook cells to Markdown
        """
        if cell["source"][:4] == "```r":
            # Ignore input R cells
            return ""
        elif cell["cell_type"] == "markdown":
            # Markdown returned ad-verbatim
            return cell["source"]
        else:
            # Create code tab
            output = ["\n````{tabs}\n"]
            
            # add Python
            output.append("```{code-tab} py\n")
            output.append(cell["source"])
            output.append("\n```\n")
            
            # add R if needed
            if "r_code" in cell.keys():
                output.append("\n```{code-tab} r R\n")
                output.append(cell["r_code"])
                output.append("\n```\n")
            
            output.append("````\n")
            
            # add output
            if "outputs" in cell.keys():
                
                output.append("\n```plaintext\n")
                output.append(cell["tidy_python_output"])
                output.append("```\n")
            
            return "".join(output)
    
    def create_markdown_output(self):
        """
        Runs create_markdown_cell for every cell
        """
        output = []
        for cell in self.nb["cells"]:
            output.append(self.create_markdown_cell(cell))
        
        self.md_output = "".join(output)
    
    def write_markdown_file(self, output_file_path):
        """
        Write out results to output_file_path
        """
        with open(output_file_path, "w") as f:
            f.write("".join(self.md_output))
    
    def cell_outputs(self):
        """
        Return a pandas DF of Python and R outputs
        """
        python_cells = [cell for cell in self.nb["cells"]
                        if cell["cell_type"] == "code"].copy()
        r_cells = [cell for cell in python_cells
                        if "r_output" in cell]
        
        for cell in r_cells:
            cell["r_output"] = "".join(cell["r_output"])
        
        return_values = pd.DataFrame.from_dict(python_cells)
        return return_values[["execution_count", "tidy_python_output",
                              "r_output"]]
    
    def write_outputs_csv(self, output_results_path):
        """
        Save cell_outputs as a CSV to output_results_path
        """
        return_values = self.cell_outputs()
        return_values.to_csv(output_results_path, index=False)

        
def markdown_from_notebook(notebook_path, output_path, r_path, output_csv):
    """
    Creates a Markdown file with code tabs for Python and R saved as
        output_path, from which a Jupyterbook can be created.
    
    The R code will be ran to ensure it works without errors. The outputs
        from both the R and Python code are saved as a CSV as output_csv.
    
    The conditions for an R cell are:
        - Cell immediately below a Python code cell
        - Begins ```r
        - Ends ```
        - Any cell not meeting this condition will be considered a regular
          Markdown cell
          
    Args:
        notebook_path (string): Path to source notebook
        output_path (string): Path to save Markdown output
        r_path (string): Path to save raw R code
        output_csv (string): Path to save Python and R output, for comparison
        
    Returns:
        MarkdownFromNotebook
    """
    md_notebook = MarkdownFromNotebook(notebook_path)
    md_notebook.run_r(r_path)
    md_notebook.create_markdown_output()
    md_notebook.write_markdown_file(output_path)
    md_notebook.write_outputs_csv(output_csv)
    
    return md_notebook