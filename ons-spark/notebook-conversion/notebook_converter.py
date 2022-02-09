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
        Extract the code from the R cells. Attaches code to previous cell if
            Python.
        """
        r_cells = [cell for cell in self.nb["cells"]
                        if cell["cell_type"] == "markdown"
                        and cell["source"][:4] == "```r"
                        and cell["source"][-3:] == "```"]
        
        for cell in r_cells:
            r_code = cell["source"][4:-3]
            previous_cell = self.nb.cells[cell["cell_no"] - 1]
            if previous_cell["cell_type"] == "code":
                # Add R code to previous cell if Python
                previous_cell["r_code"] = r_code
            else:
                cell["r_code"] = r_code
                
    def _delete_checkpoint(self, checkpoint_path, stderr):
        """
        Delete checkpoint directory if it exists
        
        stdout should only display information, so the stderr is inherited from
            the show_warnings value in run_r
        """
        if checkpoint_path != None:
            if os.path.exists(checkpoint_path):
                subprocess.run(f"hdfs dfs -rm -r -skipTrash {checkpoint_path}",
                                shell=True, stdout=stderr)
            # Remove checkpoints if running a local session
            elif checkpoint_path[:8] == "file:///":
                if os.path.exists(checkpoint_path[8:]):
                    subprocess.run(
                        f"hdfs dfs -rm -r -skipTrash {checkpoint_path}",
                        shell=True, stdout=stderr)

    def run_r(self, r_path, show_warnings, checkpoint_path=None):
        """
        Run the R code and attach results to cells
        
        The code is saved in r_path and ran with Rscript, optionally
            suppressing warnings in the R output
        
        Optionally clears checkpoint cache, which is needed when running some
            R scripts
        """
        self._remove_r_path(r_path)
        
        r_cells = [cell for cell in self.nb["cells"]
                        if "r_code" in cell]
        
        # Suppress warnings when running R code if required
        if show_warnings == False:
            r_code = "options(warn = -1)"
            with open(r_path, "a") as f:
                    f.write("".join(r_code))
            stderr = subprocess.DEVNULL
        else:
            stderr = None
                
        for cell in r_cells:
            # Delete old checkpoint files before running next cell
            self._delete_checkpoint(checkpoint_path, stderr)
            
            # Append R code cell to previous input and run 
            r_code = cell["r_code"]
            with open(r_path, "a") as f:
                f.write("".join(r_code))
            raw_r_output = subprocess.check_output(f"Rscript {r_path}",
                                                   shell=True,
                                                   stderr=stderr)
            cell["raw_r_output"] = raw_r_output.decode()
    
        prev_r_output = ""
        for cell in r_cells:
            r_output = cell["raw_r_output"]
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
    
    def create_markdown_cell(self, cell, return_python, return_r):
        """
        Translates Jupyter notebook cells to Markdown
        """
        if "r_output" in cell or cell["cell_type"] == "code":
            # Create code tab
            output = ["\n````{tabs}\n"]
            
            # add Python if needed
            if cell["cell_type"] == "code":
                output.append("```{code-tab} py\n")
                output.append(cell["source"])
                output.append("\n```\n")
            
            # add R if needed
            if "r_code" in cell.keys():
                output.append("\n```{code-tab} r R\n")
                output.append(cell["r_code"])
                output.append("\n```\n")
            
            output.append("````\n")
            
            # add Python output
            if return_python == True and "outputs" in cell.keys():
                if len(cell["outputs"]) > 0:
                    output.append("\n```plaintext\n")
                    output.append(cell["tidy_python_output"])
                    output.append("```\n")
            
            # add R output
            if return_r == True and "r_output" in cell.keys():
                if len(cell["r_output"]) > 0:
                    output.append("\n```plaintext\n")
                    output.append(cell["r_output"])
                    output.append("```\n")           

            return "".join(output)
        elif cell["source"][:4] == "```r":
            # Ignore input R cells
            return ""
        else:
            # Markdown returned ad-verbatim
            return cell["source"]
    
    def create_markdown_output(self, output_type):
        """
        Runs create_markdown_cell for every cell
        """
        if output_type == "python":
            return_python = True
            return_r = False
        elif output_type == "r":
            return_python = False
            return_r = True
        elif output_type == "all":
            return_python = True
            return_r = True
        else:
            return_python = False
            return_r = False
        
        output = []
        for cell in self.nb["cells"]:
            output.append(self.create_markdown_cell(cell,
                                                    return_python,
                                                    return_r))
            
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
        code_cells = deepcopy([cell for cell in self.nb["cells"] 
                        if cell["cell_type"] == "code" or
                        "r_code" in cell])

        [cell.setdefault("r_output", "") for cell in code_cells]
        [cell.setdefault("tidy_python_output", "") for cell in code_cells]
        [cell.setdefault("execution_count", "") for cell in code_cells]
        
        for cell in code_cells:
            cell["r_output"] = "".join(cell["r_output"])
        
        return_values = pd.DataFrame.from_dict(code_cells)
        return return_values[["cell_no", "execution_count",
                              "tidy_python_output", "r_output"]]
    
    def write_outputs_csv(self, output_results_path):
        """
        Save cell_outputs as a CSV to output_results_path
        """
        return_values = self.cell_outputs()
        return_values.to_csv(output_results_path, index=False)

        
def markdown_from_notebook(notebook_path,
                           output_path,
                           r_path,
                           output_csv,
                           show_warnings=True,
                           output_type="python",
                           checkpoint_path=None):
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
        show_warnings (boolean): Show or hide warnings in R output
        output_type {"python", "r", "all", None}:
            * python: outputs only Python code
            * r: outputs only R code
            * all: outputs Python and R code
            * None: no outputs will be returned
        checkpoint_path (string): clears checkpoints in this directory if
            specified when running each R cell
        
    Returns:
        MarkdownFromNotebook
    """
    md_notebook = MarkdownFromNotebook(notebook_path)
    md_notebook.run_r(r_path, show_warnings, checkpoint_path)
    md_notebook.create_markdown_output(output_type)
    md_notebook.write_markdown_file(output_path)
    md_notebook.write_outputs_csv(output_csv)
    
    return md_notebook