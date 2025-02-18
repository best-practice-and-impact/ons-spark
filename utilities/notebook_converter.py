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
        self._assign_cell_no()
        self._format_adjacent_markdown()
        self.tidy_python_cell_outputs()
        self.extract_r()

    def _remove_r_path(self, r_path):
        """
        Remove the R code file, if it exists
        """
        if os.path.exists(r_path):
            os.remove(r_path)
    
    def _assign_cell_no(self):
        """
        Assign cell numbers to each cell in the notebook
        """
        for cell_no, cell in enumerate(self.nb["cells"]):
            cell["cell_no"] = cell_no
            
    def _is_markdown(self, cell_type, cell_source):
        """
        Determine if a cell is pure markdown without R code
        """
        if cell_type == "markdown" and cell_source[:4] != "```r":
            return True
        else:
            return False
    
    def _format_adjacent_markdown(self):
        """
        Adds two new lines to markdown cells if adjacent, to ensure output
            matches the source notebook
        """
        # Use range rather than enumerate as do not want last item
        for cell_no in range(len(self.nb["cells"]) - 1):
            cell = self.nb["cells"][cell_no]
            next_cell = self.nb["cells"][cell_no + 1]
            if self._is_markdown(cell["cell_type"], cell["source"]):
                if self._is_markdown(next_cell["cell_type"],
                                     next_cell["source"]):
                    cell["source"] = cell["source"] + "\n\n"
            
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

    def run_r(self, r_path, show_warnings,
              checkpoint_path=None, extra_r_path=None):
        """
        Run the R code and attach results to cells
        
        The code is saved in r_path and ran with Rscript, optionally
            suppressing warnings in the R output
        
        Optionally clears checkpoint cache, which is needed when running some
            R scripts
            
        Optionally runs an R script before the run of each group of cells.
            Used to delete files that are generated on every run without write
            mode being set to overwrite.
        """
        self._remove_r_path(r_path)
        
        r_cells = [cell for cell in self.nb["cells"]
                        if "r_code" in cell]
        
        # Suppress warnings when running R code if required
        if show_warnings == False:
            r_code = "options(warn = -1, knitr.table.format = 'simple')"
            with open(r_path, "a") as f:
                    f.write("".join(r_code))
            stderr = subprocess.DEVNULL
        else:
            stderr = None

        for cell_no, cell in enumerate(r_cells):
            # Update on progress of R code
            print(f"Running R cell {cell_no + 1} of {len(r_cells)}")
            
            # Delete old checkpoint files before running next cell
            self._delete_checkpoint(checkpoint_path, stderr)
            
            # Run additional R code if needed
            if extra_r_path:
                subprocess.check_output(f"Rscript {extra_r_path}",
                                        shell=True,
                                        stderr=stderr)
            
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
                        if ((cell["cell_type"] == "code") & 
                            (cell["metadata"] != {'tags': ['remove-cell']})
                        )]
        
        for cell in python_cells:
            cell["tidy_python_output"] = self.tidy_python_cell_output(cell)
    
    def create_markdown_cell(self, cell, return_python, return_r, use_tabs):
        """
        Translates Jupyter notebook cells to Markdown
        """
        # If cell metadata contains remove-cell, we don't want it in the markdown doc,
        # so we end the func if it appears
        if (cell["metadata"] == {'tags': ['remove-cell']}):
            return ""
            print("Skipped cell with tag 'remove-cell'")

        # Set return_python to False if no Python output exists
        if return_python == True and "outputs" in cell.keys():
            if (len(cell["outputs"]) > 0):
                return_python = True
            else:
                return_python = False
        else:
            return_python = False
        
        # Set return_r to False if no R output exists
        if return_r == True and "r_output" in cell.keys():
            if len(cell["r_output"]) > 0:
                return_r = True
            else:
                return_r = False
        else:
            return_r = False
        
        # Disable tabs if no output of either type
        if return_python == False and return_r == False:
            use_tabs = False
                
        # Set opening string of output to use tabs if needed
        if use_tabs == True:
            python_output_open = "\n```{code-tab} plaintext Python Output\n"
            r_output_open = "\n```{code-tab} plaintext R Output\n"
        else:
            python_output_open = "\n```plaintext\n"
            r_output_open = "\n```plaintext\n"
            
            
        if "r_output" in cell or (cell["cell_type"] == "code"):
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
            
            if use_tabs == True:
                output.append("\n````{tabs}\n")
            
            # add Python output
            if return_python == True:
                output.append(python_output_open)
                output.append(cell["tidy_python_output"])
                output.append("```\n")
            
            # add R output
            if return_r == True:
                output.append(r_output_open)
                output.append(cell["r_output"])
                output.append("```\n")

            if use_tabs == True:
                output.append("````\n")

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
            use_tabs = False
        elif output_type == "r":
            return_python = False
            return_r = True
            use_tabs = False
        elif output_type == "all":
            return_python = True
            return_r = True
            use_tabs = False
        elif output_type == "tabs":
            return_python = True
            return_r = True
            use_tabs = True
        else:
            return_python = False
            return_r = False
            use_tabs = False
        
        output = []
        for cell in self.nb["cells"]:
            output.append(self.create_markdown_cell(cell,
                                                    return_python,
                                                    return_r,
                                                    use_tabs))
            
        self.md_output = "".join(output)
    
    def write_markdown_file(self, output_file_path):
        """
        Write out results to output_file_path
        """
        with open(output_file_path, "w", encoding="utf-8") as f:
            f.write("".join(self.md_output))
    
    def cell_outputs(self):
        """
        Return a pandas DF of Python and R outputs
        """
        code_cells = deepcopy([cell for cell in self.nb["cells"] 
                        if (cell["cell_type"] == "code") & (cell["metadata"] != {'tags': ['remove-cell']}) or
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
                           checkpoint_path=None,
                           extra_r_path=None):
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
        output_type {"python", "r", "all", "tabs", None}:
            * python: outputs only Python code
            * r: outputs only R code
            * all: outputs Python and R code
            * tabs: outputs Python and R code with named tabs
            * None: no outputs will be returned
        checkpoint_path (string): clears checkpoints in this directory if
            specified when running each R cell
        extra_r_path (string): additional R code to run before every run of
            the R cells. Main use case is to clear up files, e.g. if a file
            is created in cell 1 but not deleted until cell 2, on the second
            run of the code an error would be raised if the file exists
        
    Returns:
        MarkdownFromNotebook
    """
    md_notebook = MarkdownFromNotebook(notebook_path)
    md_notebook.run_r(r_path, show_warnings, checkpoint_path, extra_r_path)
    md_notebook.create_markdown_output(output_type)
    md_notebook.write_markdown_file(output_path)
    md_notebook.write_outputs_csv(output_csv)
    
    return md_notebook
