# -*- coding: utf-8 -*-
"""
Created on Mon Dec  4 17:33:29 2023

@author: zogkoa
"""
import re
import os
import configparser



def get_version_from_bumpversion(path: str):
    """Reads bumpversion and returns the version
    """
    
    config = configparser.ConfigParser()

    config.read(path)

    version = config.get("bumpversion","current_version")
    
    return version

def add_version(file: str, old_string: str, new_string: str):
    """Opens a file and modifies it with regex
    """
     
    with open(file, "r", encoding = 'utf-8') as f:
        text = f.read()
        f.close()
        
    updated_html = re.sub(old_string, new_string, text)  
    
    #For debugging if needed printing effected files
    if updated_html != text:
        print("File {file} has been modified".format(file = file))
         
    with open(file, "w", encoding = 'utf-8') as f:
       
        f.write(updated_html)
        f.close()
    
if __name__ == "__main__":
    
    print("Wordking dir: ", os.getcwd())

    current_version = get_version_from_bumpversion(
        ".bumpversion.cfg")
    
    # This is produced by default from jupyter book version 0.15
    old_html_part = '''
    </div>
</nav></div>
    </div>
  
  
  <div class="sidebar-primary-items__end sidebar-primary__section">
  </div>
  
  <div id="rtd-footer-container"></div>


      </div>
    '''
    
    new_html_part ='''
        </div>
    </nav></div>
            <div class="bd-sidebar__bottom">
                <!-- To handle the deprecated key -->
                
                <div class="navbar_extra_footer">
                <div>
        <p>Book version {version}</p>
    </div>
    
                </div>
                
            </div>
        </div>
        <div id="rtd-footer-container"></div>
    </div>
    '''
    
    #Loop through all folders and search for html files
    for root, dirs, files in os.walk("ons-spark/_build"):
      for file in files:
            if file.endswith('.html'):
                
                add_version(
                    os.path.join(root, file),
                    old_html_part,
                    new_html_part.format(version = current_version)
                            )

