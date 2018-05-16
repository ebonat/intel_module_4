
from library.etl_library import ETLLibrary
import xml.etree.cElementTree as ET
from enum import Enum
import csv
import config

import os
import sys

class AuthorEmail(Enum):
    author_id = 0
    full_name = 1
    email = 2
    website = 3
    
class LoadData(ETLLibrary):

    def __init__(self):
#         ETLLibrary.__init__(self)
        super().__init__()

    
    def file_load(self, df_name,  file_name, new_index_column=None, file_type=None):    
        """
        load the data frame to a file
        :param df_name: data frame name
        :param file_name: file name
        :param new_index_column: new index column
        :param file_type: file type
        :return none
        """
        try:
            project_directory_path = os.path.dirname(sys.argv[0])          
            file_path = os.path.join(project_directory_path, file_name)
            if file_type == "CSV":
                if new_index_column is not None:
                    df_name = df_name.set_index(new_index_column)          
                df_name.to_csv(file_path + ".csv")              
            elif file_type == "EXCEL":
                if new_index_column is not None:
                    df_name = df_name.set_index(new_index_column)             
                df_name.to_excel(file_path + ".xlsx")          
            elif file_type == "JSON":
                if new_index_column is not None:
                    df_name = df_name.set_index(new_index_column)          
                df_name.to_json(file_path + ".json")        
#                 pandas does not support a to_xml() method today      
#             elif file_type == "XML":                                
#                 self.to_xml_author_email(df_name, file_path)                
            elif file_type == "TXT":                
                pass
                df_name.to_csv(file_path + ".txt", header=False, index=False, sep=',', mode="a")                
            else:
                df_name.to_csv(file_path + ".csv")                    
        except Exception:     
            self.print_exception_message()                 
            
    def to_xml_author_email(self, df_name, file_path):
        """
        load data frame to xml file
        :param df_name: data frame name
        :param file_path: file path 
        :return none
        """
        try:           
            root = ET.Element("root")
            doc = ET.SubElement(root, "author_email")        
            for index, row in df_name.iterrows():
                ET.SubElement(doc, AuthorEmail.author_id.name).text = row[AuthorEmail.author_id.value]
                ET.SubElement(doc, AuthorEmail.full_name.name).text = row[AuthorEmail.full_name.value]    
                ET.SubElement(doc, AuthorEmail.email.name).text = row[AuthorEmail.email.value]    
                ET.SubElement(doc, AuthorEmail.website.name).text = row[AuthorEmail.website.value]                                        
            tree = ET.ElementTree(root)
            tree.write(file_path + ".xml")         
        except Exception:     
            self.print_exception_message()     