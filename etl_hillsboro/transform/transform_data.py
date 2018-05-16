
from library.etl_library import ETLLibrary

import pandas as pd
# import numpy as np

class TransformData(ETLLibrary):

    def __init__(self):
#         ETLLibrary.__init__(self)
        super().__init__()

    def author_name_transform(self, author_email_row, author_email_column):                    
        """
        get formatted author email data frame
        :param author_email_row: author email row list
        :param author_email_column: author email column list
        :return author email data frame
        """
        df_author_email = None
        try:
            if len(author_email_row) > 0:      
#                 load author email data frame
                df_author_email = pd.DataFrame.from_records(data=author_email_row, columns=author_email_column)                
#                 set author full name list and format type
                name_list, name_format = self.author_format_name1(format_order="LFM")
#                 format author full name
                df_author_email["full_name"] = df_author_email[name_list].apply(lambda name : name_format.format(name[0], name[1], name[2]), axis=1)      
#                 remove unused columns 
                df_author_email.drop(["first_name", "middle_name", "last_name"], inplace=True, axis=1)
#                 reorder the columns
                df_author_email = df_author_email[["author_id", "full_name", "email", "website"]] 
#                 set author_id column to index
#                 df_author_email = df_author_email.set_index("author_id")          
        except Exception:     
            self.print_exception_message()   
        return df_author_email 
     
    def author_payment_transform(self, author_payment_row, author_payment_column, df_author_email):     
        """
        get author payment data frame
        :param author_payment_row:
        :param author_payment_column:
        :return author payment data frame
        """
        df_author_payment_final = None
        try:
            if len(author_payment_row) > 0:               
#                 get author payment data frame
                df_author_payment = pd.DataFrame.from_records(data=author_payment_row, columns=author_payment_column) 
#                 group author payment data frame by author_id and calculate total payment by au_paid_amount
                df_author_payment = df_author_payment.groupby([ "author_id"], as_index=False)["au_paid_amount"].sum()     
#                 rename the au_paid_amount to paid_amount
                df_author_payment.rename(columns={'au_paid_amount':'paid_amount'}, inplace=True)                
#                 merge author email with author payment by author_id column (key)
                df_author_payment_final = pd.merge(df_author_email, df_author_payment, on="author_id")               
        except Exception:     
            self.print_exception_message()   
        return df_author_payment_final 
    
    def author_format_name1(self, format_order=None):
        """
        format author full name for lambda function
        :param format_order: format order
        :return name list and format order string
        """
        name_list = None
        name_format = None
        try:
            if format_order == "FML":
                name_list = ["first_name", "middle_name", "last_name"]
                name_format = "{} {}. {}"
            elif format_order == "FL":
                name_list = ["first_name", "last_name", "middle_name"]
                name_format = "{} {}"
            elif format_order == "LF":
                name_list = ["last_name", "first_name", "middle_name"]
                name_format = "{}, {}"
            elif format_order == "LFM":
                name_list = ["last_name", "first_name", "middle_name"]
                name_format = "{}, {} {}."
            else:
                name_list = ["first_name", "middle_name", "last_name"]
                name_format = "{} {}. {}"              
        except Exception:     
            self.print_exception_message()   
        return name_list, name_format
    
    def author_format_name2(self, first_name, last_name, middle_name, format_order=None):
        """
        format author full name 
        :param first_name: author first name
        :param last_name: author last name
        :param middle_name: author middle name
        :param format_order: format string order
        :return author full name
        """
        author_full_name = None
        try:
            if format_order == "FML":
                author_full_name = "".join(first_name, " ", middle_name, ".", last_name)
            elif format_order == "FL":
                author_full_name = "".join(first_name, " ", last_name)
            elif format_order == "LF":
                author_full_name = "".join(last_name, ", ", first_name)
            elif format_order == "LFM":
                author_full_name = "".join(last_name, ", ", first_name, " ", middle_name, ".")
            else:
                author_full_name = "".join(first_name, " ", middle_name, ".", last_name)                     
        except Exception:     
            self.print_exception_message()   
        return author_full_name
    