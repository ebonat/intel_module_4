
from extract.mysql_database import MySQLDatabase
import config

from library.etl_library import ETLLibrary

class ExtractData(ETLLibrary):

    def __init__(self):
#         ETLLibrary.__init__(self)
        super().__init__()
        
    def author_email_extract(self):
        """
        
        """
        author_email_row = None
        author_email_column = None
        try:
#             initialize mysql database object
            mysql_database = MySQLDatabase(self.b64decode_string(config.USER), self.b64decode_string(config.PASSWORD), 
                self.b64decode_string(config.HOST), self.b64decode_string(config.DATABASE))               
#             open mysql connection object
            mysql_connection = mysql_database.mysql_open_connection()                
#             sql select statement
            sql = "SELECT author_id, first_name, last_name, middle_name, email, website FROM author ORDER BY author_id"                    
#             get author email list rows and columns
            author_email_row, author_email_column  = mysql_database.mysql_cursor_select(mysql_connection, sql)             
        except Exception:     
            self.print_exception_message()                      
        finally:
#             close mysql connection object
            mysql_database.mysql_close_connection(mysql_connection)
        return author_email_row, author_email_column
                
    def author_payment_extract(self):
        """
        
        """
        author_payment_row = None
        author_payment_column = None
        try:
            mysql_database = MySQLDatabase(self.b64decode_string(config.USER), self.b64decode_string(config.PASSWORD), 
                self.b64decode_string(config.HOST), self.b64decode_string(config.DATABASE))               
            mysql_connection = mysql_database.mysql_open_connection()                
            sql = "".join(["SELECT author.author_id, article_author.au_paid_amount ",
                            "FROM (magazine.article_author article_author ",
                            "INNER JOIN magazine.article article ",
                            "ON (article_author.article_id = article.article_id)) ",
                            "INNER JOIN magazine.author author ",
                            "ON (article_author.author_id = author.author_id) ",
                            "ORDER BY author.author_id"])
            author_payment_row, author_payment_column  = mysql_database.mysql_cursor_select(mysql_connection, sql)             
        except Exception:     
            self.print_exception_message()                      
        finally:
            mysql_database.mysql_close_connection(mysql_connection)
        return author_payment_row, author_payment_column
