# coding: utf-8 

from extract.mysql_database import MySQLDatabase
from library.etl_library import ETLLibrary
import config
from enum import Enum

class PublisherContact(Enum):
    company_name = 0
    contact_name = 1
    contact_title = 2
    phone = 3
    fax = 4
    email = 5
    website = 6

if __name__ == '__main__':   
    etl_library = ETLLibrary()
    try:    
        print(etl_library.b64decode_string(config.USER))
        print(etl_library.b64decode_string(config.PASSWORD))
        print(etl_library.b64decode_string(config.HOST))
        print(etl_library.b64decode_string(config.DATABASE))
        mysql_database = MySQLDatabase(etl_library.b64decode_string(config.USER), etl_library.b64decode_string(config.PASSWORD), 
                etl_library.b64decode_string(config.HOST), etl_library.b64decode_string(config.DATABASE))               
        mysql_connection = mysql_database.mysql_open_connection()       
        sql = "SELECT company_name, contact_name, contact_title, phone, fax, email, website FROM publisher ORDER BY company_name"
        publisher_cursor, publisher_column  = mysql_database.mysql_cursor_select(mysql_connection, sql)                     
        if len(publisher_cursor) > 0:                
            for row in publisher_cursor :
                print(row[PublisherContact.company_name.value], 
                      row[PublisherContact.contact_name.value], 
                      row[PublisherContact.contact_title.value], 
                      row[PublisherContact.phone.value], 
                      row[PublisherContact.fax.value], 
                      row[PublisherContact.email.value], 
                      row[PublisherContact.website.value],                       
                      sep='\t')     
    except Exception as ex:           
        etl_library.print_exception_message()               
    finally:                
        mysql_database.mysql_close_connection(mysql_connection)