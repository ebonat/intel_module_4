# -*- coding: utf-8 -*-

# MySQL Connector/Python Developer Guide
# https://dev.mysql.com/doc/connector-python/en/

import sys
import traceback
import time

import mysql.connector

class MySQLDatabase(object):
    """
    mysql database class object
    developer by Ernest Bonat, Ph.D.
    copyright © 15 IT Resources, LLC
    """          
    def __init__(self, user, password, host, database):
        """
        mysql database class constructor
        :param user: user name
        :param password: user password
        :param host: host name (or host='127.0.0.1')
        :param database: database name
        """
        self.user = user
        self.password = password
        self.host = host
        self.database = database
        
    def mysql_open_connection(self):
        """
        open connection to mysql database
        :return mysql_connection: mysql database connection object
        """
        mysql_connection = None
        try:    
            mysql_connection = mysql.connector.connect(
                user = self.user, 
                password = self.password, 
                host = self.host, 
                database = self.database)                     
        except:     
            self.print_exception_message()                 
        return mysql_connection
        
    def mysql_close_connection(self, mysql_connection):
        """
        close  connection to mysql database
        :param mysql_connection: mysql database connection object
        """
        try:            
            if (mysql_connection.is_connected()):
                mysql_connection.close()
        except:     
            self.print_exception_message()                      
             
    def mysql_cursor_select(self, mysql_connection, sql, sql_parameters=None):
        """
        execute select sql statement
        :param mysql_connection: mysql database connection object
        :param sql: sql select statement
        :param sql_parameters: tuple of sql parameters
        :return row_list: row list of tuples
        """     
        row_list = None
        column_list = None
        try:    
            mysql_cursor = mysql_connection.cursor()    
            if sql_parameters is not None:    
                mysql_cursor.execute(sql, sql_parameters)
            else:
                mysql_cursor.execute(sql) 
            row_list = mysql_cursor.fetchall()                  
            column_list = list(mysql_cursor.column_names)                    
        except:     
            self.print_exception_message()               
        finally:
            self.mysql_close_cursor(mysql_cursor)            
        return row_list, column_list         
        
    def mysql_cursor_insert_update_delete(self, mysql_connection, sql, sql_parameters=None):
        """
        execute insert, update and delete sql statements
        :param mysql_connection: mysql database connection object
        :param sql: insert, update and delete sql statement
        :param sql_parameters: tuple of sql parameters
        :return none
        """
        try:    
            mysql_cursor = mysql_connection.cursor()    
            if sql_parameters is not None:    
                mysql_cursor.execute(sql, sql_parameters)
            else:
                mysql_cursor.execute(sql)                         
            mysql_connection.commit()
        except:     
            self.print_exception_message()               
        finally:
            self.mysql_close_cursor(mysql_cursor)
        
    def mysql_close_cursor(self, mysql_cursor):
        """
        close mysql cursor object
        :param mysql_cursor: mysql cursor
        :return none
        """
        try:                
            if mysql_cursor is not None:  
                mysql_cursor.close()                           
        except:     
            self.print_exception_message()                      
        
    def mysql_stored_procedure_select(self, mysql_connection, procedure_name, arguments=None):
        """
        execute stored procedure select statements
        :param mysql_connection: mysql database connection object
        :param procedure_name: stored procedure name
        :param arguments: tuple of  stored procedure arguments
        :return data_list: data list of tuples
        """
        data_list = None
        try:    
            mysql_cursor = mysql_connection.cursor()    
            if arguments is not None:    
                mysql_cursor.callproc(procedure_name, arguments) 
            else:
                mysql_cursor.callproc(procedure_name)                
            for result in mysql_cursor.stored_results():
                data_list = result.fetchall()                            
        except:     
            self.print_exception_message()                      
        finally:
            self.mysql_close_cursor(mysql_cursor)            
        return data_list   
    
    def mysql_stored_procedure_insert_update_delete(self, mysql_connect, procedure_name, arguments):
        """
        execute stored procedure insert, update and delete sql statements
        :param mysql_connect: mysql database connection object
        :param procedure_name: stored procedure name
        :param arguments: tuple of  stored procedure arguments
        :return none
        """
        try:    
            cursor = mysql_connect.cursor()    
            result = cursor.callproc(procedure_name, arguments) 
            mysql_connect.commit()
            if result is not None:  
                return result
            else:
                return None            
        except:     
            exception_message = self.get_exception_message()        
            print(exception_message)
        finally:
            self.mysql_close_cursor(cursor)

    def print_exception_message(self, message_orientation = "horizontal"):
            """
            print full exception message
            :param message_orientation: horizontal (default) or vertical
            :return none
            """
            try:
                exc_type, exc_value, exc_tb = sys.exc_info()            
                file_name, line_number, procedure_name, line_code = traceback.extract_tb(exc_tb)[-1]       
                time_stamp = " [Time Stamp]: " + str(time.strftime("%Y-%m-%d %I:%M:%S %p")) 
                file_name = " [File Name]: " + str(file_name)
                procedure_name = " [Procedure Name]: " + str(procedure_name)
                error_message = " [Error Message]: " + str(exc_value)        
                error_type = " [Error Type]: " + str(exc_type)                    
                line_number = " [Line Number]: " + str(line_number)                
                line_code = " [Line Code]: " + str(line_code) 
                if (message_orientation == "horizontal"):
                    print( "An error occurred:{};{};{};{};{};{};{}".format(time_stamp, file_name, procedure_name, error_message, error_type, line_number, line_code))
                elif (message_orientation == "vertical"):
                    print( "An error occurred:\n{}\n{}\n{}\n{}\n{}\n{}\n{}".format(time_stamp, file_name, procedure_name, error_message, error_type, line_number, line_code))
                else:
                    pass                    
            except Exception:
                pass