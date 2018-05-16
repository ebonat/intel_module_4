
import sys
import traceback
import time
import base64

class ETLLibrary(object):

    def __init__(self):
        pass
    
    def print_exception_message(self, message_orientation = "horizontal"):
        """
        print full exception message
        :param message_orientation: horizontal or vertical
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
        
    def b64decode_string(self, b64encode_string):
        """
        decode the  b64 encode  string
        :param b64encode_string: b64 encode string
        :return dencode string
        """
        dencode_string = None
        try:
            dencode_string = base64.b64decode(b64encode_string)
            dencode_string = dencode_string.decode("utf-8")            
        except Exception:
            self.print_exception_message()
        return dencode_string
        