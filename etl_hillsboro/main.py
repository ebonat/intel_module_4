
import time
from extract.extract_data  import ExtractData
from transform.transform_data  import TransformData
from load.load_data  import LoadData
import config

def main():

    print("ETL process starts!")
    
    print("1. Extracting  the data...")
    extract_data = ExtractData()
#     author email and payment data
    author_email_row, author_email_column = extract_data.author_email_extract()    
    author_payment_row, author_payment_column = extract_data.author_payment_extract()

    print("2. Transforming the data...")
    transform_data = TransformData()
    df_author_email = transform_data.author_name_transform(author_email_row, author_email_column)
    df_author_payment = transform_data.author_payment_transform(author_payment_row, author_payment_column, df_author_email)

    print("3. Loading the data...")
    load_data =  LoadData()    
    load_data.file_load(df_author_email, config.AUTHOR_EMAIL_LIST, new_index_column="author_id",  file_type="CSV")    
    load_data.file_load(df_author_payment, config.AUTHOR_PAYMENT_LIST, new_index_column="author_id",  file_type="CSV")
    
    print("ETL process done!")
                        
if __name__ == '__main__':
    start_time = time.time()
    main()
    end_time = time.time()
    print("Program Runtime: " + str(round(end_time - start_time, 1)) + " seconds" + "\n")

    