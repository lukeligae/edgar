__author__ = 'luke.li'
import pandas as pd
import os
import datetime
import re
from .logconfig import get_logger  ### logging.basicConfig not work within Python Notebook
from .data_file_parser import  save_df

current_day = datetime.datetime.now().strftime("%Y%m%d") ## strftime("%Y-%m-%d %H:%M:%S")
log_file_name = "sec_data_fix_" + current_day + ".log"

### logging.basicConfig not work within Python Notebook
#logging.basicConfig(filename="c:\sec_data.log", level=logging.DEBUG, format='%(asctime)s %(message)s', datefmt='%m/%d/%Y %I:%M:%S %p',)
#logger = logging.getLogger("edgar_data")

logger = get_logger('fix_data',log_file_name)

def post_parser_13F_txt_file (data_file_dir, data_file_name ):

    data_file_dir =  os.path.normpath(data_file_dir)
    ###Read the file into a variable -- html_doc
    data_file_path_name = os.path.join( data_file_dir, data_file_name)

    df_orig_13f = pd.read_csv(data_file_path_name, delimiter='|', header=-1) # if header =0 , will moss the 1st record.

    logger.info("Processing the the file %s  ........" % data_file_path_name)
    logger.info("    There are %s records in the file." % str(len(df_orig_13f)))

    #In the 13F holding lines file, usually Col2 = Issuer_Name, Col3=Class_Title, Col4=CUSIP, Col5= Value_Amt, Col6=Share_Qty, Col7=Share_Type
    #The rest Col7-Col10 are not important. Col13-Col23 should be empty.
    #Our daat fix is based on the 4th column CUSIP, it should be exactly 9 chars (digital or char).

    len_col_04 = [len(str(x)) for x in df_orig_13f[4] ]
    is_digit_col_05 = [str(x).isdigit() for x in df_orig_13f[5] ]
    is_digit_col_06 = [str(x).isdigit() for x in df_orig_13f[6] ]

    df_orig_13f["Len_Col_04"] = len_col_04
    df_orig_13f["Is_Digit_Col_05"] = is_digit_col_05
    df_orig_13f["Is_Digit_Col_06"] = is_digit_col_06

    #df_bad_rows  = df_orig_13f[(df_orig_13f["Len_Col_04"]!=9) | (~df_orig_13f["Is_Digit_Col_05"]) | (~ df_orig_13f["Is_Digit_Col_06"])]
    #df_good_rows = df_orig_13f[(df_orig_13f["Len_Col_04"]==9)& df_orig_13f["Is_Digit_Col_05"] & df_orig_13f["Is_Digit_Col_06"]]

    df_bad_rows  = df_orig_13f[(df_orig_13f["Len_Col_04"]!=9) |  (~ df_orig_13f["Is_Digit_Col_06"])]
    df_good_rows = df_orig_13f[(df_orig_13f["Len_Col_04"]==9) & df_orig_13f["Is_Digit_Col_06"]]

    df_corrected_rows = fix_bad_rows(df_bad_rows)

    #Second Around Correction
    #Some records the CUSIP were parsed wrongly with the Title_class since there is no space.
    #In This case teh frist round will not fix the error. We have to find out such BAD records
    #in the corrected rows.

    new_len_col_04 = [len(str(x)) for x in df_corrected_rows[4] ]
    new_is_digit_col_05 = [str(x).isdigit() for x in df_corrected_rows[5] ]
    new_is_digit_col_06 = [str(x).isdigit() for x in df_corrected_rows[6] ]


    #reset the length column "Len_Col_04"
    df_corrected_rows["Len_Col_04"] = new_len_col_04
    df_corrected_rows["Is_Digit_Col_05"] = new_is_digit_col_05
    df_corrected_rows["Is_Digit_Col_06"] = new_is_digit_col_06

    #df_good_rows_2  = df_corrected_rows[(df_corrected_rows["Len_Col_04"]==9)& df_corrected_rows["Is_Digit_Col_05"] & df_corrected_rows["Is_Digit_Col_06"]]
    #df_bad_rows_2   = df_corrected_rows[(df_corrected_rows["Len_Col_04"]!=9) | (~ df_corrected_rows["Is_Digit_Col_05"]) | (~ df_corrected_rows["Is_Digit_Col_06"])]

    df_good_rows_2  = df_corrected_rows[(df_corrected_rows["Len_Col_04"]==9) &  df_corrected_rows["Is_Digit_Col_06"]]
    df_bad_rows_2   = df_corrected_rows[(df_corrected_rows["Len_Col_04"]!=9) | (~ df_corrected_rows["Is_Digit_Col_06"])]


    df_final_good_rows = df_good_rows.append(df_good_rows_2)
    df_final_bad_rows  = df_bad_rows_2

    #Save the resulting DataFrame
    save_df (df_final_good_rows, 'SEC_13F_TXT_Filing_Holding_Lines_Processed_Good', data_file_dir)
    save_df (df_final_bad_rows,   'SEC_13F_TXT_Filing_Holding_Lines_Processed_Bad', data_file_dir)

    logger.info("    There are %s GOOD records and  %s BAD records saved." %(str(len(df_final_good_rows)) ,  str(len(df_final_bad_rows)) )  )



def   fix_bad_rows(df_bad_rows):
    #Find CUSIP field  location
    cusip_row_col =[]
    for row_index, row in df_bad_rows.iterrows():
        for i in range(5, 12):  #check from the 5th column to 15-th column
            if  (len(str(row[i])) == 9) and (not (' ' in str(row[i]) ) ) : # Find the first field which has non-space 9-chars.
                col_index = i
                break
            else:
                col_index =None
        if col_index:
            cusip_row_col.append([row_index, col_index])

    corrected_rows = df_bad_rows.copy() # Not corrected_rows = bad_rows  to eep two copies
    ## Using .ix, not .iloc or .loc, since row index match is label, column index is integer position
    for row_index, col_index in cusip_row_col:
        #Concaticate 2th --(cusip_ind -1)-th columns to become one
        new_col_02 = ''
        for i in range(2, col_index -1):
            new_col_02 = new_col_02 + ' ' + str(df_bad_rows.ix[row_index, i])  # Issurer_Name
        new_col_03 = str(df_bad_rows.ix[row_index, col_index-1])  # Class-Title
        corrected_rows.ix[row_index, 2 ] = new_col_02
        corrected_rows.ix[row_index, 3 ] = new_col_03
        shift = col_index - 4  # CUSIP is the 4-th field
        for i in range(4, 15):  # Check from 4th to 15-th column
            corrected_rows.ix[row_index, i] = df_bad_rows.ix[row_index, i + shift]

    return corrected_rows