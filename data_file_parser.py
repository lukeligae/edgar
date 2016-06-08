__author__ = 'luke.li'

from bs4 import BeautifulSoup  ### It came with the Anacodna installtion, you don't need to install it again.
from bs4 import SoupStrainer   ### It allow you to choose which parts of an incoming document are parsed
import pandas as pd
import os
import datetime
import re
import inspect
from .logconfig import get_logger  ### logging.basicConfig not work within Python Notebook

current_day = datetime.datetime.now().strftime("%Y%m%d") ## strftime("%Y-%m-%d %H:%M:%S")
log_file_name = "sec_data_parse_" + current_day + ".log"

### logging.basicConfig not work within Python Notebook
#logging.basicConfig(filename="c:\sec_data.log", level=logging.DEBUG, format='%(asctime)s %(message)s', datefmt='%m/%d/%Y %I:%M:%S %p',)
#logger = logging.getLogger("edgar_data")

logger = get_logger('sec_data',log_file_name)


def parser_13F_file (data_file_dir, data_file_name ):

    data_file_dir =  os.path.normpath(data_file_dir)
    ###Read the file into a variable -- html_doc
    data_file_path_name = os.path.join( data_file_dir, data_file_name)
    with open(data_file_path_name ) as f:
        doc_text =f.read()

    ###Parse the whole file as a Soup object
    soup = BeautifulSoup(doc_text, "lxml")  # or soup = BeautifulSoup(doc_text, "html.parser")
    #print(soup.prettify())
    ### Get all XML tags For 13F filing, it has two xml tags
    soup_xmls = soup.find_all('xml')

    if soup_xmls:
        logger.info("Start to parse the XML 13F file of %s." % data_file_name)
        try:
            parser_13F_xml (doc_text, data_file_dir, data_file_name)
            logger.info("End of parsing the XML 13F file of %s." % data_file_name )
        except Exception as e:
            logger.info("Error in parser_13F_file:  for parsing the XML 13F file of %s. Error msg: %s" %(data_file_name,e))

    else:
        logger.info("Start to parse the TXT 13F file of %s." % data_file_name)
        try:
            parser_13F_txt (doc_text, data_file_dir, data_file_name)
            logger.info("End of parsing the 13F TXT file of %s." % data_file_name )
        except Exception as e:
            logger.info("Error in parser_13F_file:  for parsing the TXT 13F file of %s. Error msg: %s" %(data_file_name,e))


"""
Parse the 13F data file in XML format
All 13F forms filed for 2013 Q3 and after are xml format: One XML for sec header and XML for sec holding lines
"""
def parser_13F_xml(xml_doc, save_to_file_dir, data_file_name):
    # SEC XML header and Lines format
    header_columns = ['cik', 'company_name', 'form_type', 'filed_date', 'report_period',  'entry_total', 'value_total']
    
    line_columns   =  ['cusip', 'issuer_name', 'class_title','value_amt', 'share_qty', 'share_type','investmentdiscretion', 'votingauthority_sole','votingauthority_shared', 'votingauthority_none' ]
   

    ### We are only interested in the info contained in two xml tags.
    ### The SoupStrainer class allows you to choose which parts of an incoming document are parsed.
    only_xml_tags = SoupStrainer("xml")
    soup_xml_only = BeautifulSoup(xml_doc, "html.parser", parse_only=only_xml_tags)

    ###For SEC 13F filing, there are two XML sections (tags):
    ### XML 1:   The Filing Header info -- SIK, Company, Address, Date, Type ....
    ### XML 2:   The Filing Lines  info: All the  holding positions.
    soup_xmls = soup_xml_only.find_all('xml')
    soup_header  = soup_xmls[0]
    soup_lines = soup_xmls[1]

    ###If the header XML is not empty, Get Header Info and form the Header DataFrame
    if soup_header:
        logger.info("    XML Header parsing .......")
        
        # Get the header row (only one item)
        header =  parse_13F_header_xml(soup_header) 
        
        # Add two additional columns and form the list
        header_columns = ['data_file_name', 'data_file_format'] + header_columns
        header = [data_file_name, 'xml'] + header 
              
        #form the DF 
        header_list = [header]     #Only one item in the header list
        df_header = pd.DataFrame(header_list, columns=header_columns) 

        logger.info("    XML Header completed -- OK.")

        if len(df_header) > 0:
            save_df (df_header, 'SEC_13F_XML_Filing_Header', save_to_file_dir)
            logger.info("    XML Header saved -- OK.")
    else:
        logger.info("    Error in parser_13F_xm:  There is no XML header in the file %s" %data_file_name)

    ###If the Line XML is not empty, Get Holding Line Info from the holding lines xml
    ###Each holding line is in a tag "<ns1:infotable>" or "<infotable>"
    if soup_lines:
        logger.info("    XML Holding Lines parsing .......")
        
        # Get the line List (many)
        lines = parse_13F_lines_xml (soup_lines)
        
         # Add two additional columns 
        line_columns = ['data_file_name', 'data_file_format'] + line_columns
        line_list    = [ [data_file_name, 'xml'] + x for x in lines ] 
        
        
        df_lines = pd.DataFrame(line_list ,columns=line_columns)
        

        logger.info("    XML Holding Lines completed -- OK.")

        if len(df_lines) > 0:
            save_df (df_lines, 'SEC_13F_XML_Filing_Holding_Lines', save_to_file_dir)
            logger.info("    XML Holding Lines saved -- OK.")
    else:
        logger.info("    Error in parser_13F_xml:  There is no XML holding lines in the file %s" %data_file_name)


def parse_13F_header_xml (soup_header):
    """
    Parse the 13F Header in XML format
       
    """
    # Since some XML may have different namespace for the tags, we use the reges to match the ending words.
    a_header = [
             tag_to_string(soup_header.headerdata.find(re.compile("cik$")))                   ,# cik
             tag_to_string(soup_header.formdata.coverpage.find(re.compile("name$"))  )        ,# company_name
             tag_to_string(soup_header.headerdata.find(re.compile("submissiontype$")))        ,# form_type
             tag_to_string(soup_header.formdata.find(re.compile("signaturedate$")) )          ,# filed_date
             tag_to_string(soup_header.headerdata.find(re.compile("periodofreport$"))  )      ,# report_period
             tag_to_string(soup_header.summarypage.find(re.compile("tableentrytotal$")) )     ,# entry_total
             tag_to_string(soup_header.summarypage.find(re.compile("tablevaluetotal$")) )     ,# value_total
           ]
    return a_header

def parse_13F_lines_xml (soup_lines):
    # Since some XML may have different namespace for the tags, we use the reges to match the ending words.
    
    infotab_tags = soup_lines.find_all(re.compile('infotable$'))  # Find all the tags ending with "infotable"
    lines =[]
    for infotab in infotab_tags:
        a_line = [
                tag_to_string(infotab.find(re.compile("cusip$")) )                ,# cusip
                tag_to_string(infotab.find(re.compile("nameofissuer$")))          ,# issuer_name,
                tag_to_string(infotab.find(re.compile("titleofclass$")))          ,# class_title,
                tag_to_string(infotab.find(re.compile("value$")))                 ,# value_amt,
                tag_to_string(infotab.find(re.compile("sshprnamt$")))             ,# share_qty
                tag_to_string(infotab.find(re.compile("sshprnamttype$")))         ,# share_type,
                tag_to_string(infotab.find(re.compile("investmentdiscretion$")))  ,# investmentdiscretion,
                tag_to_string(infotab.find(re.compile("sole$")))                  ,# votingauthority_sole,
                tag_to_string(infotab.find(re.compile("shared$")))                ,# votingauthority_shared,
                tag_to_string(infotab.find(re.compile("none$")))                   # votingauthority_none
                 ]
        lines.append(a_line)
    return lines




def parser_13F_txt (txt_doc, save_to_file_dir, data_file_name):
    """
    Parse the 13F data file in TEXT format
    
    All 13F forms filed for 2013 Q2 and before  are TEXT format:  The Header part is the same, but the holding lines
    are different for different companies or different years. So the DataFrames saved to the file are not the same format
    and they need to further process
    
    """
    
    # SEC TEXT header format
    header_columns = ['cik', 'company_name', 'form_type', 'filed_date', 'report_period',  'entry_total', 'value_total']
    line_columns   =  ['cusip', 'issuer_name', 'class_title','value_amt', 'share_qty', 'share_type','investmentdiscretion', 'votingauthority_sole','votingauthority_shared', 'votingauthority_none' ]

   

    # Some files have error label in the lines section " To replace ">C>" to "<C>"
    txt_doc = txt_doc.replace(">C>", "<C>") # we have some this case
    txt_doc = txt_doc.replace("<C<", "<C>") # In case

    #Then parse the txt_doc to get the SEC header info
    soup_txt_doc = BeautifulSoup(txt_doc, 'lxml')

    soup_header = soup_txt_doc.find("sec-header")  # we don't need soup_header to parae teh Header for TEXT format
    soup_lines  = soup_txt_doc.find("document")


       ###If the header XML is not empty, Get Header Info and form the Header DataFrame
    if soup_header:
        logger.info("    TXT Header parsing .......")
        
        # Get the header row (only one item)
        header =  parse_13F_header_txt(txt_doc) 
        
        # Add two additional columns and form the list
        header_columns = ['data_file_name', 'data_file_format'] + header_columns
        header = [data_file_name, 'txt'] + header 
              
        #form the DF 
        header_list = [header]     #Only one item in the header list
        df_header = pd.DataFrame(header_list, columns=header_columns) 

        logger.info("    TXT Header completed -- OK.")

        if len(df_header) > 0:
            save_df (df_header, 'SEC_13F_TXT_Filing_Header', save_to_file_dir)
            logger.info("    TXT Header saved -- OK.")
    else:
        logger.info("    Error in parser_13F_txt:  There is no TXT header in the file %s" %data_file_name)
      
        
    ###If the Line TXT is not empty, Get Holding Line Info from the holding lines section
    ###Holding lines format varies for each company or years
    if soup_lines:
        logger.info("    TXT Holding Lines parsing .......")
        
        # Get the line List (many)
        lines = parse_13F_lines_txt (soup_lines)
        
                
        # Add two additional columns 
        line_columns = ['data_file_name', 'data_file_format'] + line_columns
        line_list    = [ [data_file_name, 'txt'] + x for x in lines ]  
        
        # No line_columns list, since the number of columns might different   
        # df_lines = pd.DataFrame(line_list ,columns=line_columns)
        df_lines = pd.DataFrame(line_list)  

        logger.info("    TXT Holding Lines completed -- OK.")

        if len(df_lines) > 0:
                ## Expend the  df_lines to pre-defined column number -- 25
            current_width = len(df_lines.columns)
            now_columns = range(current_width, 25)
            for i in now_columns:
                df_lines[i] = ''

            save_df (df_lines, 'SEC_13F_TXT_Filing_Holding_Lines', save_to_file_dir)
            logger.info("    TXT Holding Lines saved -- OK.")
    else:
        logger.info("    Error in parser_13F_txt:  There is no TXT holding lines in the file %s" %data_file_name)
        

def parse_13F_header_txt (txt_doc):
    """
    We need the whole txt_doc to parse the Header Info for TEXT foramt filing:
    We don't need the soup object 
    
    """
    # Check the first 200 lines and find all the lines with assgining statement "Lable : Value" 
    first_lines = txt_doc.split("\n")[:200]
    label_lines = [x.replace("\t","") for x in first_lines if ":" in x]    
    
    #header_columns = ['cik', 'company_name', 'form_type', 'filed_date', 'report_period',  'entry_total', 'value_total']    
    cik, company_name, form_type, filed_date, report_period,  entry_total, value_total = None,None,None,None,None,None,None
    
    for line in label_lines:
        if 'INDEX KEY' in line: 
            cik = line.split(":")[1].strip()
        elif 'CONFORMED NAME' in line: 
            company_name = line.split(":")[1].strip()
        elif 'FORM TYPE' in line: 
            form_type = line.split(":")[1].strip()  
        elif 'FILED AS' in line: 
            filed_date = line.split(":")[1].strip()
        elif 'PERIOD OF REPORT' in line: 
            report_period = line.split(":")[1].strip()
        elif 'Entry Total' in line: 
            entry_total = line.split(":")[1].replace("$","").replace(",","").strip() 
        elif 'Value Total' in line: 
            value_total = line.split(":")[1].replace("$","").replace(",","").strip()
        else:
            pass     
        
    return [cik, company_name, form_type, filed_date, report_period,  entry_total, value_total] 



def parse_13F_lines_txt (soup_lines):
    """
    #The prettify() method will turn a Beautiful Soup parse tree into a nicely formatted Unicode string, with each HTML/XML tag on its own line:
    #print(soup_lines.prettify())

    #All TXT holding lines are in the tag <s><c><c>..<c>  -- one <s> and 11 <c>s
    #Some only have one <s> and 10 <c>s
    #Some only have one <s  and 9 <c>s
    #Some even have 12<c>

    # some even have 2<c>s, no <s>
    
    """
    try:
        holding_text = soup_lines.s.c.c.c.c.c.c.c.c.c.c.c.c.string  ## Try 12<c> first
    except Exception as e0:
        try:
            holding_text = soup_lines.s.c.c.c.c.c.c.c.c.c.c.c.string  ## then Try 11<c> first (most cases)
        except Exception as e1:
            try:
                holding_text = soup_lines.s.c.c.c.c.c.c.c.c.c.c.string  ## Try 10<c> then
            except Exception as e2:
                try:
                    holding_text  = soup_lines.s.c.c.c.c.c.c.c.c.c.string  ## Try 9<c> lastly
                except Exception as e3:
                    try:
                        holding_text  = soup_lines.c.c.string  ## Try 2<c> lastly
                    except Exception as e4:
                        logger.info("      The TXT holding lines text does not have 11c, 10c, 9c or 2c structure! ")


    ## Standardlize the lines text to replace the one whitespace to two whitespaces
    ## for the pattern: letter and number (L-N), or number and letter (N-L), or number and number (N-N)
    holding_lines = std_double_space(holding_text)
    
    if "------" in holding_lines:
        ## if the holding lines are sepearted by "---------"
        ## each line can be delimited by '--\n'.. It can't be split by '\n', since some company name are wrapped
        parsed_lines = holding_lines.split("--\n")
        # Remove the "------" lines
        for line in parsed_lines:
            if "-----" in line:
                parsed_lines.remove(line)
        ## Fix Some Errors : Only one line --------------------------\n
        ## But the lines are are in one Element and  separeted by 0\n
        if (len(parsed_lines) == 1) and ('0\n' in parsed_lines[0]):  # Then parse it by \n again
            parsed_lines = holding_lines.split("\n")

    else:
        ## if the holding lines are not separetd by "---------"
        ## and each line can be delimited by '\n'. there is no word wrapped
        parsed_lines = holding_lines.split("\n")

    ## For each line, Split the fields by at least two spaces '  '.
    ## The normal lines have 11 fields are expected and it is normal.
    ## There are some lines may have more then 11 fields and they need further process.
    ## We return all the lines more than 6 fields
    lines =[]
    for a_line in parsed_lines:
        
        ## Remove the '$', '--', '\n' and ',' in the parsed lines, if any 
        a_line = a_line.replace("$","").replace("--","").replace(",","").replace("\n","")
                   
        
        output = [s.strip() for s in a_line.split('  ') if s]
        
        if len(output) >= 5:
            lines.append(output)
            
    return lines

def tag_to_string( tag):
    if tag:
        text = tag.string
    else:
        text =''        
    return text


def std_double_space (text):
    """
    Parser Help Function: To Double The Space
    
    If in the text, there is only one space between letter and number (L-N), or number and letter (N-L),
    or number and number (N-N), then replace the single-space to doulbe-space
    so that we use the doulbe- or more spaces to split the text.
    """
    ## Define the replace function
    def f_doublespace(matchobj):
        matched_str = matchobj.group(0)
        if ' ' in matched_str:
            result_str = matched_str.replace(' ', '  ' )
        else:
            result_str = '  '
        return result_str
    ## Replace the matched single-space patterns L-N, N-L or N-N
    replaced = re.sub(r'(\d [A-Z])|([A-Z] \d)|(\d \d)', f_doublespace, text)
    return replaced


def save_df (df_name, file_base_name, save_to_file_dir):
    """
    Save a specified DataFrame to a text file in the specified directory. 
    The file name will be automatically suffixed with the CURRENT_DATE (yyyymmdd) to the "file_base_name".    
    The DataFrames are saved to file in "append" mode without headering line. 
    
    Input: df_name          -- the dataframe to save
           file_base_name   -- the base file name, the full file name will look like "file_base_name_yyyymmdd.txt"
           save_to_file_dir -- the dir path where to store the file. The files are sotred in the subfolder "save_to_file_dir/Result".
    Output: None (the  dataFrame is saved in the specified text file).
    
    """
    current_day = datetime.datetime.now().strftime("%Y%m%d")
    save_file_name = file_base_name +'_'+current_day+'.txt'
    save_file_dir = os.path.join( save_to_file_dir,'Result')
    os.makedirs(save_file_dir, exist_ok=True) # Make sure to create the folder if not existing
    save_file_path = os.path.join(save_file_dir, save_file_name )

    # Save the dataframe wihtout header and index with "|" delimited
    df_name.to_csv(save_file_path, sep='|', mode = 'a', header=False, index=False)




