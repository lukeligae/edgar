__author__ = 'luke.li'

from bs4 import BeautifulSoup  ### It came with the Anacodna installtion, you don't need to install it again.
from bs4 import SoupStrainer   ### It allow you to choose which parts of an incoming document are parsed
import pandas as pd
import os
import datetime
import re
from .config import get_logger  ### logging.basicConfig not work within Python Notebook

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
    soup = BeautifulSoup(doc_text)
    #print(soup.prettify())
    ### Get all XML tags For 13F filing, it has two xml tags
    soup_xmls = soup.find_all('xml')

    if soup_xmls:
        logger.debug("Start to parse the XML 13F file of %s." % data_file_name)
        try:
            parser_13F_xml (doc_text, data_file_dir, data_file_name)
            logger.debug("End of parsing the XML 13F file of %s." % data_file_name )
        except Exception as e:
            logger.debug("Error in parsing the XML 13F file of %s. Error msg: %s" %(data_file_name,e))

    else:
        logger.debug("Start to parse the TXT 13F file of %s." % data_file_name)
        try:
            parser_13F_txt (doc_text, data_file_dir, data_file_name)
            logger.debug("End of parsing the 13F TXT file of %s." % data_file_name )
        except Exception as e:
            logger.debug("Error in parsing the TXT 13F file of %s. Error msg: %s" %(data_file_name,e))


"""
Parse the 13F data file in XML format
All 13F forms filed for 2013 Q3 and after are xml format: One XML for sec header and XML for sec holding lines
"""
def parser_13F_xml(xml_doc, save_to_file_dir, data_file_name):
    # SEC XML header format
    header_columns = ['Data_File_Name', 'Data_File_Format','CIK', 'Form_Type', 'Filied_Date', 'Calendar_Period',  'Entry_Total', 'Value_Total',
                  'Company_Name', 'Addr_Street1','Addr_Street2','Addr_City','Addr_State', 'Addr_Zipcode' ]

    # SEC XML holding format
    line_columns = ['Data_File_Name', 'Data_File_Format', 'CUSIP', 'Issuer_Name', 'Class_Title','Value_Amt', 'Share_Qty', 'Share_Type',
                'InvestmentDiscretion', 'VotingAuthority_Sole','VotingAuthority_Shared', 'VotingAuthority_None' ]

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
        logger.debug("    XML Header parsing .......")
        header = parse_xml_header(soup_header, data_file_name)
        df_header = pd.DataFrame(columns = header_columns)
        df_header.loc[0] = header  # Add the row header to the Header datafrome

        logger.debug("    XML Header completed -- OK.")

        if len(df_header) > 0:
            save_df (df_header, 'SEC_13F_XML_Filing_Header', save_to_file_dir)
            logger.debug("    XML Header saved -- OK.")
    else:
        logger.debug("    Error: There is no XML header in the file %s" %data_file_name)

    ###If the Line XML is not empty, Get Holding Line Info from the holding lines xml
    ###Each holding line is in a tag "<ns1:infotable>" or "<infotable>"
    if soup_lines:
        logger.debug("    XML Holding Lines parsing .......")
        lines = parse_xml_lines(soup_lines, data_file_name)
        df_lines = pd.DataFrame(lines,columns = line_columns)

        logger.debug("    XML Holding Lines completed -- OK.")

        if len(df_lines) > 0:
            save_df (df_lines, 'SEC_13F_XML_Filing_Holding_Lines', save_to_file_dir)
            logger.debug("    XML Holding Lines saved -- OK.")

    else:
        logger.debug("    Error: There is no XML holding lines in the file %s" %data_file_name)


"""
Parse the 13F data file in TEXT format
All 13F forms filed for 2013 Q2 and before  are TEXT format:  The Header part is the same, but the holding lines
are different for different companies or different years. So the DataFrames saved to the file are not the same format
and they need to further process
"""
def parser_13F_txt (txt_doc, save_to_file_dir, data_file_name):
    # SEC TEXT header format
    header_columns = ['Data_File_Name','Data_File_Format', 'CIK', 'Form_Type', 'Filied_Date', 'Calendar_Period',  'Entry_Total', 'Value_Total',
                  'Company_Name', 'Addr_Street1','Addr_Street2','Addr_City','Addr_State', 'Addr_Zipcode' ]

    # Get the header Summary info from the txt_doc

    #Get summary info
    summary_list =["Form 13F Information Table Entry Total", "Form 13F Information Table Value Total"]
    summary_list_2 =["Form13F Information Table Entry Total", "Form13F Information Table Value Total"]

    header_summary =[]
    logger.debug("    Summary parsing .......")

    try:
        for field in summary_list:
            # get match lines and remove extra space
            header_summary = header_summary+ [' '.join(line.split()) for line in txt_doc.split('\n') if field in line]
        if not header_summary: # If not find summary
            for field in summary_list_2:  #try the second summary_list
                # get match lines and remove extra space
                header_summary = header_summary+ [' '.join(line.split()) for line in txt_doc.split('\n') if field in line]

        sum_entity_total = header_summary[0].split(":")[1].replace('$', '').replace(',', '').strip() # remove '$', ',', and spaces
        sum_value_total  = header_summary[1].split(":")[1].replace('$', '').replace(',', '').strip() # remove '$', ',', and spaces
        logger.debug("    Got Summary OK")
    except Exception as e:
        logger.debug("    Error in Summary parsing")
        sum_entity_total =''
        sum_value_total =''

    # Some files have error label in the lines section " To replace ">C>" to "<C>"
    txt_doc = txt_doc.replace(">C>", "<C>") # we have some this case
    txt_doc = txt_doc.replace("<C<", "<C>") # In case

    #Then parse the txt_doc to get the SEC header info
    soup_txt_doc = BeautifulSoup(txt_doc)

    soup_header = soup_txt_doc.find("sec-header")
    soup_lines  = soup_txt_doc.find("document")


    ###If the header TXT is  not empty, Get Header Info and form the Header DataFrame
    if soup_header:
        logger.debug("    TXT Header parsing .......")
        header = parse_txt_header(soup_header, data_file_name) # Return the CIK, Report Date and Company info
        #Update teh summary total
        header [6] = sum_entity_total
        header [7] = sum_value_total
        df_header = pd.DataFrame(columns = header_columns)
        df_header.loc[0] = header  # Add the row header to the Header datafrome

        ## Remove the '$', '--', '\n' and ',' and '_' in the lines dataframe
        df_header = df_header.replace ({'\$': '', '--':'',  '\,':'', '\n':' ', '_':'' }, regex=True)

        logger.debug("    TXT Header completed -- OK.")
        if len(df_header) > 0:
            save_df (df_header, 'SEC_13F_TXT_Filing_Header', save_to_file_dir)
            logger.debug("    TXT Header saved -- OK.")
    else:
        logger.debug("    Error: There is no TXT header in the file %s" %data_file_name)

    ###If the Line TXT is not empty, Get Holding Line Info from the holding lines section
    ###Holding lines format varies for each company or years
    if soup_lines:
        logger.debug("    TXT Holding Lines parsing .......")
        lines = parse_txt_lines(soup_lines, data_file_name)
        df_lines = pd.DataFrame(lines)  #No line_columns list, since the number of columns might different

        ## Remove the '$', '--', '\n' and ',' in the lines dataframe
        df_lines = df_lines.replace ({'\$': '', '--':'',  '\,':'', '\n':' '}, regex=True)

        logger.debug("    TXT Holding Lines completed -- OK.")

        if len(df_lines) > 0:
                ## Expend the  df_lines to pre-defined column number -- 25
            current_width = len(df_lines.columns)
            now_columns = range(current_width, 25)
            for i in now_columns:
                df_lines[i] = ''

            save_df (df_lines, 'SEC_13F_TXT_Filing_Holding_Lines', save_to_file_dir)
            logger.debug("    TXT Holding Lines saved -- OK.")
    else:
        logger.debug("    Error: There is no XML holding lines in the file %s" %data_file_name)


def parse_xml_header (soup_header, file_name):
    # Since some XML may have different namespace for the tags, we use the reges to match the ending words.
    a_header = [
             file_name                                                                       ,# data_file_name
             'xml'                                                                           ,#'data_file_format',
             tag_to_string(soup_header.headerdata.find(re.compile("cik$")))                   ,# cik
             tag_to_string(soup_header.headerdata.find(re.compile("submissiontype$")))        ,# form_type
             tag_to_string(soup_header.formdata.find(re.compile("signaturedate$")) )          ,# filied_date
             tag_to_string(soup_header.headerdata.find(re.compile("periodofreport$"))  )      ,# calendar_period
             tag_to_string(soup_header.summarypage.find(re.compile("tableentrytotal$")) )     ,# entry_total
             tag_to_string(soup_header.summarypage.find(re.compile("tablevaluetotal$")) )     ,# value_total
             tag_to_string(soup_header.formdata.coverpage.find(re.compile("name$"))  )        ,# company_name
             tag_to_string(soup_header.formdata.coverpage.find(re.compile("street1$")))       ,# addr_street1
             tag_to_string(soup_header.formdata.coverpage.find(re.compile("street2$")) )      ,# addr_street2
             tag_to_string(soup_header.formdata.coverpage.find(re.compile("city$")))           ,# addr_city
             tag_to_string(soup_header.formdata.coverpage.find(re.compile("stateorcountry$"))) ,# addr_state
             tag_to_string(soup_header.formdata.coverpage.find(re.compile("zipcode$")) )        # addr_zipcode
        ]
    return a_header



def parse_txt_header (soup_header, file_name):
    # Since some XML may have different namespace for the tags, we use the reges to match the ending words.
    # Most files have "acceptance-datetime" , some do not.
    try:
        sec_header = soup_header.find("acceptance-datetime").string  # Most files at this level
    except Exception as e1:
        try:
            sec_header = soup_header.string  # some files at <sec-header> level.
        except Exception as e2:
            logger ("    The TXT file does not have validate header section")

    texts = sec_header.split("\n")

    header_list =["CENTRAL INDEX KEY", "FORM TYPE", "FILED AS OF DATE", "CONFORMED PERIOD OF REPORT",
                  "COMPANY CONFORMED NAME","STREET 1", "STREET 2", "CITY", "STATE", "ZIP" ]
    header_in_text =[]
    for a in texts:  # search the lines of txt_doc from begin to end
        x = a.replace("\t","").upper() #Remove \t's and convert to upper case
        x = ' '.join(x.split()) # remove extra space
        for filed in header_list:  # lookup the Fields List in current line
            if filed in x:
                header_in_text.append(x)

        """ The order of the fields in the Header Section of the TXT doc  will be:
        0 - "CONFORMED PERIOD OF REPORT"
        1 - "FILED AS OF DATE"
        2 - "COMPANY CONFORMED NAME"
        3 - "CENTRAL INDEX KEY"
        4 - "STATE OF INCORPORATION"   # Drop this
        5 - "FORM TYPE"
        6 - "STREET 1",
        7 - "STREET 2",
        8 - "CITY",
        9 - "STATE",
        10 - "ZIP"
        11 - mailing "STREET 1",  # Drop this
        12 - mailing "STREET 2",  # Drop this
        13 - mailing "CITY",      # Drop this
        14 - mailing "STATE",     # Drop this
        15 - mailing "ZIP"        # Drop this
        """

    ## Some Header has "STREET 2", some Header does not has "STREET 2"
    street_2=False
    for a in header_in_text:
        if 'STREET 2:' in a:
            street_2=True
    if not street_2: # If no Street 2, Insert it at the position 7
        header_in_text.insert(7, "STREET 2: ")

    a_header = [
             file_name                              ,# data_file_name
             'txt'                                  ,#'data_file_format',
             header_in_text[3].split(":")[1]        ,# cik
             header_in_text[5].split(":")[1]        ,# form_type
             header_in_text[1].split(":")[1]        ,# filied_date
             header_in_text[0].split(":")[1]        ,# calendar_period
             0                                      ,# entry_total
             0                                      ,# value_total
             header_in_text[2].split(":")[1]        ,# company_name
             header_in_text[6].split(":")[1]        ,# addr_street1
             header_in_text[7].split(":")[1]        ,# addr_street2
             header_in_text[8].split(":")[1]        ,# addr_city
             header_in_text[9].split(":")[1]        ,# addr_state
             header_in_text[10].split(":")[1]       # addr_zipcode
        ]
    return a_header


def parse_xml_lines (soup_lines, file_name):
    # Since some XML may have different namespace for the tags, we use the reges to match the ending words.

    infotab_tags = soup_lines.find_all(re.compile('infotable$'))  # Find all the tags ending with "infotable"
    lines =[]
    for infotab in infotab_tags:
        a_line = [
                file_name                                                         ,#  data_file_name
                'xml'                                                             ,#'data_file_format',
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

def parse_txt_lines (soup_lines, file_name):

    #The prettify() method will turn a Beautiful Soup parse tree into a nicely formatted Unicode string, with each HTML/XML tag on its own line:
    # PrettyFy() will turn soup object into str. it is used only for print.
    #print(soup_lines.prettify())

    #All TXT holding lines are in the tag <s><c><c>..<c>  -- one <s> and 11 <c>s
    #Some only have one <s> and 10 <c>s
    #Some only have one <s and 9 <c>s
    #Some even have 12<c>

    # some even have 2<c>s, no <s>
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
                        logger.debug("      The TXT holding lines text does not have 11c, 10c, 9c or 2c structure! ")


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
        output = [s.strip() for s in a_line.split('  ') if s]
        output.insert(0,'txt' ) # Insert the file format at the beginning
        output.insert(0,file_name ) # Insert the file name at the beginning
        if len(output) >= 7:
            lines.append(output)
    return lines

def tag_to_string( tag):
    if tag:
        text = tag.string
    else:
        text =''        
    return text

"""
If in the text, there is only one space between letter and number (L-N), or number and letter (N-L),
or number and number (N-N), then replace the single-space to doulbe-space
so that we use the doulbe- or more spaces to split the text.
"""
def std_double_space (text):
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

"""
Save the resulting DataFrame to a text file in the sub-folder "Result.
One file for each processing day. The files name will be automatically suffixed with the current date
"""
def save_df (df_name, file_base_name, save_to_file_dir):
    current_day = datetime.datetime.now().strftime("%Y%m%d")
    save_file_name = file_base_name +'_'+current_day+'.txt'
    save_file_dir = os.path.join( save_to_file_dir,'Result')
    os.makedirs(save_file_dir, exist_ok=True) # Make sure to create the folder if not existing
    save_file_path = os.path.join(save_file_dir, save_file_name )

    # Save the dataframe wihtout header and index with "|" delimited
    df_name.to_csv(save_file_path, sep='|', mode = 'a', header=False, index=False)




