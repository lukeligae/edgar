import urllib.request
from bs4 import BeautifulSoup
import string, datetime
import hashlib
from .logconfig import get_logger  ### logging.basicConfig not work within Python Notebook

current_day = datetime.datetime.now().strftime("%Y%m%d") ## strftime("%Y-%m-%d %H:%M:%S") 
log_file_name =  "yahoo_finance_lookup_" + current_day + ".log" 

logger = get_logger('yahoo',log_file_name)

def yf_get_key_stat_columns ():
    """
    Purpose: Get the columns list of the Key Statistics from the Yahoo Finance website.
    Inputs:  None 
    Output:  A list of KS column names ["column1","column2", ....]
    
    Notes:   If the column name start with numeric char, add 'x' in front of it.  
    """
    url = "http://finance.yahoo.com/q/ks?s=YHOO+Key+Statistics"
    resp = urllib.request.urlopen(url)
    page = resp.read().decode('utf-8').replace("\n","").replace("&nbsp;","").strip()        
    soup = BeautifulSoup(page, 'lxml') 
    
    #Get all the KS fields, but we need the first 31 of them 
    # Use tag.text, not tag.string
    ks_kv_pairs = [[x.text for x in y.parent.contents] for  y in soup.findAll('td', attrs={"class" : "yfnc_tablehead1"})][:31]
    ks_columns_org  = [ str(x[0]).lower() for x in ks_kv_pairs] # covert to string and lower case
    ks_columns = [col.split('(')[0].replace(':', '').strip().replace(' ', '_').replace('/','_over_') \
              .replace('-', '_').replace('&','').replace('%', 'pct').lower() for col in ks_columns_org ]
    #Add 'x' in beginning of column name if it does not start with a letter
    ks_columns = [ 'x'+ col if not col[0].isalpha() else col  for col in ks_columns ]
    
    return ks_columns

        


def yf_get_key_stat_from_symbol (cusip_symbol):
    """
    Purpose: For a given tuple (cusip, symbol), lookup the Key Statistics from the Yahoo Finance website.
    Inputs:  cusip_symbol -- A list [cusip, SYM] 
    Output:  A list [cusip, SYM, matched_flag, matched_symbol, symbol_search_path, ks_md5_hash ] +  ks_values 
    """
    cusip =  cusip_symbol[0]
    SYM   =  cusip_symbol[1]
    
    logger.info ("    Looking up for symbol %s :  ..... " % SYM)
    
     #Try orginal symbol first, if not found, then try "-", lastly try "." if it ontains '/' 
    lkup_symbols = [SYM,  SYM.replace("/", "-"), SYM.replace("/", ".")]
    cnt = 0
    for symbol in lkup_symbols: 
        cnt = cnt + 1
        matched_symbol = symbol
        symbol_search_path = ' > '.join(lkup_symbols[:cnt])
        
        url = "http://finance.yahoo.com/q/ks?s=" + SYM + "+Key+Statistics"
        resp = urllib.request.urlopen(url)
        page = resp.read().decode('utf-8').replace("\n","").replace("&nbsp;","").strip()        
        soup = BeautifulSoup(page, 'lxml')       
        
        #Get all the KS fields, but we need the first 31 of them 
        # Use tag.text, not tag.string
        ks_kv_pairs = [[x.text for x in y.parent.contents] for  y in soup.findAll('td', attrs={"class" : "yfnc_tablehead1"})][:31]
        ks_values   = [ str(x[1]) for x in ks_kv_pairs] # convert to string for computing the md5-hashing
        
        if ks_values:
            # Hashing based on all the KS values if           
            ks_md5_hash    = hashlib.md5(''.join(ks_values).encode('utf-8')).hexdigest()
            matched_flag = 'Y'
        else:
            ks_md5_hash    = hashlib.md5(''.encode('utf-8')).hexdigest()
            matched_flag = 'N'
            
        if (matched_flag == 'N') and (cnt < 3 ) and ("/" in SYM):
            continue
        else:
            break
        
    if matched_flag == 'Y':
        # Add 6 additional items to the KS list (58 items)
        result = [cusip, SYM, matched_flag, matched_symbol, symbol_search_path, ks_md5_hash ] +  ks_values 
        logger.info ("    Complete the look up and found the KS for symbol %s through the symbol search path: %s" % (SYM, symbol_search_path))
        return  result  
    else: 
        # Total 58 KS values, but we only take the 31 important ones
        nomatch_result = [cusip, SYM, matched_flag, matched_symbol, symbol_search_path, ks_md5_hash ] +  [None for x in range(31)] 
        logger.info ("    Complete the look up and did not find the KS for symbol %s through the symbol search path: %s" % (SYM, symbol_search_path))
        return nomatch_result
    
    
def yf_get_income_stat_from_symbol(SYM):

    logger.info ("Looking up for symbol %s :  ..... " % SYM)
    url = "http://finance.yahoo.com/q/is?s=IBM+Income+Statement&annual"

    page = urllib.request.urlopen(url)
    soup = BeautifulSoup(page, "lxml")
    res = [[x.text for x in y.parent.contents] for  y in soup.findAll('td', attrs={"colspan" : "2"})]

    res = [[y.replace(u"\n","") for y in x] for x in res]
    res = [[y.replace(u"\xa0","").strip() for y in x] for x in res]
    res = [x for x in res if len(x) > 1]

    res = [[y.replace(u"-","0") for y in x] for x in res]
    res = [[y.replace(u",","") for y in x] for x in res]

    for i in range(len(res)):
        if 'Period' in res[i][0]:
            for j in range(1,len(res[i])):
                res[i][j] = datetime.datetime.strptime(str(res[i][j]), '%b %d %Y')
        else:
            for j in range(1,len(res[i])):
                res[i][j] = int(res[i][j] )
    logger.info ("Complete the Yahoo Income Stats lookup for symbol %s :" % SYM)
    return 	res


def yf_get_profile_from_symbol (cusip_symbol):
    
    """
    Purpose: For a given tuple (cusip, symbol), lookup the Stock Profile or Fund Overview from the Yahoo Finance website.
    Inputs:  cusip_symbol -- A list [cusip, SYM] 
    Output:  A list ["cusip", "symbol", "matched_flag", "legal_type", "matched_symbol", "symbol_search_path", "index_Membership", 
            "sector", "industry", "full_time_emp", "fund_category",  "fund_family", "fund_net_assets", "fund_yield", "fund_nception_date"]
    """
    cusip =  cusip_symbol[0]
    SYM   =  cusip_symbol[1]
    
    logger.info ("Looking up for symbol %s :  ..... " % SYM)
    
    #Try orginal symbol first, if not found, then try "-", lastly try "." if it ontains '/' 
    lkup_symbols = [SYM,  SYM.replace("/", "-"), SYM.replace("/", ".")]
    cnt = 0
    for symbol in lkup_symbols: 
        cnt = cnt + 1
        matched_symbol = symbol
        symbol_search_path = ' > '.join(lkup_symbols[:cnt])
        url = "http://finance.yahoo.com/q/pr?s=" + symbol + "+Profile"
        resp = urllib.request.urlopen(url)
        page = resp.read().decode('utf-8').replace("\n","").replace("&nbsp;","").strip()
        
        if '<span class="yfi-module-title">Business Summary</span>' in page:
            legal_type = 'Stock'
        elif '<span class="yfi-module-title">Fund Summary</span>' in page:
            legal_type = 'ETF'
        else:
            legal_type = None
            
        
        if (not legal_type) and (cnt < 3 ) and ("/" in SYM):
            continue
        else:
            break
            
    if legal_type:
        soup = BeautifulSoup(page, 'lxml')
        table_cells = soup.find('td', attrs={"class" : "yfnc_tablehead1", "width":"50%"}).parent.parent.findAll('td') 
        cell_values = [x.string for x in table_cells] 
    else:
        cell_values = None
      
    result = None
    #res_cols = ["cusip", "symbol", "matched_flag", "legal_type", "matched_symbol", "symbol_search_path", "index_Membership", "sector", "industry",   
    #             "full_time_emp", "fund_category",  "fund_family", "fund_net_assets", "fund_yield", "fund_nception_date"]
    
    if cell_values:  # If find the Profile Page
        if legal_type == 'Stock':  
            # Stock: ['Index Membership:', 'N/A', 'Sector:', 'Healthcare', 'Industry:', 'Specialized Health Services',
            #         'Full Time Employees:', '1,600']
            # stock result  -  4 columns,    fund result  -  5 columns,
            result =[cusip, SYM, 'Y', legal_type, matched_symbol, symbol_search_path, cell_values[1], cell_values[3], cell_values[5], cell_values[7], None, None, None, None, None]
                                
        elif legal_type == 'ETF':  
            # Fund:  ['Category:', 'Energy Limited Partnership', 'Fund Family:', 'ALPS',
            # 'Net Assets:', '8.16B', 'Yield:', '9.73%', 'Fund Inception Date:', 'Aug 25, 2010', 'Legal Type:', 'Exchange Traded Fund']
            result =[cusip, SYM, 'Y', legal_type, matched_symbol, symbol_search_path, None, None, None, None, cell_values[1], cell_values[3], cell_values[5], cell_values[7],  cell_values[9] ]
        else:
             result = None
                
    if  result:
        logger.info ("Complete the look up and found the Profile Page for symbol %s through the symbol search path: %s" % (SYM, symbol_search_path))
        return  result  
    else:  
        nomatch_result =[cusip, SYM, 'N', legal_type, matched_symbol, symbol_search_path] + [None for x in range(9)]
        logger.info ("Complete the look up and did not find Profile Page for symbol %s through the symbol search path: %s" % (SYM, symbol_search_path))
        return nomatch_result


def process_from_list(procedure, input_list):
    
    """
    Purpose: For a given input list, call the procedure to process individual item of the list and then return a list of the results
    Inputs:  procedure_name -- 
             input_list     -- A list of inputs , like [cusips, symbol] 
    Output:  A list of  the results  [ result1, result2,  ....  ] , where result =[item1, item2, ..., ]
    """
    count = 0
    total = len(input_list) 
    results = []
    for input in input_list:
        count = count + 1
        logger.info ("Process status: %s of %s ......" %(str(count), str(total)) ) 
        input_res =  procedure(input)
        results.append(input_res)

    logger.info ("Complete all the total %s symbol lookkups from Yahoo Finance." %str(total) )     
    return results



def yf_get_profile_from_symbol_list(cusip_symbol_list):
    """
    Purpose: For a given list of tuple (cusip, symbol), lookup the Stock Profile or Fund Overview from the Yahoo Finance website. 

    Inputs:  cusip_symbol_list -- A list of tuple [cusips, symbol] 
    Output:  A list of  the list: [ profile_result, profile_result,  ....  ] , 
             where  profile_result =[ "cusip", "symbol", "legal_type",..]
    """
    return process_from_list(yf_get_profile_from_symbol, cusip_symbol_list)



def yf_get_key_stat_from_symbol_list(cusip_symbol_list):
    
    """
    Purpose: Purpose: For a given tuple (cusip, symbol), lookup the Key Statistics from the Yahoo Finance website.

    Inputs:  cusip_symbol_list -- A list of  [cusips, symbol] 
    Output:  A list of  the list: [ ks_result, ks_result,  ....  ] , 
             where  ks_result = [cusip, SYM, matched_flag, matched_symbol, symbol_search_path, ks_md5_hash,58_ks_values ]
    """
    return process_from_list(yf_get_key_stat_from_symbol, cusip_symbol_list)

    

