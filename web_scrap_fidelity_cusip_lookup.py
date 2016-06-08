__author__ = 'luke.li'

import urllib.request
from bs4 import BeautifulSoup
import string, datetime
from .logconfig import get_logger  ### logging.basicConfig not work within Python Notebook

# Fidelity LOOKUP URLs
current_day = datetime.datetime.now().strftime("%Y%m%d") ## strftime("%Y-%m-%d %H:%M:%S") 
log_file_name =  "fidelity_security_lookup_" + current_day + ".log" 
logger = get_logger('fidelity',log_file_name)

def fidelity_symbol_lkup_by_cusip(cusip):
    """
    Purpose: For a given CUSIP number, lookup the Stock Trading Symbol and Security Name (Company Name) from the Fidelity website
    Inputs:  cusip -- string:  9 characters CUSIP# 
    Output:  A list of list: [ [cusip, name, symbol, null, fund_num] ]
    """

    PUBLIC_LKUP_URL  = r"http://quotes.fidelity.com/mmnet/SymLookup.phtml?reqforlookup=REQUESTFORLOOKUP&productid=mmnet&isLoggedIn=mmnet&rows=50"
    SEARCH_FOR   = ['stock','index','fund','annuity']

    logger.info ("    Looking up for CUSIP %s :  ..... " % str(cusip))
    for search_for in SEARCH_FOR:
        url = PUBLIC_LKUP_URL + r"&for=" + search_for + r"&by=cusip&criteria=" + str(cusip) + r"&submit=Search"
        resp = urllib.request.urlopen(url)
        page = resp.read().decode('utf-8').replace("\n","").replace("&nbsp;","").strip()
        #print(page)
        if ("The security you entered is not recognized"  in page ) :
            logger.info ("        No match for %s" % search_for )
            res =[cusip, "No Match", "No Match", None, None]
        else:
            soup = BeautifulSoup(page, "lxml")
            
            #res = [[x.string for x in y.parent.findAll('td')] for  y in soup.findAll('td', attrs={"height":"20", "align":"", "valign":""  })]
            #[x.insert(0, cusip ) for x in res ] ## Add CUSIP to the result
            
            y = soup.find('td', attrs={"height":"20", "align":"", "valign":""  })
            res = [x.string for x in y.parent.findAll('td')]
            res = [cusip] +  res ## Add CUSIP to the result
            break
            
    logger.info ("    Complete the look up for CUSIP %s :" % str(cusip))
    return res


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


def fidelity_symbol_lkup_by_cusip_list (cusip_list):
    """
    Purpose: For a given cusip list,  lookup the Stock Trading Symbols and Security Names (Company Names) from the Fidelity website.
    Inputs:  cusip_list -- A list of cusips (string:  9 characters CUSIP#)
    Output:  A list of  the list: [ [cusip, name, symbol, null, fund_num],  [cusip, name, symbol, null, fund_num], ....  ]  
    """
    return process_from_list (fidelity_symbol_lkup_by_cusip, cusip_list)
    
  