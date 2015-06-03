__author__ = 'luke.li'
"""
Purpose: For a given CUSIP number,
1. Firstly lookup the Stock Trading Symbol and Security Name (Company Name) from the Fidelity website
2. Secondly, Use the found trading symbol to scrap Yahoo! Finance data to get the Company's market-cap, Industry and Sector information
"""

import urllib.request
from bs4 import BeautifulSoup
import string, datetime
from .config import get_logger  ### logging.basicConfig not work within Python Notebook

# Fidelity LOOKUP URLs


log_file_name = "Fidelity_Security_Lookup.log"
logger = get_logger('lkup',log_file_name)

def fidelity_search_by_cusip(cusip):

    PUBLIC_LKUP_URL  = r"http://quotes.fidelity.com/mmnet/SymLookup.phtml?reqforlookup=REQUESTFORLOOKUP&productid=mmnet&isLoggedIn=mmnet&rows=50"
    SEARCH_FOR   = ['stock','index','fund','annuity']
    SEARCH_BY   = {'SecurityName':'stock','CUSIP':'cusip','TradingSymbol':'symbol','FundNum':'fund'}

    if len(cusip) !=9:
        logger.debug ("Provided CUSIP %s is invalid. It must have length of 9!" % str(cusip))
        res = None
    else:
        logger.debug ("Looking up for CUSIP %s :  ..... " % str(cusip))
        for search_for in SEARCH_FOR:
            url = PUBLIC_LKUP_URL + r"&for=" + search_for + r"&by=cusip&criteria=" + str(cusip) + r"&submit=Search"
            resp = urllib.request.urlopen(url)
            page = resp.read().decode('utf-8').replace("\n","").replace("&nbsp;","").strip()
            #print(page)
            if ("The security you entered is not recognized"  in page ) :
                logger.debug ("   No match for %s" % search_for )
                res =[[cusip, "No Match", "No Match", None, None]]
            else:
                soup = BeautifulSoup(page)
                res = [[x.string for x in y.parent.contents] for  y in soup.findAll('td', attrs={"height":"20", "align":"", "valign":""  })]
                [x.insert(0, cusip ) for x in res ] ## Add CUSIP to the result
                break
        logger.debug ("Complete the look up for CUSIP %s :" % str(cusip))
    return res
