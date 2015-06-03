import urllib.request
from bs4 import BeautifulSoup
import string, datetime
from .config import get_logger  ### logging.basicConfig not work within Python Notebook

log_file_name = "Yahoo_Finance_Lookup.log"
logger = get_logger('lkup',log_file_name)


def yf_get_key_stat(SYM):

    url = "http://finance.yahoo.com/q/ks?s=" + SYM + "+Key+Statistics"
    page = urllib.request.urlopen(url)
    soup = BeautifulSoup(page)
    res = [[x.text for x in y.parent.contents] for  y in soup.findAll('td', attrs={"class" : "yfnc_tablehead1"})]
    return res

def yf_get_inc_stat(SYM):

    logger.debug ("Looking up for symbol %s :  ..... " % SYM)
    url = "http://finance.yahoo.com/q/is?s=IBM+Income+Statement&annual"

    page = urllib.request.urlopen(url)
    soup = BeautifulSoup(page)
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
    logger.debug ("Complete the look up for symbol %s :" % SYM)
    return 	res


def yf_get_profile (SYM):

    logger.debug ("Looking up for symbol %s :  ..... " % SYM)

    url = "http://finance.yahoo.com/q/pr?s=" + SYM + "+Profile"
    page = urllib.request.urlopen(url)
    soup = BeautifulSoup(page)
    if soup:
        res = [[x.text for x in y.parent.contents] for  y in soup.findAll('td', attrs={"class" : "yfnc_tablehead1", "width":"50%"})]
    else:
        res = None

    if res:  # If find the Profile Page
        logger.debug ("Complete the look up and found the Profile Page for symbol %s :" % SYM)
        return res
    elif ('/' in SYM): # else, if  the SYM contains "/"
        sym_var = SYM.replace('/', '.')
        logger.debug ("    Can't find the Yahoo Profile Page for symbol %s. Re-try the varied symbol %s:" % (SYM, sym_var))
        res2 = yf_get_profile(sym_var)
        if res2:
            logger.debug ("    Re-try the varied symbol %s and found the profile page." % (sym_var))
            logger.debug ("Complete the look up and found the Profile Page for symbol %s :" % SYM)
            return res2
        else:
            logger.debug ("    Re-try the varied symbol %s and did not find the profile page." % (sym_var))
            logger.debug ("Complete the look up and did not find Profile Page for symbol %s :" % SYM)
            return None
    else: # if 1st not found and symbol not contains '/'
        logger.debug ("Complete the look up and did not find Profile Page for symbol %s :" % SYM)
        return None
