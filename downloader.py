__author__ = 'luke.li'

import datetime
import zipfile
import tempfile
import os.path
from .config import get_logger  ### logging.basicConfig not work within Python Notebook

FTP_ADDR = "ftp.sec.gov"
EDGAR_PREFIX = "ftp://%s/" % FTP_ADDR
CURRENT_YEAR = datetime.date.today().year
CURRENT_QUARTER = (datetime.date.today().month-1)//3 + 1

current_day = datetime.datetime.now().strftime("%Y%m%d") ## strftime("%Y-%m-%d %H:%M:%S") 
log_file_name =  "sec_file_download_" + current_day + ".log" 

### logging.basicConfig not work within Python Notebook
#logging.basicConfig(filename="c:\sec_data.log", level=logging.DEBUG, format='%(asctime)s %(message)s', datefmt='%m/%d/%Y %I:%M:%S %p',)
#logger = logging.getLogger("edgar_data")
logger = get_logger('sec_index',log_file_name)


def quarterly_file_list(from_year=1993, to_year=CURRENT_YEAR ):
    """
    Generate the list of quarterly zip files archived in EDGAR
    since 1993 until the specified to_year.
    If the to_year is null or current year, then down all the files until previous quarter of this year.
    """
    logger.debug("Generating quarter files from the Year %s to the Year %s"  % (from_year, to_year))
    years = range(from_year, to_year +1)
    quarters = ["QTR1", "QTR2", "QTR3", "QTR4"]
    history =  list((y, q) for y in years for q in quarters)
    history.reverse()

    """
    # The following code block remove the future year quarters

    quarter = "QTR%s" % CURRENT_QUARTER
    while history:
        y, q = history[0]
        if ( (y == CURRENT_YEAR) and ( q == quarter)):
            history.pop(0)
            break
        else:
            history.pop(0)
    """
    return ["edgar/full-index/%s/%s/master.zip" % (x[0], x[1]) for x in history]


def daily_file_list(from_date, to_date=current_day ):
    """ 
    Generate the list of daily index files archived in EDGAR for the current quarter until the date specified by to_date.
    The to_date is excluded. Default to_date is current day
    from_date and to_date format: yyyymmdd
    """
    logger.debug("Generating daily files from the Date %s to the Date before %s." % (from_date, to_date))
    dates = range(from_date, to_date)
    return ["edgar/daily-index/master.%s.idx" % x for x in dates]


def ftp_retr(ftp, filename, buffer):
    """ Write remote filename's bytes from ftp to local buffer """
    ftp.retrbinary('RETR %s' % filename, buffer.write)
    #logger.debug("FTP RETR %s" % filename)
    return buffer


def download_all_index_daily(ftp, from_date=(CURRENT_YEAR * 10000 + 101), to_date=current_day, dest=""):
    """
    Convenient method to download all daily files at once. Deafult from date is the first day of the current year
    """
    if not dest:
         dest = tempfile.mkdtemp()

    for file in daily_file_list(from_date, to_date):
        download_daily_index(ftp, file, dest)

def download_all_index_quarter(ftp, from_year=1993, to_year=CURRENT_YEAR, dest=""):
    """
    Convenient method to download all quarter files at once
    """
    if not dest:
         dest = tempfile.mkdtemp()

    for file in quarterly_file_list(from_year, to_year):
        download_quarter_index(ftp, file, dest)


def download_daily_index(ftp, file, dest):
    """
    Download a daily index file and remove the first 7 lines of csv headers
    """
    dest = os.path.normpath(dest) #Make path name works both for Window or Unix format	

    logger.debug("downloading %s file in %s" % (file, dest))

    if file.startswith(EDGAR_PREFIX):
        file = file[len(EDGAR_PREFIX):]

    dest_name = file.replace("/", ".")
    full_dest = os.path.join(dest,dest_name)

    with tempfile.TemporaryFile() as tmp:
        try:
            ftp_retr(ftp, file, tmp)
            tmp.seek(0) 
            for x in range(0,7): # remove csv headers
                tmp.readline()
            tmp.seek(0, 1) #reset 
            with open(full_dest, 'w') as idxfile:
                idxfile.write(tmp.read().decode('utf-8'))
            logger.debug("wrote %s" % full_dest)
        except Exception as e:
            logger.debug ("Error in downloading the file: %s. Error msg: %s" %(file,e))

def download_quarter_index(ftp, file, dest):
    """
    Download an quarterly archived zipped idx file or archive from EDGAR
    This will  unzip archives + read the master.idx file inside	 and remove the first 7 lines of csv headers
    """
    dest = os.path.normpath(dest) #Make path name works both for Window or Unix format	

    logger.debug("downloading %s file in %s" % (file, dest))

    if file.startswith(EDGAR_PREFIX):
        file = file[len(EDGAR_PREFIX):]

    dest_name = file.replace("/", ".").replace("zip", "idx")
    full_dest = os.path.join(dest,dest_name)

    with tempfile.TemporaryFile() as tmp:
        try:
            ftp_retr(ftp, file, tmp)
            with zipfile.ZipFile(tmp).open("master.idx") as z:
                for x in range(0,10):
                    z.readline()
                with open(full_dest, 'w') as idxfile:
                    idxfile.write(z.read().decode('utf-8'))
            logger.debug("wrote %s" % full_dest)
        except Exception as e:
            logger.debug( "Error in downloading the file: %s. Error msg: %s" %(file,e))

def download_data_file(ftp, file, dest):
    """
    Download a daily index file and remove the first 7 lines of csv headers
    """
    dest = os.path.normpath(dest) #Make path name works both for Window or Unix format	

    logger.debug("    downloading %s file in %s" % (file, dest))

    if file.startswith(EDGAR_PREFIX):
        file = file[len(EDGAR_PREFIX):]

    dest_name = file.replace("/", ".")
    full_dest = os.path.join(dest,dest_name)

    with tempfile.TemporaryFile() as tmp:
        try:
            ftp_retr(ftp, file, tmp)
            tmp.seek(0) 
            with open(full_dest, 'w') as idxfile:
                idxfile.write(tmp.read().decode('utf-8'))
            logger.debug("wrote %s" % full_dest)
        except Exception as e:
            logger.debug("Error in downloading the file: %s. Error msg: %s" %(file,e))
