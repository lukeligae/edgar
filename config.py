__author__ = 'luke.li'
import logging
import os 


### logging.basicConfig not work within Python Notebook
#We  have to manually configure the logging to use in Python Notebook
##logging.basicConfig(filename="c:\sec_data.log", level=logging.DEBUG, format='%(asctime)s %(message)s', datefmt='%m/%d/%Y %I:%M:%S %p',)
##logger = logging.getLogger("edgar_data")

def get_logger (logger_name, log_file):
    subdir = './logs'
    os.makedirs(subdir, exist_ok=True)
    log_file_loc = os.path.join(subdir,log_file)
    logger = logging.getLogger(logger_name)
    fhandler = logging.FileHandler(filename=log_file_loc, mode='a')
    formatter = logging.Formatter('%(asctime)s %(name)s: %(message)s')
    formatter.datefmt ='%Y-%m-%d %H:%M:%S'
    fhandler.setFormatter(formatter)
    logger.addHandler(fhandler)
    logger.setLevel(logging.DEBUG)

    return logger
