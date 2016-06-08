__author__ = 'luke.li'
import logging
import os

"""
In normal python, or the IPython console, there is no handler to the root logger installed (check: logging.getLogger().handlers)
However, in the IPython notebook, there is the default hander "stderr" to root logger installed. That is, the messages are logged into termial, not the notebook output cells.

In order to log the message into output cells in notebook, we need to add an additional  StreamHandler(stream=sys.stdout) to the logger.

"""

def get_logger (logger_name, log_file):    
    
    # setup log file location
    subdir = './logs'
    os.makedirs(subdir, exist_ok=True)
    log_file_loc = os.path.join(subdir,log_file) 
    
    # Create log formatter 
    formatter = logging.Formatter('%(asctime)s %(levelname)s: %(message)s')
    formatter.datefmt ='%Y-%m-%d %H:%M:%S'
    
        
    # Create a non-root logger  and set its log format and log level:  DEBUG -> INFO -> WARNING -> ERROR -> CRITICAL 
    logger = logging.getLogger(logger_name)
    logger.setLevel(logging.DEBUG)
    
    #print(logger_name)
    #print(len(logger.handlers))  # output: 0
   
    # Create FileHandler and set its log format
    fileHandler = logging.FileHandler(filename=log_file_loc, mode='a') 
    fileHandler.setFormatter(formatter)    
    
    # Create consoleHandler and set its log format
    consoleHandler = logging.StreamHandler()
    consoleHandler.setFormatter(formatter)
    
    #Remove the duplicate loggers (duplicate log entries)
    logger.handlers =[]
    
    # Add the handlers to the logger
    logger.addHandler(fileHandler)
    logger.addHandler(consoleHandler)    
    #print(len(logger.handlers))  # output: 2
    #print("------------") 

    return logger

"""    
========  USAGE: ===============
from  logconfig import get_logger 

logger = get_logger('AppName.ModuleName', 'test.log')

logger.info('this is info message')
logger.warning('this is warn message')
logger.debug('this is debug message')
logger.error('this is Error message')
logger.critical ('this is Critical message')
"""
