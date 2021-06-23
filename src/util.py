"""
****************************************
 * @author: Chandler Qian
 * Date: 
 * Project: 
 * Purpose: common utilities for the project
 * Python version: 3.8.1
 * Project root: 
 * Environment package: safety_rec on the remote
 ****************************************
"""

from email.mime.text import MIMEText
import time, sys
from functools import wraps
from time import strftime, localtime


def email_sender(subject, text, email):
    """ function from utils brought in so as to reduce dependency. sends an email from bigdata.cdh@bell.ca

    Args:
        subject: email subject
        text: email body
        email: email addreses to send email to

    Returns:
    """
    msg = MIMEText(text)
    msg['Subject'] = subject
    msg['From'] = 'usr@gmai.com'
    msg['To'] = ",".join(email)
    s = smtplib.SMTP('host')
    s.sendmail('usr@gmail.com', email, msg.as_string())
    s.quit()


def logger_log(filepath):
    """ added the logger arguments in order to specify the logger ths script is utilizing.

    Args:
        filepath: log file path , example: filename + strftime("%Y-%m-%d", localtime()) + "-func.log"

    Returns:
        the decorated function with proper logger
    """
    import logging
    import os
    if not os.path.exists(filepath):
        os.makedirs(filepath)
    file = filepath + '/' + strftime(
        "%Y-%m-%d", localtime()) + "-func.log"
    logger = logging.getLogger('safety cluster logger')
    if not logger.hasHandlers():  # force to only have 1 file handler rather than multiple
        logger.setLevel(
            logging.DEBUG)  # level: logging level, usually logging.DEBUG, etc..
        fh = logging.FileHandler(file)  # file handler
        fh.setLevel(logging.DEBUG)
        ch = logging.StreamHandler(sys.stdout)  # console handler
        ch.setLevel(logging.INFO)
        logger.addHandler(fh)
        logger.addHandler(ch)

    def log(func):
        """ logs start time and method run time. use wraps from functools to preserve the func.__name__, etc..

        Args:
            func: function to wrap

        Return:
            func() add prints for the start time
        """

        @wraps(func)
        def wrapper(*args, **kwargs):
            start = time.time()
            func_str = func.__name__
            run_time = strftime("%Y-%m-%d %H:%M:%S", localtime())
            result = func(*args, **kwargs)
            end = time.time()
            logger.info(
                '-------------------' + func_str + ' start running @' + str(
                    run_time) + '-------------------')
            logger.info(
                '-------------------' + func_str + ' completed, spend in total ' + str(
                    end - start) + ' seconds-------------------')
            return result

        return wrapper

    return log

