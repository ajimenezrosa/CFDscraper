#! /usr/bin/env python3
# -*- coding: utf-8
"""
A module to scrape financial data from web tables.
"""

import sys
from time import sleep, time
import datetime
##### For scraping ######
from selenium import webdriver
from selenium.common.exceptions import NoSuchWindowException
from bs4 import BeautifulSoup
from sqlalchemy import (create_engine, MetaData, Table, Column,
                        Integer, Float, DateTime)
from dateutil.parser import parse
import pandas as pd
##### Logging ############
import logging
import logging.handlers
###### For timeout ########
from functools import wraps
import errno
import os
import signal


######## Set up logging  ######################################################
logger = logging.getLogger('__name__')
logger.setLevel(logging.DEBUG)
# create file handler which logs even debug messages
file_hand = logging.handlers.RotatingFileHandler('scrapeLog.log',
                                                 maxBytes=10000,
                                                 backupCount=2)
file_hand.setLevel(logging.INFO)  # Set logging level here.
# create console handler with a higher log level
console_hand = logging.StreamHandler()
console_hand.setLevel(logging.DEBUG)  # Set logging level here. Normally INFO
# create formatter and add it to the handlers
form_string = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
formatter = logging.Formatter(form_string)
formatter2 = logging.Formatter('%(message)s')
console_hand.setFormatter(formatter2)
file_hand.setFormatter(formatter)
# add the handlers to logger
logger.addHandler(console_hand)
logger.addHandler(file_hand)


###### Make timeout wrapper for pageloads and such ############################
class TimeoutError(Exception):
    pass


def timeout(seconds=10, error_message=os.strerror(errno.ETIME)):
    """
    Timeout wrapper.
    From:
    http://stackoverflow.com/questions/2281850/
    timeout-function-if-it-takes-too-long-to-finish?lq=1
    """
    def decorator(func):
        def _handle_timeout(signum, frame):
            raise TimeoutError(error_message)
        #@wraps(func)

        def wrapper(*args, **kwargs):
            signal.signal(signal.SIGALRM, _handle_timeout)
            signal.alarm(seconds)
            try:
                result = func(*args, **kwargs)
            finally:
                signal.alarm(0)
            return result

        return wraps(func)(wrapper)

    return decorator


###### Configuration data #####################################################

total_rows_scraped = 0  # Don't change this. It's just a counter.
last_write_time = time()  # Also a counter.
browser_choice = "chrome"  # Choose chrome, firefox, or phantomjs

chromepath = '/Users/jonathanamorris/Code/chromedriver'

# Database info:
db_host = 'dataserve.local'
db_user = 'jonathan'
db_pass = 
db_name = 'mydb'
db_dialect = 'mysql+pymysql'
connect_string = (db_dialect + '://' +
                  db_user + ':' +
                  db_pass + '@' +
                  db_host + '/' +
                  db_name)

# Page info:
page_source_timeout = 5  # In seconds. Must be an integer!
browser_lifetime = 1680  # In seconds. 14400 is four hours.
base_url = 'http://www.investing.com'
url_string = base_url + '/rates-bonds/government-bond-spreads'
web_tz = 'GMT'

# Table info:
attribute = {'id': 'bonds'}
time_col = "UTCTime"
row_title_column = 'Country'  # Need this to know index column.
refresh_rate = 10.5  # Number of seconds between scrapes.

# Construct table form:
# bootstrap = (db_table_name,
#         ((db_column1_name, web_row_string, web_col_string),
#         (db_column2_name, web_row_string, web_col_string)))
# Timestamp column name is special and will be made primary key
# All others will be Float by default.
# The timestamp column, whatever its dytpe, must be the first for
# everything to work.

###### Tables list ############################################################

bootstrap_list = []

bootstrap1 = ("German10yrbond",
             (("UTCTime", "Germany", "Time"),
              ("Rate", "Germany", "Yield")))
bootstrap_list.append(bootstrap1)

bootstrap2 = ("Spain10yrbond",
             (("UTCTime", "Spain", "Time"),
              ("Rate", "Spain", "Yield")))
bootstrap_list.append(bootstrap2)

bootstrap3 = ("US10yrbond",
             (("UTCTime", "United States", "Time"),
              ("Rate", "United States", "Yield")))
bootstrap_list.append(bootstrap3)

bootstrap4 = ("France10yrbond",
             (("UTCTime", "France", "Time"),
              ("Rate", "France", "Yield")))
bootstrap_list.append(bootstrap4)

bootstrap5 = ("Japan10yrbond",
             (("UTCTime", "Japan", "Time"),
              ("Rate", "Japan", "Yield")))
bootstrap_list.append(bootstrap5)

bootstrap6 = ("Australia10yrbond",
             (("UTCTime", "Australia", "Time"),
              ("Rate", "Australia", "Yield")))
bootstrap_list.append(bootstrap6)

bootstrap7 = ("Italy10yrbond",
             (("UTCTime", "Italy", "Time"),
              ("Rate", "Italy", "Yield")))
bootstrap_list.append(bootstrap7)

bootstrap8 = ("Portugal10yrbond",
             (("UTCTime", "Portugal", "Time"),
              ("Rate", "Portugal", "Yield")))
bootstrap_list.append(bootstrap8)

bootstrap_list.sort()


######## Open database and check that it can be reached #######################
def db_setup():
    """
    Connects to the database using SQLalchemy-core. This is the only
    function that is called outside of main().
    """
    logger.info('Connecting to database.')

    try:
        engine = create_engine(connect_string,
                               echo=False,
                               pool_recycle=3600)
        metadata = MetaData(bind=engine)
        conn = engine.connect()

    except:
        logger.error('ERROR: Database not reachable. Exiting')
        sys.exit()

    return engine, metadata, conn


########## Webdrivers class #########################################
class Browser(object):
    """
    Wrapper class for webdriver.

    Usage:
    browser = Browser()
    browser = Browser("phantomjs")  # Default is chrome, also firefox.
    browser.refresh()
    browser.quit()
    browser.age()
    browser.type()
    browser.source()

    TODO:
    Move popup close and url load to separate functions.
    Make internal methods "private".

    Note: Python classes are brilliant. Self is passed so
    that functions can access the class namespace and be more flat.
    """
    def __init__(self, browser_type="chrome"):
        self.browser_type = browser_type.lower()
        self.driver = self.new_driver(self.browser_type)
        self.start_time = time()

    def new_driver(self, browser_type):
        if browser_type == "chrome":
            driver = self.new_chrome_driver()
        elif browser_type == "firefox":
            driver = self.new_firefox_driver()
        elif browser_type == "phantomjs":
            driver = self.new_phantomjs_driver()
        else:
            logger.critical("Invalid browser choice. Exiting")
            clean_up(self)
        self.start_time = time()
        return driver

    def new_chrome_driver(self):
        """
        Opens or refreshes a Chrome webdriver instance.

        Options:
        http://peter.sh/experiments/chromium-command-line-switches/
        """
        try:
            options = webdriver.ChromeOptions()
            options.add_argument('--disable-bundled-ppapi-flash')
            options.add_argument('--disable-pepper-3d')
            options.add_argument('--disable-internal-flash')
            options.add_argument('--disable-flash-3d')
            options.add_argument('--disable-flash-stage3d')
            options.add_argument('--disable-core-animation-plugins')
            options.add_argument('--disable-plugins')
            options.add_argument('--views-corewm-window-animations-disabled')
            # options.add_argument('--disable-images')
            # options.add_argument('--disable-javascript') # bad idea
            # list of switches: print(options.arguments)
            logger.info("Loading Chrome webdriver.")
            driver = webdriver.Chrome(executable_path=chromepath,
                                      chrome_options=options)
            logger.info("Loading webpage.")
            driver.get(url_string)
        except:
            logger.error("Can't open webdriver.")
        try:
            # browser.find_element_by_class_name("popupAdCloseIcon").click()
            driver.find_element_by_partial_link_text("Continue").click()
        except:
            logger.critical("ERROR: Can't close the popup.")
            clean_up(self)
        return driver

    def new_firefox_driver(self):
        """
        Opens or refreshes a Firefox browser and closes the popup.
        I switched to Firefox from Chrome because for some reason lxml doesn't
        work with Chrome and Python 3.3. Because unicode from Chrome being
        ignored by lxml.
        """
        ## Firefox profile object
        try:
            firefox_profile = webdriver.FirefoxProfile()
            # Disable images
            # firefox_profile.set_preference('permissions.default.image', 2)
            # Diasble flash
            firefox_profile.set_preference(
                'dom.ipc.plugins.enabled.libflashplayer.so', 'false')
            # (try to) Disable popups
            firefox_profile.set_preference('network.http.prompt-temp-redirect',
                                           'false')
            # browser.browserHandle = webdriver.Firefox(firefox_profile)

            firefox_profile.set_preference('plugin.state.flash', 0)
            logger.info("Loading FireFox webdriver.")
            driver = webdriver.Firefox(firefox_profile)
        except:
            logger.critical("ERROR: Can't open browser.")
            clean_up(self)
        try:
            logger.info("Loading webpage.")
            driver.get(url_string)
        except:
            logger.critical("ERROR: Can't open page.")
            clean_up(self)
        try:
            driver.find_element_by_partial_link_text("Continue").click()
        except:
            logger.critical("ERROR: Can't close popup.")
            clean_up(self)
        return driver

    def new_phantomjs_driver(self):
        """
        Opens or refreshes a ghostjs webdriver.

        For OSX:
        brew install phantomjs

        For others:
        http://phantomjs.org/download.html
        """
        try:
            # Is this right? a capital letter?
            logger.info("Loading PhantomJS webdriver.")
            driver = webdriver.PhantomJS('phantomjs')
        except:
            logger.critical("ERROR: Can't open browser.")
            clean_up(self)
        try:
            logger.info("Loading webpage.")
            driver.get(url_string)
        except:
            logger.critical("ERROR: Can't open page.")
            clean_up(self)
        try:
            driver.find_element_by_partial_link_text("Continue").click()
        except:
            logger.critical("ERROR: Can't close popup.")
            clean_up(self)
        return driver

    def refresh(self):
        """ """
        try:
            self.driver.quit()
        except:
            pass
        self.driver = self.new_driver(self.browser_type)

    def type(self):
        return self.browser_type

    def age(self):
        self.browser_age = (time() - self.start_time)
        return self.browser_age

    def quit(self):
        self.driver.quit()
        return

    def source(self):
        logger.debug("Browser.source() called.")
        try:
            self.html_source = self.source_inner()

        except NoSuchWindowException:
            logger.error("Window missing.")
            self.refresh()
            try:
                self.html_source = self.source_inner()
            except:
                logger.error("Second try on source load failed. Exiting")
                clean_up(self)
        except TimeoutError:
            logger.error("Time limit exceeded for webdriver.page_source.")
            logger.error("Refreshing webdriver.")
            self.refresh()
            try:
                self.html_source = self.source_inner()
            except:
                logger.error("Second try on source load failed. Exiting")
                clean_up(self)
        return self.html_source

    @timeout(page_source_timeout)
    def source_inner(self):
        """
        Wrapper for browser.page_source so that it can be timed out if hung.
        """
        return self.driver.page_source  # Must be unbound method.

#####################################################################


def setup_tables(bootstrap_list, metadata):
    """
    Creates needed tables in the database using bootstrap_list as guide.

    TODO:
    Autoincrement isn't being set on the integer column.
    "Setting the autoincrement field has no effect for columns that are
    not part of the primary key."
    There are ways around this but they seem like hacks that will not
    be portable to another database.
    Update: Now have two primary keys. Problem? Not sure.
    """
    logger.info("Setting up database tables.")
    for entry in bootstrap_list:
        column_list = [row[0] for row in entry[1]]
        Table(entry[0], metadata,
              Column('id', Integer(),
                     nullable=False,
                     autoincrement=True,
                     primary_key=True),
              *((Column(time_col, DateTime(),
                        primary_key=True,
                        autoincrement=False,
                        nullable=False))
                if colname == time_col
                else (Column(colname, Float(), nullable=False))
                for colname in column_list))
    metadata.create_all()


def get_last_row_dict(table_title):
    """
    Gets the last entry in the table for to see if the web entry is
    new enough to update.
    """
    sql_table = Table(table_title, metadata, autoload=True)
    query = sql_table.select().order_by('-id').limit(1)
    result_set = query.execute()
    keys = result_set.keys()
    values = result_set.fetchone()
    if values is None:
        values = len(keys) * [None]
    data_dict = dict(zip(keys, values))
    return data_dict


def fill_from_db(bootstrap_list, conn):
    """
    Using bootstrap_list as guide, creates list_of_rows and fills from last
    entry in the db.
    """
    logger.info("Loading last database rows.")
    list_of_rows = []
    for entry in bootstrap_list:
        # print("bootstrap row: ", entry[0])
        row_dict = get_last_row_dict(entry[0])
        # print("row dict: ", row_dict)
        col_list = []
        for column in entry[1]:
            # print (column[0])
            col = [column[0], row_dict[column[0]]]
            col_list.append(col)
        # print("col_list: ", col_list)
        row = [entry[0], col_list]
        logger.debug("Loaded row from db: %s", str(row))
        list_of_rows.append(row)
    return list_of_rows


def browser2dframe(browser, attribute):
    """
    Makes a dataframe from a webdriver instance given a table
    attribute: {'id':'bonds'}.
    TODO:
    Exhibits a strange bug where after 15-30 calls the time for execution
    grows from ~ 0.290s to 8 seconds and then to 20. Why?
    The culprit is browser.page_source()
    Fixed! Moved from Firefox to phantomjs. Works much, much faster and
    with much less memory.

    Stupid lxml is causing me stress. ["lxml", "xml"] is best for
    Firefox but phantomjs and Chrome work only with html5lib so that
    is what I'm going with. The difference is only 330 milliseconds
    so that's fine for now. Write an lxml-based custom parser later.
    (I gained around that much when I switched to phantomjs so that
    is also fine.)
    """
    profiler = []
    start1 = time()
    logger.debug("Getting source in browser2dframe.")
    html_source = browser.source()

    end_time1 = time() - start1
    profiler.append("html_source = browser.page_source: " + str(end_time1))

    start2 = time()
    logger.debug("Parsing source in browser2dframe.")
    soup = BeautifulSoup(html_source, "html5lib")  # Parser important.
    end_time2 = time() - start2
    profiler.append("BeautifulSoup(html_source, ...): " + str(end_time2))

    start3 = time()
    table = soup.find('table', attribute)
    try:
        header = [th.text for th in table.find('thead').select('th')]
    except AttributeError:
        logger.critical("Can't find the web table!")
        clean_up(browser)
    # header[:1] = ['',' ']  # Not needed here.
    body = [[td.text for td in row.select('td')]
            for row in table.findAll('tr')]
    body2 = [x for x in body if x != []]  # Must remove empty rows.
    cols = zip(*body2)  # Turn it into tuples.
    tbl_d = {name: col for name, col in zip(header, cols)}
    end_time3 = time() - start3
    profiler.append("Body of function: " + str(end_time3))

    start4 = time()
    logger.debug("Creating Dataframe in browser2dframe.")
    result = pd.DataFrame(tbl_d, columns=header)
    end_time4 = time() - start4
    profiler.append("pd.DataFrame(tbl_d, columns=header): " + str(end_time4))
    total_time = time() - start1
    if total_time > 3:
        logger.error("Page source time exceeded!")
        logger.error(profiler[0])
        logger.error(profiler[1])
        logger.error(profiler[2])
        logger.error(profiler[3])
        logger.error("Code a refresh of the window and timeout here!")
    return result


def fill_from_web(browser, attribute):
    """
    Loads the table of interest into a pandas Dataframe for easy lookup
    by row and column.
    """
    logger.debug("Calling browser2dframe in fill_from_web.")
    table_df = browser2dframe(browser, attribute)
    logger.debug("Setting index in fill_from_web.")
    table_df = table_df.set_index(row_title_column)
    logger.debug("Iterating bootstrap_list in fill_from_web.")
    list_of_rows = []
    for entry in bootstrap_list:
        # logger.debug("tablename: %s", entry[0])
        col_list = []
        for column in entry[1]:
            # print (column[0], column[1], column[2])
            # look up in table here.
            table_value = table_df.loc[column[1], column[2]]
            # logger.debug(table_value)
            if column[0] == time_col:
                table_value = custom_date_parser(table_value, browser)
            else:
                table_value = float(table_value)
            col = [column[0], table_value]
            col_list.append(col)
        # print("col_list: ", col_list)
        row = [entry[0], col_list]
        logger.debug("Loaded row from web: %s", str(row))
        list_of_rows.append(row)
    return list_of_rows


def custom_date_parser(date_string, browser):
    """
    Date parser for the oddball date format. Also atempts to handle
    the difference between the page date time and the system datetime.
    This is especially an issue around midnight when the two times might
    be in different days.
    """
    good_time = ':' in date_string
    if good_time is False:
        return None
    good_len = (len(date_string) == 7) or (len(date_string) == 8)
    if good_len is False:
        logger.critical("Unrecognized web source date format.%s", date_string)
        clean_up()
    if (len(date_string) == 7):
        date_string = '0' + date_string
    if ((web_tz == 'GMT') or (web_tz == 'UTC')):
        # Fancy stuff for when the web and utc date are not synced at midnight.
        current_utc = datetime.datetime.utcnow()
        web_hour = int(date_string[0:2])
        if current_utc.hour == 0:
            if web_hour == 23:
                one_day = datetime.timedelta(days=1)
                current_utc = current_utc - one_day
        return parse(date_string, default=(current_utc))
    else:
        logger.critical("Non GMT web dates not yet supported.")
        clean_up(browser)


def compare_lists(old_list, new_list):
    """
    Compare list_of_rows data structure row by row to determine what has
    changed and must be written to the database.
    """
    logger.debug("Comparing lists.")
    differences = []
    for entry in new_list:
        if entry not in old_list:
            differences.append(entry)
    return differences


def write2db(changed_list):
    """
    Writes rows to the database. Only does an update if the datetime
    is not None.

    I'm using pymysql as my underlying DBAPI and there is a bug that
    allows a hang if the session is interupted.
    The last line of exception is in python3.3/socket.py
    "return self._sock.recv_into(b)"
    The bug is in 0.6.1
    ref: https://github.com/PyMySQL/PyMySQL/issues/136
    pip install --upgrade https://github.com/PyMySQL/PyMySQL/tarball/master
    Hopefully this will not be needed after 0.6.1

    TODO:
    Using connectionless execution. Fix this.
    Make the update happen en masse rather than one
    table at a time. This could be much faster.
    Not sure if possible when updates are in diferent tables.

    Put some error handling when you get back some errors.
    """
    global total_rows_scraped  # The only global keyword in this module.
    global last_write_time  # Well, that makes two.
    for entry in changed_list:
        null_date = (entry[1][0][1] is None)
        if null_date:
            pass
        else:
            logger.debug("Writing row to the db: %s", str(entry))
            total_rows_scraped += 1
            current_table = Table(entry[0], metadata)
            inserter = current_table.insert()
            insert_dict = dict(entry[1])

            inserter.execute(insert_dict)
            last_write_time = time()
            logger.debug("Finished db insert.")

    return


############ Shut down ########################################################
def clean_up(browser):
    """
    Closes any webdriver instances and ends program.
    """
    global metadata
    logger.info("Closing webdriver.")
    try:
        browser.quit()
    except:
        logger.error("Browser process won't terminate.")
    logger.info("Exiting program.")
    conn.close()  # Close connection.
    engine.dispose()  # Actively close out connections.
    metadata = None

    sys.exit()


######### Main Function #######################################################
engine, metadata, conn = db_setup()


def main():
    """
    TODO:
    Not happy with the try...except capture of ^C as method to end while
    loop.
    However, being as I have searched for a way to do it a number of times
    and I have always come up unsatisfied, I am giving up for now.
    Note that this is going to cause some zombie browser processes to hang
    around after ^C, if the ^C is not caught by the right exception handler.
    In the future, look into UIs like pygame or Tkinter for this
    function or get curses working.
    Or, look into one of the solutions that uses threads. Though, if I use
    threads here, I cannot use them for doing timeouts on page loads because
    the signals might get crossed.
    """
    global last_write_time  # need to keep it global so I can reach it.

    logger.info("Launching CFDscraper. Jonathan Morris Copyright 2013")
    setup_tables(bootstrap_list, metadata)
    browser = Browser(browser_choice)
    module_start_time = time()
    last_write_time = time()

    old_list = fill_from_db(bootstrap_list, conn)
    logger.info("Starting scraping loop...")

    try:
        while True:
            cycle_start = time()
            new_list = fill_from_web(browser, attribute)
            # print('Fill from web:', time()-cycle_start)
            # print(new_list)
            changed_list = compare_lists(old_list, new_list)
            write2db(changed_list)
            old_list = new_list

            if browser.age() > browser_lifetime:
                logger.info("\nBrowser lifetime exceeded. Refreshing.")
                browser.refresh()

            cycle_length = time() - cycle_start
            sleep_time = refresh_rate - cycle_length
            if sleep_time < 0:
                sleep_time = 0
            # Write some stuff to stdout so I know it is alive.
            uptime = int(time() - module_start_time)
            since_write = int(time() - last_write_time)
            sys.stdout.write("\rRows Scraped: %d" % (total_rows_scraped))
            sys.stdout.write(", Uptime: %ss" % str(uptime))
            sys.stdout.write(", Since last write: %ss" % str(since_write))
            sys.stdout.write(", Sleeping: %.2fs" % sleep_time)
            sys.stdout.flush()

            sleep(sleep_time)
            logger.debug(" ")  # So that output is readable during debug.
    except KeyboardInterrupt:
        logger.info("^C from main loop.")
        clean_up(browser)
    except Exception:
        logger.debug("Error in main loop.")
        clean_up(browser)

if __name__ == "__main__":
    #  if (sys.platform == 'darwin')
    # main(sys.argv)
    main()
    sys.exit()
