#! /usr/bin/env python3
# -*- coding: utf-8
"""
A module to scrape financial data from web tables and write to MySQL.

Usage: python CFDscraper.py ./config1.cfg
First and only arg is optional path to a config file.

One of the items is a list of lists with table info in it that seems like
a headache to parse with configparser so this module simply exec()s a text
file excerpted from the config section below. (Yes, yes, I know using exec()
like this is frowned upon.)

TODO:
Issue #1:
Find a way to detect when the phantomjs driver becomes
inactive. For some reason, the page at investing.com stops updating. On
inspecting, I can see that networks GETs are still occrring. How then to detect
this? How about running two different instances of the page at a time and
comparing them? When there is a mismatch, do a reload on one or both.
Make this a switch in the config. You will have to rewrite things to be a
little more OO to make it work. Might actually be fun to implement.
Make it so that any number of browsers can be called for purposes of
redundancy.

You might think about putting a lock on the database when you check it and
then remove the lock when you are done. This way, you could have mulitple
instances of each program running concurrently. They could even be running on
different computers in different locations.


Issue #2:
Move away from SQLalchemy connectionless execution and add
rollback. In order to implement this properly I'll need to have something in
memory to fill and empty in the order it was filled. I've already implemented
this in the dbbuffer class I wrote. Move it over if needed.

Issue #3:
Move logging options to a command line switch.
Make sql server stop updating and stopping sql server every week.
Make scraper deal gracfully with sql server going away.
(Wait loop with stored data. Write rows to a flat file perhaps.)
Go through every sys.exit() below and make it enter a wait loop.

Issue #4:
Test scraper for recovery with a restart of the SQL database.

Issue #5:
After "loading webpage" I need to check for the page actually being loaded.
I have had a couple errors where I got a "Couldn't close popup" and the
screenshot was just blank. There is no way I should be getting that far. Some
element needs to be checked for. The title?

Issue #6:
Now spawning zombie or orphan processes.
/usr/sbin/mysqld
Nope, this is normal behavior for mysql. It does this to improve performance.

Also phantomjs
This does not look normal.
Perhaps this with 1.9.1:
https://github.com/Obvious/phantomjs/issues/71
I'm on 1.9.2 right now on my mac and 1.9.0 on linux.
current is 1.9.7

I uninstalled with apt and put the 1.9.7 executable into /usr/bin
This executes just fine but I'm still getting 17 processes.
I wonder if instead of launching phantomjs multiple times, I'm supposed to
launch multiple windows?

This may have nothing to do with phantomjs. It may be selenium or ghostdriver
that is messing up.
Upgraded selenium as well to 2.39 and no luck.
This is a problem. I've reached my memory,(but not cpu) limit for running
CFDscraper at seven instances. That's probably 49 instances of phantomjs at
133M each. (6.5GB) At this rate, going back to chrome would be much better.

Next try calling the browser manually while cloesly watching top to see
where things go south. Then go deeper and see if the same behavior arises in
pure phantomjs. Not sure how to do this. Do I need to do it in a js
command line?

Next try running the same with chrome. Seven chrome browsers isn't too bad.
You will need to make sure chromedriver is installed in the right place.
Also move chromedriver to the correct place for osx so you don't have to
specify in the configs.

Ultimately, set something to do unix ps and count zombies. I can't have this
happen again.


Other Issues:
Look for chrome where it belongs for each OS.
Check for function on linux and windows.
If the database is not available, write rows to an object that can be "emptied"
later.
Include hours of operation in the config file and don't scrape at these times.
Make a way to scrape a page with just one data point.
Turn the database writer into a class in order to do away with pesky globals.
Move classes into a seperate file.
"""

import sys
from time import sleep, time
import datetime
##### For scraping ######
from selenium import webdriver
from selenium.common.exceptions import NoSuchWindowException
from selenium.webdriver.common.desired_capabilities import DesiredCapabilities
from bs4 import BeautifulSoup
from sqlalchemy import (create_engine, MetaData, Table, Column,
                        Integer, DateTime, Float)
from dateutil.parser import parse
import pandas as pd
##### Logging ############s
import logging
import logging.handlers
import uuid  # For creating unique name for screenshots.
###### For timeout ########
from functools import wraps
import errno
import os
import signal


###### Some globals #########################################################
# Put these into a class at some point.

total_rows_scraped = 0  # Don't change this. It's just a counter.
last_write_time = time()  # Also a counter.

##############################################################################
###### Default Configuration Data ############################################
###### Copy this section to a config file and load it with a CL argument #####
dataname = 'bondCFD'
logpath = dataname + '_scrape.log'

chromepath = '/Users/jonathanamorris/Code/chromedriver'
browser_choice = "phantomjs"  # Choose chrome, firefox, or phantomjs
phantom_log_path = dataname + '_phantomjs.log'
# Database info:
db_host = 'dataserve.local'
db_user = 'jonathan'
db_pass = ''
db_name = 'mydb'
db_dialect = 'mysql+pymysql'
# Page info:
page_source_timeout = 5  # In seconds. Must be an integer.
browser_lifetime = 1680  # In seconds. 14400 is four hours.
base_url = 'http://www.investing.com'
url_string = base_url + '/rates-bonds/government-bond-spreads'
web_tz = 'GMT'

# Table info:
attribute = {'id': 'bonds'}
time_col = "UTCTime"
row_title_column = 'Country'  # Need this to know index column.
refresh_rate = 10.5  # Minimum number of seconds between scrapes.

# Table form:
# bootstrap = (db_table_name,
#            ((db_column1_name, web_row_string, web_col_string),
#             (db_column2_name, web_row_string, web_col_string)))
# Timestamp column name is special and will be made primary key
# All others default to float.
# The timestamp column, whatever its dytpe, must be the first for
# everything to work.
# It can be just one big list of lists. I just thought the format below
# would be more readable and less prone to making typos.
###### Tables list #########

bootstrap_list = []

bootstrap1 = ("German10yrbond",
             (("UTCTime", "Germany", "Time"),
              ("Value", "Germany", "Yield")))

bootstrap_list.append(bootstrap1)

bootstrap_list.sort()
###############################################################################
###############################################################################


def import_config():
    """
    """
    if len(sys.argv) > 1:
        filename = sys.argv[1]
        print("loading config file:" + sys.argv[1])
    else:
        filename = './CFDscraper.cfg'
    exec(compile(open(filename, "rb").read(), filename, 'exec'),
         globals(),
         globals())  # Force import to global namespace.

import_config()  # This needs to happen before the logger gets set up.


######## Set up logging  ######################################################
logger = logging.getLogger('CFDscraper')  # Or __name__
logger.setLevel(logging.DEBUG)
# Create file handler which logs even debug messages.
file_hand = logging.handlers.RotatingFileHandler(logpath,
                                                 maxBytes=10000,
                                                 backupCount=2)
file_hand.setLevel(logging.ERROR)  # Set logging level here.
# Create console handler with a higher log level.
console_hand = logging.StreamHandler()
console_hand.setLevel(logging.ERROR)  # Set logging level here. Normally INFO
# Create formatter and add it to the handlers.
form_string = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
formatter = logging.Formatter(form_string)
formatter2 = logging.Formatter('%(message)s')
console_hand.setFormatter(formatter2)
file_hand.setFormatter(formatter)
# Add the handlers to logger.
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


###############################################################################
######## Open database and check that it can be reached #######################
def db_setup():
    """
    Connects to the database using SQLalchemy-core. This is the only
    function that is called outside of main().
    """
    # print("Enter password for " + db_user + "@" + db_host + ":")

    logger.info('Connecting to database.')

    connect_string = (db_dialect + '://' +
                      db_user + ':' +
                      db_pass + '@' +
                      db_host + '/' +
                      db_name)

    try:
        engine = create_engine(connect_string,
                               echo=False,
                               pool_recycle=3600)
        metadata = MetaData(bind=engine)
        conn = engine.connect()

    except:
        logger.error('ERROR: Database not reachable. Exiting', exc_info=1)
        sys.exit()

    return engine, metadata, conn


########## Webdrivers class ###################################################
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
        Opens a Chrome webdriver instance.

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
        except:
            logger.error("Can't open webdriver.", exc_info=1)

        attempts = 0
        while attempts < 10:
            try:
                logger.info("Loading webpage: " + url_string)
                driver.get(url_string)
                break
            except:
                attempts += 1
                logger.error("Page load failed. Retrying.")
                sleep(2)
                # The TBs generated here are of little use.
                # All the good stuff is inside phantomjs.
                # logger.critical("Can't load webpage.", exc_info=1)
                # clean_up(self)
        if attempts == 10:
            logger.critical("Page load re-try limit exceeded.")
            clean_up(self)
        try:
            # browser.find_element_by_class_name("popupAdCloseIcon").click()
            driver.find_element_by_partial_link_text("Continue").click()
        except:
            logger.error("ERROR: Can't close the popup.")
            pass
        return driver

    def new_firefox_driver(self):
        """
        Opens a Firefox browser and closes the popup.
        I switched to Firefox from Chrome because for some reason lxml doesn't
        work with Chrome and Python 3.3. (Because unicode from Chrome being
        ignored by lxml.)
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
            logger.critical("ERROR: Can't open browser.", exc_info=1)
            clean_up(self)

        attempts = 0
        while attempts < 10:
            try:
                logger.info("Loading webpage: " + url_string)
                driver.get(url_string)
                break
            except:
                attempts += 1
                logger.error("Page load failed. Retrying.")
                sleep(2)
                # logger.critical("Can't load webpage.", exc_info=1)
                # clean_up(self)

        try:
            driver.find_element_by_partial_link_text("Continue").click()
        except:
            logger.error("ERROR: Can't close popup.")
            pass
        return driver

    def new_phantomjs_driver(self):
        """
        Opens a ghostjs webdriver.

        For OSX:
        brew install phantomjs

        If not using brew:
            Install NodeJS
            Using Node's package manager install phantomjs:
                npm -g install phantomjs
            Install selenium (in virtualenv, if using.)

        For others:
        http://phantomjs.org/download.html
        """
        # PhantomJS args:
        # service_args : A List of command line arguments to pass to PhantomJS
        # service_log_path: Path for phantomjs service to log to.
        # Command lin:
        # github.com/ariya/phantomjs/wiki/API-Reference#command-line-options
        # PhantomJS user agent out of the box:
        # "Mozilla/5.0 (Macintosh; PPC Mac OS X) AppleWebKit/534.34
        # (KHTML, like Gecko) PhantomJS/1.9.2 Safari/534.34"
        # https://github.com/ariya/phantomjs/issues/11156
        # Set the user agent string to something less robotronic:
        user_agent = ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_1) " +
                      "AppleWebKit/534.34 (KHTML, like Gecko) " +
                      "Chrome/31.0.1650.63 Safari/534.34")
        dcap = dict(DesiredCapabilities.PHANTOMJS)
        dcap["phantomjs.page.settings.userAgent"] = user_agent
        service_args = ['--debug=false',
                        '--ignore-ssl-errors=true'
                        ]  # Set phantomjs command line options here.

        try:
            logger.info("Loading PhantomJS webdriver.")
            driver = webdriver.PhantomJS(executable_path="phantomjs",
                                         desired_capabilities=dcap,
                                         service_log_path=phantom_log_path,
                                         service_args=service_args)
        except:
            logger.critical("ERROR: Can't open browser.", exc_info=1)
            clean_up(self)

        driver.set_window_size(1024, 768)

        attempts = 0
        while attempts < 10:
            try:
                logger.info("Loading webpage: " + url_string)
                driver.get(url_string)
                break
            except:
                attempts += 1
                logger.error("Page load failed. Retrying.")
                sleep(2)
                # logger.critical("Can't load webpage.", exc_info=1)
                # clean_up(self)

        try:
            driver.find_element_by_partial_link_text("Continue").click()
        except:
            logger.error("ERROR: Can't close popup.")
            tempname = str(uuid.uuid4()) + '.png'
            driver.save_screenshot(tempname)
            logger.error("Screenshot: " + tempname)

        return driver

    def refresh(self):
        """ """
        try:
            self.driver.quit()
        except:
            logger.error("ERROR: Browser process won't die.", exc_info=1)
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
                logger.critical("2nd try on source load failed.", exc_info=1)
                clean_up(self)
        except TimeoutError:
            logger.error("Time limit exceeded for webdriver.page_source.")
            logger.error("Refreshing webdriver.")
            self.refresh()
            try:
                self.html_source = self.source_inner()
            except:
                logger.critical("2nd try on source load failed.", exc_info=1)
                clean_up(self)
        return self.html_source

    @timeout(page_source_timeout)
    def source_inner(self):
        """
        Wrapper for browser.page_source so that it can be timed out if hung.
        """
        return self.driver.page_source  # Must be unbound method.

###############################################################################


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
        logger.debug("Load db: %s", str(row))
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
    (a lot later.)
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
    if table is None:
        logger.critical("Can't find the table. Is the attribute correct?")
        clean_up(browser)
    try:
        header = [th.text for th in table.find('thead').select('th')]
    except AttributeError:
        logger.critical("Can't find the table head!")
        clean_up(browser)

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
        browser.refresh()
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
            table_value = table_df.loc[column[1], column[2]]
            # logger.debug(table_value)
            if column[0] == time_col:
                table_value = custom_date_parser(table_value, browser)
            else:
                table_value = table_value.replace(',', '')
                table_value = float(table_value)
            col = [column[0], table_value]
            col_list.append(col)

        row = [entry[0], col_list]
        logger.debug("Load web: %s", str(row))
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
        # Fancy stuff for when the web and utc date are not synced @ midnight.
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
    table at a time. This could be faster.
    Not sure if possible when updates are in diferent tables.

    Put some error handling when you get back some errors.
    """
    global total_rows_scraped
    global last_write_time
    for entry in changed_list:
        null_date = (entry[1][0][1] is None)
        if null_date:
            pass
        else:
            logger.debug("Write db: %s", str(entry))
            total_rows_scraped += 1

            current_table = Table(entry[0], metadata)
            inserter = current_table.insert()
            insert_dict = dict(entry[1])  # keep this.
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
    logger.critical("Closing webdriver.")

    try:
        browser.quit()

    except:
        logger.critical("Browser process won't terminate.")
    logger.critical("Exiting program.")
    conn.close()  # Close connection.
    engine.dispose()  # Actively close out connections.
    metadata = None

    sys.exit()

#  Now: move db set up stuff inside of main() or at least inside of a function.
######### Main Function #######################################################


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
    logger.info("CFDscraper by Jonathan Morris Copyright 2014")
    global metadata
    global engine
    global conn
    engine, metadata, conn = db_setup()
    setup_tables(bootstrap_list, metadata)
    browser = Browser(browser_choice)
    module_start_time = time()
    last_write_time = time()
    old_list = fill_from_db(bootstrap_list, conn)
    logger.info("Starting scraping loop.")

    try:
        while True:
            cycle_start = time()
            new_list = fill_from_web(browser, attribute)
            changed_list = compare_lists(old_list, new_list)
            write2db(changed_list)
            old_list = new_list

            if browser.age() > browser_lifetime:
                logger.info("Lifetime exceeded. Refreshing.")
                browser.refresh()

            cycle_length = time() - cycle_start
            sleep_time = refresh_rate - cycle_length

            if sleep_time < 0:
                sleep_time = 0
            # Write some stuff to stdout so I know it is alive.
            uptime = int(time() - module_start_time)
            since_write = int(time() - last_write_time)
            sys.stdout.write("\rRows: %d" % (total_rows_scraped))
            sys.stdout.write(", Uptime: %ss" % str(uptime))
            sys.stdout.write(", Since write: %ss" % str(since_write))
            sys.stdout.write(", Sleeping: %.2fs" % sleep_time)
            sys.stdout.flush()
            sleep(sleep_time)

    except KeyboardInterrupt:
        logger.critical("^C from main loop.")
        clean_up(browser)

if __name__ == "__main__":
    main()
    sys.exit()
