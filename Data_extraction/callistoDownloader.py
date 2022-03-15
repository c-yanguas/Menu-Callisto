"""
AUTHORS: Vivek Reddy, Carlos Yanguas, Mario Fernandez
GITHUB: https://github.com/vvkrddy https://github.com/c-yanguas
"""

import requests
import os
import urllib.request
from bs4 import BeautifulSoup
import gzip
import shutil
from astropy.io import fits
import numpy as np
import datetime
import matplotlib.pyplot as plt
from matplotlib import cm
url = 'http://soleil80.cs.technik.fhnw.ch/solarradio/data/2002-20yy_Callisto/'

def years():
    """

    Returns those years for which any data (irrespective of instruments) is available.

    Parameters
    ----------

    Returns
    -------
    years : list
      a list of strings representing the years for which data is available

    """

    page = requests.get(url)
    soup = BeautifulSoup(page.content, 'html.parser')
    years = []
    for i in range(len(soup.find_all('a')) - 6):
        years.append(soup.find_all('a')[5 + i].get_text()[0:-1])

    return years


def months(select_year):
    """

    Returns those months for which any data (irrespective of instruments) is available, in the specified year.

    Parameters
    ----------
    select_year:
      int

    Returns
    -------
    months : list
      a list of strings representing the months for which data is available in the given year

    """

    # error handling
    if len(str(select_year)) != 4 or type(select_year) != int:
        print("The only argument year must be a 4-digit integer.")
        return -1
    # assert (len(str(select_year)) == 4 and type(
    #     select_year) == int), "The only argument year must be a 4-digit integer."
    #

    select_year_str = str(select_year)

    # data unavailability
    if select_year_str not in years():
        print("The specified year {} doesn't have any data".format(select_year))
        return -1
    #assert (select_year_str in years()), "The specified year {} doesn't have any data".format(select_year)
    #

    url_year = url + select_year_str + '/'
    page = requests.get(url_year)
    soup = BeautifulSoup(page.content, 'html.parser')
    months = []

    for i in range(len(soup.find_all('a')) - 5):
        months.append(soup.find_all('a')[5 + i].get_text()[0:-1])

    return months


def days(select_year, select_month):
    """

    Returns those days for which any data (irrespective of instruments) is available, in the specified month and year.

    Parameters
    ----------
    select_year:
      int
    select_month:
      int

    Returns
    -------
    days : list
      a list of strings representing the days for which data is available in the given month and year

    """

    # error handling
    assert (len(str(select_year)) == 4 and type(select_year) == int), "First argument year must be a 4-digit integer"
    assert (type(select_month) == int and 1 <= select_month <= 12), "Second argument month must be a valid integer"
    #assert (1 <= select_month <= 12), "Second argument month must be a valid integer"
    if datetime.date.today().year == select_year:
        assert (datetime.date.today().month >= select_month), "The month {} in {} has not yet occurred".format(
            select_month, select_year)
    assert (datetime.date.today().year >= select_year), "The year {} has not yet occurred".format(select_year)
    #

    if select_month < 10:
        select_month_str = '0' + str(select_month)
    else:
        select_month_str = str(select_month)
    select_year_str = str(select_year)

    # data unavaiability
    if select_year_str not in years():
        return -1
    if select_month_str not in months(select_year):
        return -1
    # assert (select_year_str in years()), "The specified year {} doesn't have any data".format(select_year)
    # assert (select_month_str in months(select_year)), "The specified month {} in {} doesn't have any data".format(
    #     select_month, select_year)
    #

    url_month = url + select_year_str + '/' + select_month_str + '/'
    page = requests.get(url_month)
    soup = BeautifulSoup(page.content, 'html.parser')
    days = []

    for i in range(len(soup.find_all('a')) - 5):
        days.append(soup.find_all('a')[5 + i].get_text()[0:-1])

    return days


def which_years():
    """

    Prints those years for which any data (irrespective of instruments) is available.

    Parameters
    ----------

    Returns
    -------
    None

    """
    for i in years():
        print(i, end="\t")
    print("\n")


def which_months(select_year):
    """

    Prints those months for which any data (irrespective of instruments) is available, in the specified year.

    Parameters
    ----------
    select_year:
      int

    Returns
    -------
    None

    """

    for i in months(select_year):
        print(i, end="\t")
    print("\n")


def which_days(select_year, select_month):
    """

    Prints those days for which any data (irrespective of instruments) is available, in the specified month and year.

    Parameters
    ----------
    select_year:
      int
    select_month:
      int

    Returns
    -------
    None

    """
    days_list = []
    for i in days(select_year, select_month):
        days_list.append(i)
        print(i, end='\t')
    print("\n")


def instrument_codes():
    """

    Instrument codes are codes derived specifically for this package
    and each code corresponds to one of the instrument-location combination
    from link http://soleil.i4ds.ch/solarradio/data/readme.txt.

    If the file name is BLEN5M_20090411_100001_58.fit.gz;
    then the instrument code is the characters before the first underscore,
    which in this case is BLEN5M.

    This will download all spectrograms from all antenna types at Blein, Switzerland.

    OR

    From the http://soleil80.cs.technik.fhnw.ch/solarradio/data/2002-20yy_Callisto/
    you can choose to visit the webpage of a particular day, say January 01, 2021;

    The particular webpage for the day is http://soleil80.cs.technik.fhnw.ch/solarradio/data/2002-20yy_Callisto/2021/01/01/

    Suppose, you would want to download files from ALASKA-ANCHORAGE files,
    then use the instrument code 'ALASKA-ANCHORAGE' as the fourth parameter in the download() function.


    Parameters
    ----------

    Returns
    -------
    INSTRUCTIONS

    """

    s = "http://soleil.i4ds.ch/solarradio/data/readme.txt"
    print("Visit " + s)

    print("If the file name is BLEN5M_20090411_100001_58.fit.gz; \
    \nthen the instrument code is the characters before the first underscore.\
    \nwhich in this case is BLEN5M\
    \n\nThis will download all spectrograms from all antenna types at Blein, Switzerland")


def download(select_year, select_month, select_day, instruments, extension, file_burst_names, global_path):
    """

    Downloads files for list of instruments from given list of days of a given month and year

    if select_day = 'ALL', data is downloaded for all the days of the given year and given month for the given list of instruments

    Parameters
    ----------
    select_year: int
    select_month: int
    select_day:
      int
      list of int
      'ALL'
    instruments:
      str
      list of str

    Returns
    -------
    None

    """

    # error handling
    assert (len(str(select_year)) == 4 and type(select_year) == int), "Year must be a 4-digit integer."
    assert (1 <= select_month <= 12 and type(select_month) == int), "Month must be a valid number."
    if type(instruments) != list:
        instruments = [str(instruments)]
    for instrument in instruments:
        assert (type(instrument) == str), "Instruments must be a string or list of strings"
    if select_month < 10:
        select_month_str = '0' + str(select_month)
    else:
        select_month_str = str(select_month)

    # initializing
    select_year_str = str(select_year)


    if select_day != 'ALL' and type(select_day) != list:
        select_day = [int(str(select_day))]
    else:
        select_day = days(select_year, select_month)
        if select_day == -1:
            return
        select_day = list(map(int, select_day))

    # error handling
    for d in select_day:
        assert (type(d) != str), "Days must be an integer or list of integers"

    for d in select_day:
        if d < 10:
            d_str = '0' + str(d)
        else:
            d_str = str(d)

        # error handling
        try:
            select_date = datetime.date.fromisoformat('{}-{}-{}'.format(select_year_str, select_month_str, d_str))
        except:
            print("{}-{}-{} Date is invalid".format(select_year_str, select_month_str, d_str))
            continue

        assert (select_date < datetime.date.today()),\
            "{}-{}-{} The date has not yet occurred".format(select_year_str, select_month_str, d_str)

        #
        url_day = url + select_year_str + '/' + select_month_str + '/' + d_str + '/'
        page    = requests.get(url_day)
        soup    = BeautifulSoup(page.content, 'html.parser')
        if len(file_burst_names) > 0:
            path = global_path + 'Instruments/{}_NSB/'.format(instrument) # NSB = No solar burst
        else:
            path = global_path + 'Instruments/{}_WSB/'.format(instrument) # WSB = With solar burst
        if not os.path.isdir(path):
            os.makedirs(path)
        if '404 Not Found' not in soup:
            for instrument in instruments:
                files = [url_day + node.get('href') for node in soup.find_all('a')
                         if node.get('href').startswith(instrument)
                         and node.get('href').endswith('.fit.gz')
                         and node.get('href') not in file_burst_names]
                for file in files:
                    aux        = file.split('/')[-1]
                    fname_disk = path + aux[:len(aux) - 7]
                    # if its already downloaded, we can skip this iteration
                    if extension == '.fit':
                        if os.path.exists(fname_disk + '.fit'):
                            continue
                    elif extension == '.npy':
                        if os.path.exists(fname_disk + '.npy'):
                            continue
                    elif extension == '.gz':
                        if os.path.exists(fname_disk + '.fit.gz'):
                            continue
                    elif extension == '.png':
                        if os.path.exists(fname_disk + '.png'):
                            continue
                    # if not already downloaded
                    urlb       = urllib.request.urlopen(file)
                    with open(fname_disk + '.fit.gz', 'wb') as fout:
                        fout.write(urlb.read())
                    urlb.close()
                    if   extension == '.fit':
                        gz_to_fit(fname_disk)
                    elif extension == '.npy':
                        gz_to_npy(fname_disk)
                    elif extension == '.png':
                        gz_to_png(fname_disk)
                    # elif extension == '.png':
                    #     gz_to_png(fout)
            print('{}-{}-{} {} files downloaded'.format(select_year_str, select_month_str, d_str, instrument))
        else:
            print('{}-{}-{}'.format(select_year_str, select_month_str, d_str), 'No data for the date')


def gz_to_npy(file_name):
    with gzip.open(file_name + '.fit.gz', 'rb') as fin:
        with fits.open(fin) as fitfile:
            img = fitfile['PRIMARY'].data.astype(np.float32)
            with open(file_name[:len(file_name)-7] + '.npy', 'wb') as f:
                np.save(f, img)
    os.remove(file_name + '.fit.gz')

def gz_to_fit(file_name):
    with gzip.open(file_name + '.fit.gz', 'rb') as fin:
        with open(file_name[:len(file_name)-7] + '.fit', 'wb') as fout:
            shutil.copyfileobj(fin, fout)
    os.remove(file_name + '.fit.gz')

from PIL import Image

def gz_to_png(file_name):
    with gzip.open(file_name + '.fit.gz', 'rb') as fin:
        with fits.open(fin) as fitfile:
            try:
                # READ AND GET FILE INFORMATION
                img   = fitfile['PRIMARY'].data.astype(np.uint8)
                img   = img[:-10]  # REMOVE WHITE MARKS AT THE BOTTOM OF THE IMG
                freqs = fitfile[1].data['Frequency'][0]
                times = fitfile[1].data['Time'][0]
                fitfile.close()
                # CREATE PNG WITHOUT PLOTING ON WINDOW
                plt.ioff()  # Avoid plotting on window, so we save resources
                plt.axis('off')
                plt.imshow((img - img.mean(axis=1, keepdims=True)) / img.std(axis=1, keepdims=True), aspect='auto', extent=(times[0], times[-1], freqs[-1], freqs[0]), cmap=cm.CMRmap,  vmin=0, vmax=12)
                plt.savefig(file_name + '.png', bbox_inches='tight', pad_inches=0.0)
                plt.close()
                os.remove(file_name + '.fit.gz')
            except:
                print('Error downloading ', file_name)