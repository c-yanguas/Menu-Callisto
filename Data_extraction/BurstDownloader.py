"""
AUTHOR: Carlos Yanguas
GITHUB: https://github.com/c-yanguas
"""

# UTILS
import time
import pandas as pd
import datetime
# SELENIUM
from selenium import webdriver
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By

# REQUESTS AND FILE MANAGEMENT
import requests
import os
import urllib.request
from bs4 import BeautifulSoup
import gzip
from astropy.io import fits
import numpy as np
import shutil

# SOLVE THE "Fail to create pixmap with Tk_GetPixmap in TkImgPhotoInstanceSetSize" ERROR with Agg backend
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
from matplotlib import cm









def get_file_names(raw_text, files_data):
    """
    Given a row data burst like
    ['20210301', '01:08-01:08', 'III', 'ALASKA-HAARP']
    it returns the name of files as:
    ALASKA-HAARP_20210301_010000_01.fit.gz
    """
    raw_text        = raw_text.splitlines()
    rows_data_burst = [row.split() for row in raw_text[8:]]
    for row_data_burst in rows_data_burst:
        try:
            int(row_data_burst[1][0])  # Just to see if it is a valid row
            station_names = row_data_burst[3:]
            if len(row_data_burst[1]) == 11:  # if HH:MM-HH:MM format, 20210521	00:11-0:13	III	ALASKA-COHOE, Australia-ASSA, BAD FORMED, POSTERIOR ERROR WHILE DOWNLOADING DATA
                burst_start   = row_data_burst[1][:2] + row_data_burst[1][3:5]
                burst_end     = row_data_burst[1][6:8] + row_data_burst[1][9:]
                # minutes       = obtain_minutes_format(row_data_burst[1])
                for station in station_names:
                    # CLEAN STATION NAME
                    station_clear = station.replace(',', '')
                    station_clear = station_clear.replace('(', '')
                    station_clear = station_clear.replace(')', '')
                    station_clear = station_clear.replace('[', '')
                    station_clear = station_clear.replace(']', '')
                    station_clear = station_clear.replace('/', '')
                    # Clean BURST TYPE ['III', 'V', 'VI', 'II', 'IV', 'CTM', 'U', 'VI/V', 'I', 'V?','III?', 'III/U', 'IIIr', '???', 'I?', 'CAU', 'U?', 'IV?', 'DCIM']
                    # Just in order to save it, we need to avoid '/', then you are free to clean it in your way
                    type_burst_cl = row_data_burst[2].replace('/', '-')
                    type_burst_cl = type_burst_cl.replace('?', 'X')

                    # DATA TO SAVE
                    if station_clear != '':
                        files_data[0].append(station_clear)        # STATION NAME
                        files_data[1].append(row_data_burst[0])    # FILE DATE
                        files_data[2].append(burst_start)          # MINUTES START
                        files_data[3].append(burst_end)            # MINUTES END
                        files_data[4].append(type_burst_cl)        # BURST TYPE
        except:
            pass

    return files_data



def get_file_burst_data(global_path):
    """
    Solar burst data from 2020 ahead will be downloaded
    2020 from /html/body/table/tbody/tr[6]/td[2]/a to /html/body/table/tbody/tr[17]/td[2]/a
    2021 from /html/body/table/tbody/tr[4]/td[2]/a to /html/body/table/tbody/tr[15]/td[2]/a
    2022 from /html/body/table/tbody/tr[4]/td[2]/a to /html/body/table/tbody/tr[5]/td[2]/a
    """
    url   = 'http://soleil.i4ds.ch/solarradio/data/BurstLists/2010-yyyy_Monstein/'
    years = [2020, 2021, 2022]
    options = webdriver.ChromeOptions()
    options.add_argument('--start-maximized')
    options.add_argument('--disable-extensions')
    driver = webdriver.Chrome(ChromeDriverManager().install(), options=options)
    driver.set_window_position(2000, 0)
    driver.maximize_window()
    time.sleep(1)
    files_data = [[], [], [], [], []]  # INSTRUMENT_NAME, YYYYMMDD, HHMM SOLAR BURST START, HHMM SOLAR BURST END, TYPE SOLAR BURST
    for year in years:
        if year == 2020:
            # 2020 from /html/body/table/tbody/tr[6]/td[2]/a to /html/body/table/tbody/tr[17]/td[2]/a
            for file in range(6,18):
                driver.get(url + '/' + str(year) + '/')
                WebDriverWait(driver, 5).until(EC.element_to_be_clickable((By.XPATH, '/html/body/table/tbody/tr['+str(file)+']/td[2]/a'))).click()
                text       = driver.find_element_by_xpath('/html/body/pre').text
                file_names = get_file_names(text, files_data)

        if year == 2021:
            # 2021 from /html/body/table/tbody/tr[4]/td[2]/a to /html/body/table/tbody/tr[15]/td[2]/a
            for file in range(4,15):
                driver.get(url + '/' + str(year) + '/')
                WebDriverWait(driver, 5) \
                    .until(EC.element_to_be_clickable((By.XPATH,
                                                       '/html/body/table/tbody/tr['+str(file)+']/td[2]/a'))).click()
                text = driver.find_element_by_xpath('/html/body/pre')
                file_names = get_file_names(text.text, files_data)
        if year == 2022:
            # 2022 from /html/body/table/tbody/tr[4]/td[2]/a to /html/body/table/tbody/tr[5]/td[2]/a
            for file in range(4, 6):
                driver.get(url + '/' + str(year) + '/')
                WebDriverWait(driver, 5) \
                    .until(EC.element_to_be_clickable((By.XPATH,
                                                       '/html/body/table/tbody/tr['+str(file)+']/td[2]/a'))).click()
                text = driver.find_element_by_xpath('/html/body/pre')
                file_names = get_file_names(text.text, files_data)

    df = pd.DataFrame(files_data)
    df = df.transpose()
    df.rename(columns={0: 'stations', 1: 'date', 2: 'start', 3: 'end', 4: 'type_sb'}, inplace=True)
    df.to_excel(global_path + 'solar_burst_data.xlsx')



def is_file_in_range(start_burst, file, end_burst, download_all, global_path):
    """
    1-Given the time of end and start of solar burst and the name of the file, returns true if that file
    contains the solar burst

    2-Since when a solar burst is reported to have a duration greater than 15 min in most of cases there are times that
    there isn't any solar burst, files without clear solar burst are downloaded. This function ensueres that the files
    downloaded contains solar bursts at the cost of ignoring reported solar bursts of more than 15 minutes.
    """
    try:
        data = file.split('_')
        if data[0] == 'Malaysia' and data[1] == 'Banting':  # Malaysia_Banting name of the station... error handler
            date_pos = 2
            time_pos = 3
        else:
            date_pos = 1
            time_pos = 2
        # FILE DATE
        file_year  = int(data[date_pos][:4])
        file_month = int(data[date_pos][4:6])
        file_day   = int(data[date_pos][6:])
        # FILE TIME
        file_hour  = int(data[time_pos][:2])
        file_mins  = int(data[time_pos][2:4])
        file_sec   = int(data[time_pos][4:])
        # BURST TIME START
        burst_hour_s = int(start_burst[:2])
        burst_mins_s = int(start_burst[2:])
        # BURST TIME END
        burst_hour_e = int(end_burst[:2])
        burst_mins_e = int(end_burst[2:])

        # DATETIMES CREATION
        file_date_time    = datetime.datetime(file_year, file_month, file_day, file_hour, file_mins, file_sec)
        burst_date_time_s = datetime.datetime(file_year, file_month, file_day, burst_hour_s, burst_mins_s, file_sec)
        burst_date_time_e = datetime.datetime(file_year, file_month, file_day, burst_hour_e, burst_mins_e, file_sec)

        # MINIMUM DATETIME TO GET THE SOLAR BURST
        min_date_time   = burst_date_time_s - datetime.timedelta(minutes=14)
        max_date_time   = burst_date_time_e - datetime.timedelta(minutes=1)

        #See if file is in range
        in_range = min_date_time <= file_date_time <= max_date_time
        if download_all == 0: # IF ONLY DOWNLOAD BURSTS <= 15 MIN
            in_range = in_range and (burst_date_time_e - burst_date_time_s  <= datetime.timedelta(minutes=15))
        return in_range

    except:
        with open(global_path + 'ERROR_download_solar_bursts.txt', 'a+') as error_log:
            error_log.write(file + '\t' + start_burst + '\t' + end_burst + '\n')
        return False


def duration_is_15():
    """
    Since when a solar burst is reported to have a duration greater than 15 min in most of cases there are times that
    there isn't any solar burst, files without clear solar burst are downloaded. This function ensueres that the files
    downloaded contains solar bursts at the cost of ignoring reported solar bursts of more than 15 minutes.
    """



def download_solar_burst_concurrence(data_burst_stations, data_burst_dates, data_burst_starts, data_burst_ends,
                                     data_burst_types, unique_dates, global_path, url, extension, current_files, download_all):

    for date in unique_dates:
        # WE MAKE ONLY ONE REQUEST PER DAY
        indexes   = np.where(data_burst_dates == date)[0]
        url_day   = url + date[:4] + '/' + date[4:6] + '/' + date[6:8] + '/'
        page      = requests.get(url_day)
        soup      = BeautifulSoup(page.content, 'html.parser')
        if '404 Not Found' not in soup:
            for index in indexes:
                # INDEXES = ALL ROWS OF SOLAR BURSTS FOR SPECIFIC DAY
                start_burst     = data_burst_starts[index]
                end_burst       = data_burst_ends[index]
                file_name_start = data_burst_stations[index] + '_' + date + '_'
                files           = [url_day + node.get('href') for node in soup.find_all('a')
                                   if  node.get('href').startswith(file_name_start)
                                   and node.get('href').endswith('.fit.gz')
                                   and node.get('href') not in current_files
                                   and is_file_in_range(start_burst, node.get('href'), end_burst, download_all, global_path)
                                  ]
                for file in files:
                    fname_url  = file                                                 # name of the file in web
                    aux        = file.split('/')[-1]
                    fname_disk = aux[:len(aux) - 7] + '_' + data_burst_types[index]   # name of the file in disk
                    urlb       = urllib.request.urlopen(fname_url)
                    outfile    = global_path + fname_disk
                    with open(outfile + '.fit.gz', 'wb') as fout:
                        fout.write(urlb.read())
                    urlb.close()
                    if extension == '.fit':
                        gz_to_fit(outfile)
                    elif extension == '.npy':
                        gz_to_npy(outfile)
                    elif extension == '.png':
                        gz_to_png(outfile)
            print(date, 'Files downloaded')





def gz_to_npy(file_name):
    with gzip.open(file_name + '.fit.gz', 'rb') as fin:
        with fits.open(fin) as fitfile:
            img = fitfile['PRIMARY'].data.astype(np.float32)
            with open(file_name + '.npy', 'wb') as f:
                np.save(f, img)
    os.remove(file_name + '.fit.gz')


def gz_to_fit(file_name):
    with gzip.open(file_name + '.fit.gz', 'rb') as fin:
        with open(file_name[:len(file_name)-7] + '.fit', 'wb') as fout:
            shutil.copyfileobj(fin, fout)
    os.remove(file_name + '.fit.gz')


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