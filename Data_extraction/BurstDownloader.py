"""
AUTHORS: Carlos Yanguas, Mario Fernandez
GITHUB: https://github.com/c-yanguas
"""

# UTILS
import time
import pandas as pd
from datetime import datetime, timedelta
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

#Print progress bar
from tqdm import tqdm




#-----------------------------FUNCTIONS TO EXTRACT SOLAR BURST REPORT INFORMATION-----------------------------
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
            # 2022 from /html/body/table/tbody/tr[4]/td[2]/a to /html/body/table/tbody/tr[6]/td[2]/a
            current_month = datetime.now().month
            for file in range(4, 4 + current_month):
                driver.get(url + '/' + str(year) + '/')
                WebDriverWait(driver, 5) \
                    .until(EC.element_to_be_clickable((By.XPATH,
                                                       '/html/body/table/tbody/tr['+str(file)+']/td[2]/a'))).click()
                text = driver.find_element_by_xpath('/html/body/pre')
                file_names = get_file_names(text.text, files_data)

    df = pd.DataFrame(files_data)
    df = df.transpose()
    df.rename(columns={0: 'stations', 1: 'date', 2: 'start', 3: 'end', 4: 'type_sb'}, inplace=True)
    if not os.path.isdir(global_path): os.makedirs(global_path)
    df.to_excel(global_path + 'solar_burst_data.xlsx')


#-----------------------------FUNCTIONS TO DOWNLOAD SOLAR BURST-----------------------------
def to_date_time(start_burst, file, end_burst):
    """
    file: STATION_DATE_TIME_FOCUSCODE.fit.gz
    start_burst, end_burst: HH:MM
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
        file_date_time    = datetime(file_year, file_month, file_day, file_hour, file_mins, file_sec)
        burst_date_time_s = datetime(file_year, file_month, file_day, burst_hour_s, burst_mins_s, file_sec)
        burst_date_time_e = datetime(file_year, file_month, file_day, burst_hour_e, burst_mins_e, file_sec)
        return file_date_time, burst_date_time_s, burst_date_time_e

    except:
        pass


def is_file_in_range(start_burst, file, end_burst, download_all, global_path):
    """
    1-Given the time of end and start of solar burst and the name of the file, returns true if that file
    contains the solar burst

    2-Since when a solar burst is reported to have a duration greater than 15 min in most of cases there are times that
    there isn't any solar burst, files without clear solar burst are downloaded. This function ensueres that the files
    downloaded contains solar bursts at the cost of ignoring reported solar bursts of more than 15 minutes.
    """
    try:
        # DATETIMES CREATION
        file_date_time, burst_date_time_s, burst_date_time_e = to_date_time(start_burst, file, end_burst)

        # MINIMUM DATETIME TO GET THE SOLAR BURST
        min_date_time   = burst_date_time_s - timedelta(minutes=14)
        max_date_time   = burst_date_time_e - timedelta(minutes=1)

        # See if file is in range
        in_range = min_date_time <= file_date_time <= max_date_time
        if download_all == 0: # IF ONLY DOWNLOAD BURSTS <= 15 MIN
            in_range = in_range and (burst_date_time_e - burst_date_time_s  <= timedelta(minutes=15))
        return in_range

    except:
        with open(global_path + 'ERROR_download_solar_bursts.txt', 'a+') as error_log:
            error_log.write(file + '\t' + start_burst + '\t' + end_burst + '\n')
        return False


def get_indexes(file, start_burst, end_burst, num_splits):
    """
    1-Given the time of end and start of solar burst, the name of the file and the num of splits of the original img file
    return the indexes where the burst exists
    i.e, if we divide a img in 3 maybe only exists the solar burst in the first one divided.
    """
    # DATETIMES CREATION
    file_date_time, burst_date_time_s, burst_date_time_e = to_date_time(start_burst, file, end_burst)

    first_index = int((burst_date_time_s - file_date_time).seconds/60) if burst_date_time_s > file_date_time else\
                  0
    last_index = int((burst_date_time_e - file_date_time).seconds / 60 + first_index) if first_index == 0 else \
                 int((burst_date_time_e - burst_date_time_s).seconds / 60 + first_index)


    # HERE WE GET INDEXES LIKE 1,2,3,4,5
    indexes = np.arange(first_index, last_index + 1) if last_index < 15 else np.arange(first_index, 15)
    # HERE WE GROUP THEN, i.e, if num_splits = 3. 1/3 = 0, 2/3=0, 3/3=1, 4/3=1, 5/3=1 and then we eliminate duplicates with unique, so indexes --> [0,1]
    indexes = np.unique(np.array([int(index / (15/num_splits)) for index in indexes]))
    return indexes


def update_sb_database(data_burst_stations, data_burst_dates, data_burst_starts, data_burst_ends,
                       unique_dates, global_path, url, download_all, thread_id):
    """
    The aim of this function is to update the solar_burst_filenames.xlsx file in order to be able to download new data without
    solar bursts
    """
    files = []
    for date in tqdm(unique_dates, desc='THREAD ' + str(thread_id)):
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
                tmp_files       = [node.get('href') for node in soup.find_all('a')
                                   if  node.get('href').startswith(file_name_start)
                                   and node.get('href').endswith('.fit.gz')
                                   and is_file_in_range(start_burst, node.get('href'), end_burst, download_all, global_path)
                                  ]
                files = files + tmp_files
    pd.DataFrame(files).to_excel(global_path + 'database_' + str(thread_id) + '.xlsx')


def join_databases(global_path):
    tmp_databases = [pd.read_excel(global_path + file, index_col=[0]) for file in os.listdir(global_path) if file.startswith('database_')]
    df            = pd.concat(tmp_databases)
    df            = df.rename(columns={0: 'solar_bursts_file_names'})
    df.to_excel(global_path + 'solar_burst_file_names.xlsx')
    for file in os.listdir(global_path):
        if file.startswith('database_'):
            os.remove(global_path + file)




def download_solar_burst_concurrence(data_burst_stations, data_burst_dates, data_burst_starts, data_burst_ends,
                                     data_burst_types, unique_dates, global_path, url, extension, current_files, download_all,
                                     num_splits, thread_id):
    for date in tqdm(unique_dates, desc='THREAD ' + str(thread_id)):
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
                        gz_to_png(outfile, start_burst, end_burst, num_splits,file[len(url_day):])





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


def gz_to_png(file_name, start_burst, end_burst, num_splits, file):
    with gzip.open(file_name + '.fit.gz', 'rb') as fin:
        with fits.open(fin) as fitfile:
            try:
                if file_name == '../../Probador/Solar_bursts_files/pngs_15min_15/HUMAIN_20201121_104500_59_III':
                    print('DEBUG')
                # READ AND GET FILE INFORMATION
                img   = fitfile['PRIMARY'].data.astype(np.uint8)
                img   = img[:-10]  # REMOVE WHITE MARKS AT THE BOTTOM OF THE IMG
                freqs = fitfile[1].data['Frequency'][0]
                times = fitfile[1].data['Time'][0]
                # img   = (img - img.mean(axis=1, keepdims=True)) / img.std(axis=1, keepdims=True)
                fitfile.close()

                if num_splits !=0:
                #IF SPLIT IMGS
                    # GET IMGS TO PLOT
                    indexes_to_plot = get_indexes(file, start_burst, end_burst, num_splits)
                    split_img       = np.array(np.hsplit(img, num_splits))
                    imgs_to_plot    = split_img[indexes_to_plot]

                    # CREATE PNG WITHOUT PLOTING ON WINDOW
                    for i, img in enumerate(imgs_to_plot):
                        plt.ioff()  # Avoid plotting on window, so we save resources
                        plt.axis('off')
                        plt.imshow(img, aspect='auto', extent=(times[0], times[-1], freqs[-1], freqs[0]), cmap=cm.CMRmap,  vmin=0, vmax=12)
                        plt.savefig(file_name + '-' + str(indexes_to_plot[i] + 1) + '.png', bbox_inches='tight', pad_inches=0.0)
                        plt.close()

                else:
                #IF FULL IMG
                    plt.ioff()  # Avoid plotting on window, so we save resources
                    plt.axis('off')
                    plt.imshow(img, aspect='auto', extent=(times[0], times[-1], freqs[-1], freqs[0]), cmap=cm.CMRmap, vmin=0, vmax=12)
                    plt.savefig(file_name + '.png', bbox_inches='tight', pad_inches=0.0)
                    plt.close()

                os.remove(file_name + '.fit.gz')


            except:
                print('Error downloading ', file_name)
                os.remove(file_name + '.fit.gz')

