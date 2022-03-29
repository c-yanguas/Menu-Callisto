"""
AUTHOR: Carlos Yanguas
GITHUB: https://github.com/c-yanguas
"""

import callistoDownloader as cd
import BurstDownloader as BD
from multiprocessing import Pool
import os
from itertools import repeat
import pandas as pd
import numpy as np
import time
import datetime
import shutil

#----------------------------------------------------------STATIONS AVAILABLE----------------------------------------------------------
name_stations = [
    'ALASKA-ANCHORAGE',   'ALASKA-COHOE',        'ALASKA-HAARP',        'ALGERIA-CRAAG',      'ALMATY',
    'AUSTRIA-Krumbach',   'AUSTRIA-MICHELBACH',  'Acreibo-observatory', 'AUSTRIA-OE3FLB',     'AUSTRIA-UNIGRAZ',
    'Australia-ASSA',     'Australia-LMRO',      'DENMARK',             'GLASGOW',            'EGYPT',
    'GERMANY-DLR',        'GREENLAND',           'HUMAIN',              'HURBANOVO',          'INDIA-GAURI',
    'INDIA-IISERP',       'INDIA-Nashik',        'INDIA-OOTY',          'INDIA-UDAIPUR',      'INDONESIA',
    'JAPAN-IBARAKI',      'KASI',                'KRIM',                'MEXART',             'MONGOLIA-GOBI',
    'MONGOLIA-UB',        'MRO',                 'NEWZEALAND-AUT',      'NORWAY-NY-AALESUND', 'NORWAY-RANDABERG',
    'ROSWELL-NM',         'SOUTHAFRICA-SANSA',   'SPAIN-ALCALA',        'SPAIN-PERALEJOS',    'SPAIN-SIGUENZA',
    'SWISS-BLEN5M',       'SWISS-BLEN7M',        'SWISS-HB9SCT',        'SWISS-IRSOL',        'SWISS-Landschlacht',
    'SWISS-MUHEN',        'TRIEST',              'URUGUAY',             'USA-ARIZONA-ERAU'
]

GLOBAL_PATH = '../Data/'
DEBUG       = 0

#----------------------------------------------------------AUXILIAR FUNCTIONS----------------------------------------------------------
def threads_managements(tasks):
    threads = os.cpu_count()
    k, m    = divmod(len(tasks), threads)
    return list((tasks[i * k + min(i, m):(i + 1) * k + min(i + 1, m)] for i in range(threads)))

def tabulate(words, termwidth=120, pad=3):
    new_words = []
    for n, word in enumerate(words):
        word = str(n) + '-' + word
        new_words.append(word)
    words = new_words
    width = len(max(words, key=len)) + pad
    ncols = max(1, termwidth // width)
    nrows = (len(words) - 1) // ncols + 1
    table = []
    for i in range(nrows):
        row = words[i::nrows]
        format_str = ('%%-%ds' % width) * len(row)
        table.append(format_str % tuple(row))
    return '\n'.join(table)

def remove_files(path, extension):
    """Given a path, and extension remove all files that ends with that extension"""
    files = [path + file for file in os.listdir(path) if file.endswith(extension)]
    for file in files:
        os.remove(file)

def get_stations_available():
    return 'Currently available stations: \n' + tabulate(name_stations)


def print_msg_box(msg, indent=1, width=None, title=None):
    """Print message-box with optional title."""
    lines = msg.split('\n')
    space = " " * indent
    if not width:
        width = max(map(len, lines))
    box = f'╔{"═" * (width + indent * 2)}╗\n'  # upper_border
    if title:
        box += f'║{space}{title:<{width}}{space}║\n'  # title
        box += f'║{space}{"-" * len(title):<{width}}{space}║\n'  # underscore
    box += ''.join([f'║{space}{line:<{width}}{space}║\n' for line in lines])
    box += f'╚{"═" * (width + indent * 2)}╝'  # lower_border
    return box

def get_file_burst_names(instrument):
    """
    This function return the file names which contains solar burst for specific station.
    """
    if not os.path.isfile(GLOBAL_PATH + 'solar_burst_file_names.xlsx'):
        if os.path.isdir(GLOBAL_PATH + 'Solar_bursts_files/pngs'):
            file_names = ['_'.join(file.split('_')[:-1]) + '.fit.gz' for file in os.listdir(GLOBAL_PATH + 'Solar_bursts_files/pngs') if file.startswith(instrument)]
            pd.DataFrame(file_names, columns=['solar_bursts_file_names']).to_excel(GLOBAL_PATH + 'solar_burst_file_names.xlsx')
    df = pd.read_excel(GLOBAL_PATH + 'solar_burst_file_names.xlsx')
    df.drop('Unnamed: 0', axis=1, inplace=True)
    df = df[df.solar_bursts_file_names.str.contains(instrument)]
    return df.solar_bursts_file_names.values



def print_start_download():
    start = time.time()
    now = datetime.datetime.now()
    print('Starting download (' + str(now.hour) + ':' + str(now.minute) + ':' + str(now.second) + ')')
    return start

def print_end_download(start):
    now = datetime.datetime.now()
    end = time.time()
    print('End download (' + str(now.hour) + ':' + str(now.minute) + ':' + str(now.second) + '), time needed:',
          str(round(end-start)) + 's')


#----------------------------------------------------------ERROR MANAGEMENT FUNCTIONS----------------------------------------------------------
def ask_for_int_option(min_option, max_option, msg):
    valid_option = 0
    while not valid_option:
        print(msg)
        option = input(">>>>> ")
        try:
            option       = int(option)
            valid_option = True if min_option <= option <= max_option else input(
                'Please, choose one posible option \'' + str(option) + '\' is not one of them: ')
        except ValueError:
            option = input('Please, choose one posible option \'' + str(option) + '\' is not one of them: ')

        return option


def ask_for_station():
    msg   = "\n"*100 + get_stations_available()
    station = ask_for_int_option(0, 48, msg)
    return station


def ask_for_year():
    msg  = 'Introduce the start year which you would like to download the data [1980-2022]: '
    year = ask_for_int_option(1980, 2022, msg)
    return year

def ask_second_year(start_year):
    end_valid_year = False
    while not end_valid_year:
        print('Introduce the end year which you would like to download the data [' + str(start_year) + '-2022]: ')
        end_year = input()
        try:
            end_year = int(end_year)
            if start_year <= end_year <= 2022:
                end_valid_year = True
        except ValueError:
            continue
    return end_year


def ask_download_solar_burst(station):
    files_burst  = []
    download_all = ask_for_int_option(0, 1, 'Would you like to download also the solar burst? 0/1: ')
    if not download_all:
        files_burst = get_file_burst_names(station)
    return files_burst


def ask_burst_15():
    msg = 'would you also like to download solar bursts with more than 15 minutes duration? 0/1: '\
          '\nPlease note that usually, when they are longer than 15 minutes, there are files in the range that do not actually contain solar burst.'
    download_all = ask_for_int_option(0, 1, msg)
    return download_all


def ask_for_splits():
    valid_splits = False
    available_splits = [0, 3, 5, 15]
    while not valid_splits:
        splits = input('Introduce the number of divisions you would like to make on the image | 0 for no divisions.'
                       '\nPlease note that since each image is 15 minutes, the possible divisions are 3, 5, 15: ')
        try:
            splits = int(splits)
            if splits in available_splits:
                valid_splits = True

        except ValueError:
            continue
    return splits



def update_sb_database():
    BD.get_file_burst_data(GLOBAL_PATH)
    url                 = 'http://soleil80.cs.technik.fhnw.ch/solarradio/data/2002-20yy_Callisto/'
    data_burst_data     = pd.read_excel(GLOBAL_PATH + 'solar_burst_data.xlsx', dtype='object')
    data_burst_stations = data_burst_data['stations'].values
    data_burst_dates    = data_burst_data['date'].values
    data_burst_starts   = data_burst_data['start'].values
    data_burst_ends     = data_burst_data['end'].values
    unique_dates        = np.unique(data_burst_dates)
    tasks_per_thread    = threads_managements(unique_dates)
    download_all        = 1

    threads = 1 if DEBUG else os.cpu_count()


    """
    def update_sb_database(data_burst_stations, data_burst_dates, data_burst_starts, data_burst_ends,
                       unique_dates, global_path, url, current_files, download_all, thread_id):
    """
    # DEBUG ONE THREAD
    if threads == 1:
        BD.update_sb_database(data_burst_stations, data_burst_dates, data_burst_starts, data_burst_ends,
                              unique_dates, GLOBAL_PATH, url, download_all, 1)
    # # OPTIMIZING MULTIPLE THREADS
    else:
        threads_id = list(range(threads))
        with Pool(threads) as executor:
            executor.starmap(BD.update_sb_database,
                             zip(repeat(data_burst_stations), repeat(data_burst_dates), repeat(data_burst_starts), repeat(data_burst_ends),
                                 tasks_per_thread,            repeat(GLOBAL_PATH),      repeat(url),               repeat(download_all),
                                 threads_id
                                 )
                            )
    BD.join_databases(GLOBAL_PATH)


#----------------------------------------------------------DOWNLOAD FUNCTIONS----------------------------------------------------------
def download_all_data_all_stations(extension):
    """ DOWNLOAD ALL THE DATA FROM 1980 TO 2022 """
    months = list(range(1, 13))
    num_splits = ask_for_splits()
    for station in name_stations:
        files_burst  = ask_download_solar_burst(name_stations[station])
        for year in range(1980, 2024):
            path = GLOBAL_PATH + 'Instruments/' + name_stations[station] + '_WSB_' + str(num_splits) + 'splits/' if len(files_burst) == 0 else \
                   GLOBAL_PATH + 'Instruments/' + name_stations[station] + '_NSB_' + str(num_splits) + 'splits/'
            with Pool(12) as executor:
                executor.starmap(cd.download, zip(repeat(year), months, repeat('ALL'), repeat(station), repeat(extension), repeat(path), repeat(num_splits)))


def download_year_one_station(extension):
    year         = ask_for_year()
    station      = ask_for_station()
    months       = list(range(1, 13))
    files_burst  = ask_download_solar_burst(name_stations[station])
    num_splits   = ask_for_splits()
    start        = print_start_download()
    path         = GLOBAL_PATH + 'Instruments/' + name_stations[station] + '_WSB_' + str(num_splits) + 'splits/' if len(files_burst) == 0 else\
                   GLOBAL_PATH + 'Instruments/' + name_stations[station] + '_NSB_' + str(num_splits) + 'splits/'
    threads_id   = 1

    print('Downloading data for ' + name_stations[station] + ' in the year ' + str(year) + '\nFiles will be saved at' + path)

    if DEBUG:
        cd.download(year, 1, 'ALL', name_stations[station], extension, files_burst, path, num_splits, threads_id)
    else:
        threads_id = list(range(12))
        with Pool(12) as executor:
            executor.starmap(cd.download, zip(repeat(year), months, repeat(name_stations[station]),
                                              repeat(extension), repeat(files_burst), repeat(path), repeat(num_splits), threads_id))
    print_end_download(start)

def download_all_data_one_station(extension):
    """DOWNLOAD ALL YEARS OF SPECIFIC STATION"""
    start_year  = 1989
    end_year    = 2022
    months      = list(range(1, 13))
    station     = ask_for_station()
    files_burst = ask_download_solar_burst(name_stations[station])
    num_splits  = ask_for_splits()
    start       = print_start_download()
    path        = GLOBAL_PATH + 'Instruments/' + name_stations[station] + '_WSB_' + str(num_splits) + 'splits/' if len(files_burst) == 0 else\
                  GLOBAL_PATH + 'Instruments/' + name_stations[station] + '_NSB_' + str(num_splits) + 'splits/'
    threads_id  = 1

    for year in range(start_year, end_year + 1):
        if DEBUG:
            cd.download(year, 1, 'ALL', name_stations[station], extension, files_burst, path, num_splits, threads_id)
        else:
            threads_id = list(range(12))
            with Pool(12) as executor:
                executor.starmap(cd.download, zip(repeat(year), months, repeat(name_stations[station]),
                                                  repeat(extension), repeat(files_burst), repeat(path),
                                                  repeat(num_splits), threads_id))
    print_end_download(start)

def download_customize(extension):
    months      = list(range(1, 13))
    start_year  = ask_for_year()
    end_year    = ask_second_year(start_year)
    station     = ask_for_station()
    files_burst = ask_download_solar_burst(name_stations[station])
    num_splits  = ask_for_splits()
    start       = print_start_download()
    path        = GLOBAL_PATH + 'Instruments/' + name_stations[station] + '_WSB_' + str(num_splits) + 'splits/' if len(files_burst) == 0 else\
                  GLOBAL_PATH + 'Instruments/' + name_stations[station] + '_NSB_' + str(num_splits) + 'splits/'
    threads_id  = 1

    print('Downloading data for ' + name_stations[station] + ' for the years[' + str(start_year) + '-' + str(end_year) + ']\nFiles will be saved at' + path)
    for year in range(start_year, end_year + 1):
        if DEBUG:
            cd.download(year, 1, 'ALL', name_stations[station], extension, files_burst, path, num_splits, threads_id)
        else:
            threads_id = list(range(12))
            with Pool(12) as executor:
                executor.starmap(cd.download, zip(repeat(year), months, repeat(name_stations[station]),
                                                  repeat(extension), repeat(files_burst), repeat(path),
                                                  repeat(num_splits), threads_id))
    print_end_download(start)


def download_solar_burst(extension):
    if not os.path.isfile(GLOBAL_PATH + 'solar_burst_data.xlsx'): BD.get_file_burst_data(GLOBAL_PATH)
    url                 = 'http://soleil80.cs.technik.fhnw.ch/solarradio/data/2002-20yy_Callisto/'
    data_burst_data     = pd.read_excel(GLOBAL_PATH + 'solar_burst_data.xlsx', dtype='object')
    data_burst_stations = data_burst_data['stations'].values
    data_burst_dates    = data_burst_data['date'].values
    data_burst_starts   = data_burst_data['start'].values
    data_burst_ends     = data_burst_data['end'].values
    data_burst_types    = data_burst_data['type_sb'].values
    unique_dates        = np.unique(data_burst_dates)
    tasks_per_thread    = threads_managements(unique_dates)
    download_all        = ask_burst_15()
    num_splits          = ask_for_splits()
    threads_id          = 1

    threads = 1 if DEBUG else os.cpu_count()

    if DEBUG:
        normal_path      = GLOBAL_PATH + 'DEBUG_Solar_bursts_files/' + extension[1:] + 's_' + str(num_splits) + 'splits/'
        fifteen_min_path = GLOBAL_PATH + 'DEBUG_Solar_bursts_files/' + extension[1:] + 's_15min_' + str(num_splits) + 'splits/'

        # if os.path.isdir(normal_path):      shutil.rmtree(normal_path)
        # if os.path.isdir(fifteen_min_path): shutil.rmtree(fifteen_min_path)

        path = normal_path if download_all else fifteen_min_path

    else:
        path = GLOBAL_PATH + 'Solar_bursts_files/' + extension[1:] + 's_' + str(num_splits) + 'splits/' if download_all else\
               GLOBAL_PATH + 'Solar_bursts_files/' + extension[1:] + 's_15min_' + str(num_splits) + 'splits/'


    if not os.path.isdir(path): os.makedirs(path)
    current_files = ['_'.join(file.split('_')[:-1]) + '.fit.gz' for file in os.listdir(path)]

    start = time.time()
    now = datetime.datetime.now()
    print('Starting download (' + str(now.hour) + ':' + str(now.minute) + ':' + str(now.second) + '), current files in dir:', len(current_files), 'please be patient')
    print('Data will be saved at: ' + path)
    # DEBUG ONE THREAD
    if threads == 1:
        BD.download_solar_burst_concurrence(data_burst_stations, data_burst_dates, data_burst_starts, data_burst_ends,
                                            data_burst_types, unique_dates[:], path, url, extension, current_files, download_all, num_splits)
    # # OPTIMIZING MULTIPLE THREADS
    else:
        threads_id = list(range(12))
        with Pool(threads) as executor:
            executor.starmap(BD.download_solar_burst_concurrence,
                             zip(repeat(data_burst_stations), repeat(data_burst_dates), repeat(data_burst_starts),
                                 repeat(data_burst_ends),     repeat(data_burst_types), tasks_per_thread,
                                 repeat(path),                repeat(url),              repeat(extension),
                                 repeat(current_files),       repeat(download_all),     repeat(num_splits),
                                 threads_id)
                            )
    now = datetime.datetime.now()
    end = time.time()
    print('End download (' + str(now.hour) + ':' + str(now.minute) + ':' + str(now.second) + '), time needed:',
          str(round(end-start)) + 's, files in directory:', len(os.listdir(path)))

    if os.path.isfile(GLOBAL_PATH + 'Perfomance.xlsx'):
        df = pd.read_excel(GLOBAL_PATH + 'Perfomance.xlsx')
        # df.drop('Unnamed: 0', axis=1, inplace=True)
        row = pd.DataFrame({
            'Threads': [str(threads)],
            'Files_Downloaded': [str(len(os.listdir(path)))],
            'Time': [str(round(end-start))]
        })
        df = df.append(row, ignore_index=True)
        df.to_excel(GLOBAL_PATH + 'Perfomance.xlsx', index=False)
    else:
        row = pd.DataFrame({
            'Threads': [str(threads)],
            'Files_Downloaded': [str(len(os.listdir(path)))],
            'Time': [str(round(end-start))]
        })
        row.to_excel(GLOBAL_PATH + 'Perfomance.xlsx', index=False)

#----------------------------------------------------------MAIN MENU----------------------------------------------------------
def print_menu():
    print(print_msg_box("Welcome to E-Callisto data downloader\nAuthors: Carlos Yanguas, Mario Fernandez, Vivek Reddy"))
    is_int      = 0
    end_program = 0
    main_msg    = print_msg_box("Please select one of the following options: "
                            "\n1-Show available stations"
                            "\n2-Download one year of data for specific station"
                            "\n3-Download all data for specific station"
                            "\n4-Download all data for all stations"
                            "\n5-Customize time lapse and stations "
                            "\n6-Download Solar bursts from 01/01/2020 to 15/02/2022"
                            "\n7-Update Solar burst database"
                            "\n8-Exit")
    msg_extension = "Please choose one posible extension for the data"\
                    "\n1- .npy if you want to download image 2D representation as npy array (Size around 2813KB/file)"\
                    "\n2- .fit if you want to download whole metadata as frequency or time  (Size around  732KB/file)"\
                    "\n3- .gz  if you want to unzip with other script later                 (Size around  200KB/file)"\
                    "\n4- .png if you want to download images with high contrast            (Size around  100KB/file)"

    while not end_program:
        main_option = ask_for_int_option(1, 8, main_msg)

        if   main_option == 1: print(get_stations_available())
        elif main_option == 7: update_sb_database()
        elif main_option == 8: end_program = 1
        else :
            extension = ask_for_int_option(1, 4, msg_extension)
            if   extension == 1: extension = '.npy'
            elif extension == 2: extension = '.fit'
            elif extension == 3: extension = '.gz'
            elif extension == 4: extension = '.png'

            if   main_option == 2: download_year_one_station(extension)
            elif main_option == 3: download_all_data_one_station(extension)
            elif main_option == 4: download_all_data_all_stations(extension)
            elif main_option == 5: download_customize(extension)
            elif main_option == 6: download_solar_burst(extension)



def main():
    """TESTS: DOWNLOAD ONE YEAR OF SPECIFIC STATION
    With thread pool - 12 days = 1:47 minutes
    Sequencially     - 1  day  = 0.48 minutes * 12 = 9 minutes * 12
    thread pool approx 6.122 times faster
    """
    #cd.download(2020, 1, 1, name_stations[10]) #Test
    print_menu()
    # prueba_lectura()
    # get_file_burst_names('GREENLAND')

if __name__ == '__main__':
    main()
