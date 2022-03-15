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

#-----------------------------STATIONS AVAILABLE-----------------------------
name_stations = [
    'ALASKA-ANCHORAGE', 'ALASKA-COHOE', 'ALASKA-HAARP', 'ALGERIA-CRAAG', 'ALMATY', 'AUSTRIA-Krumbach',
    'AUSTRIA-MICHELBACH', 'Acreibo-observatory',
    'AUSTRIA-OE3FLB', 'AUSTRIA-UNIGRAZ', 'Australia-ASSA', 'Australia-LMRO', 'DENMARK', 'GLASGOW', 'EGYPT',
    'GERMANY-DLR', 'GREENLAND', 'HUMAIN',
    'HURBANOVO', 'INDIA-GAURI', 'INDIA-IISERP', 'INDIA-Nashik', 'INDIA-OOTY', 'INDIA-UDAIPUR', 'INDONESIA',
    'JAPAN-IBARAKI', 'KASI', 'KRIM', 'MEXART',
    'MONGOLIA-GOBI', 'MONGOLIA-UB', 'MRO', 'NEWZEALAND-AUT', 'NORWAY-NY-AALESUND', 'NORWAY-RANDABERG', 'ROSWELL-NM',
    'SOUTHAFRICA-SANSA', 'SPAIN-ALCALA',
    'SPAIN-PERALEJOS', 'SPAIN-SIGUENZA', 'SWISS-BLEN5M', 'SWISS-BLEN7M', 'SWISS-HB9SCT', 'SWISS-IRSOL',
    'SWISS-Landschlacht', 'SWISS-MUHEN', 'TRIEST',
    'URUGUAY', 'USA-ARIZONA-ERAU'
]

#-----------------------------PATH-----------------------------
"""
Path where data will be saved, please, if you want to change this path (NOT RECOMMENDED), move also solar_burst_file_names.xlsx
to the desired path, since its neccesary to avoid downloading solar burst files while downloading data for specified station.
"""
GLOBAL_PATH = '../Data/'

#-----------------------------AUXILIAR FUNCTIONS-----------------------------
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

def print_stations_available():
    print('Currently available stations: ')
    # for n_station, station in enumerate(name_stations):
    #     print("{: >20}", str(n_station), '-', station)
    print(tabulate(name_stations))


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
    return df.solar_bursts_file_names

#-----------------------------DOWNLOAD FUNCTIONS-----------------------------
def download_all_data_all_stations(extension):
    """ DOWNLOAD ALL THE DATA FROM 1980 TO 2022 """
    months = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]
    for station in name_stations:
        for year in range(1980, 2024):
            with Pool(12) as executor:
                executor.starmap(cd.download, zip(repeat(year), months, repeat('ALL'), repeat(station), repeat(extension), repeat(GLOBAL_PATH)))


def download_year_one_station(extension):
    # cd.download(2020, 1, 1, name_stations[10], extension)  # Test
    year = 0
    station = 0
    valid_year = False
    valid_station = False
    files_burst = []
    while not valid_year:
        year = input('Introduce the year which you would like to download the data [1980-2022]: ')
        try:
            year = int(year)
            if 1980 <= year <= 2022:
                valid_year = True

        except ValueError:
            pass

    while not valid_station:
        print("\n" * 100)
        print_stations_available()
        station = input('Introduce the station which you would like to download the data [0-48]: ')
        try:
            station = int(station)
            if 0 <= station <= 48:
                valid_station = True
                if year >= 2020:
                    download_all = input('Would you like to download also the solar burst? Y/other: ')
                    if download_all.lower() != 'y':
                        files_burst = get_file_burst_names(name_stations[station])
        except ValueError:
            pass
    #cd.download(2020, 1, 1, name_stations[10])
    months = list(range(1, 13))
    now = datetime.datetime.now()
    print('Starting download (' + str(now.hour) + ':' + str(now.minute) + ':' + str(now.second) + '), please be patient')
    with Pool(12) as executor:
        executor.starmap(cd.download, zip(repeat(year), months, repeat('ALL'), repeat(name_stations[station]),
                                          repeat(extension), repeat(files_burst), repeat(GLOBAL_PATH)))
    # cd.download(2020, 1, 1, name_stations[10], extension, files_burst, GLOBAL_PATH)

def download_all_data_one_station(extension):
    """DOWNLOAD ALL YEARS OF SPECIFIC STATION"""
    station = 0
    start_year = 1989
    end_year = 2022
    months = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]
    valid_station = False
    files_burst = []
    while not valid_station:
        print("\n" * 100)
        print_stations_available()
        station = input('Introduce the station which you would like to download the data [0-48]: ')
        try:
            station = int(station)
            if 0 <= station <= 48:
                valid_station = True
                download_all = input('Would you like to download also the solar burst? Y/other: ')
                if download_all.lower() != 'y':
                    files_burst = get_file_burst_names(name_stations[station])
        except ValueError:
            pass
    now = datetime.datetime.now()
    print('Starting download (' + str(now.hour) + ':' + str(now.minute) + ':' + str(now.second) + '), please be patient')
    for year in range(start_year, end_year + 1):
        with Pool(12) as executor:
            executor.starmap(cd.download, zip(repeat(year), months, repeat('ALL'), repeat(name_stations[station]),
                                              repeat(extension), repeat(files_burst), repeat(GLOBAL_PATH)))


def download_customize(extension):
    start_valid_year = False
    end_valid_year = False
    valid_stations = False
    start_year = 0
    stations_validate = []
    end_year = 0
    files_burst = []
    while not start_valid_year:
        start_year = input('Introduce the start year which you would like to download the data [1980-2022]: ')
        try:
            start_year = int(start_year)
            if 1980 <= start_year <= 2022:
                start_valid_year = True
        except ValueError:
            pass

    while not end_valid_year:
        print('Introduce the end year which you would like to download the data[' + str(start_year) + '-2022]: ')
        end_year = input()
        try:
            end_year = int(end_year)
            if start_year <= end_year <= 2022:
                end_valid_year = True
        except ValueError:
            pass

    while not valid_stations:
        print("\n" * 100)
        print_stations_available()
        stations = input('Introduce the stations which you would like to download the data [0-48] space separated: ')
        stations = list(set(stations.split(' ')))
        valid_stations = True
        for station in stations:
            try:
                station = int(station)
                if 0 <= station <= 48:
                    stations_validate.append(station)
                else:
                    print(str(station), ' is not a valid station')
                    valid_stations = False
            except ValueError:
                break
        if end_year >= 2020:
            download_all = input('Would you like to download also the solar burst? Y/other: ')
            if download_all.lower() != 'y':
                files_burst = get_file_burst_names(name_stations[station])
    print('Stations ', list(map(name_stations.__getitem__, stations_validate)),
          'data would be downloaded for the years [' + str(start_year) + '-' + str(end_year) + ']')
    months = list(range(1, 13))
    for station in stations_validate:
        for year in range(start_year, end_year + 1):
            with Pool(12) as executor:
                executor.starmap(cd.download, zip(repeat(year), months, repeat('ALL'), repeat(name_stations[station]),
                                                  repeat(extension), repeat(files_burst), repeat(GLOBAL_PATH)))

def download_solar_burst(extension):
    print('would you also like to download solar bursts with more than 15 minutes duration? Y/other: '
         '\nPlease note that usually, when they are longer than 15 minutes, there are files in the range that do not actually contain solar burst.')
    download_all = input(">>>>> ")
    if download_all.lower() != 'y':
        download_all = 0
    else:
        download_all = 1

    if not os.path.isfile(GLOBAL_PATH + 'solar_burst_data.xlsx'): BD.get_file_burst_data(GLOBAL_PATH)
    url              = 'http://soleil80.cs.technik.fhnw.ch/solarradio/data/2002-20yy_Callisto/'
    data_burst_data  = pd.read_excel(GLOBAL_PATH + 'solar_burst_data.xlsx', dtype='object')
    # DEBUG
    # files = 200
    # threads = 1
    threads = os.cpu_count()
    # data_burst_data     = data_burst_data[:files]
    data_burst_stations = data_burst_data['stations'].values
    data_burst_dates    = data_burst_data['date'].values
    data_burst_starts   = data_burst_data['start'].values
    data_burst_ends     = data_burst_data['end'].values
    data_burst_types    = data_burst_data['type_sb'].values
    unique_dates        = np.unique(data_burst_dates)
    if download_all:
        path = GLOBAL_PATH + 'Solar_bursts_files/' + extension[1:] + 's/'
    else:
        path = GLOBAL_PATH + 'Solar_bursts_files/' + extension[1:] + 's_15_min/'
    tasks_per_thread    = threads_managements(unique_dates)
    # shutil.rmtree(path)
    if not os.path.isdir(path): os.makedirs(path)
    current_files = ['_'.join(file.split('_')[:-1]) + '.fit.gz' for file in os.listdir(path)]

    start = time.time()
    now = datetime.datetime.now()
    print('Starting download (' + str(now.hour) + ':' + str(now.minute) + ':' + str(now.second) + '), current files in dir:',len(current_files), 'please be patient')
    # DEBUG ONE THREAD
    if threads == 1:
        BD.download_solar_burst_concurrence(data_burst_stations, data_burst_dates, data_burst_starts, data_burst_ends,
                                            data_burst_types, unique_dates[:], path, url, extension, current_files, download_all)
    # # OPTIMIZING MULTIPLE THREADS
    else:
        with Pool(threads) as executor:
            executor.starmap(BD.download_solar_burst_concurrence,
                             zip(repeat(data_burst_stations), repeat(data_burst_dates), repeat(data_burst_starts),
                                 repeat(data_burst_ends),     repeat(data_burst_types), tasks_per_thread,
                                 repeat(path),                repeat(url),              repeat(extension),
                                 repeat(current_files),       repeat(download_all)
                                 )
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

#-----------------------------MAIN MENU-----------------------------
def print_menu():
    print(print_msg_box("Welcome to E-Callisto data downloader\nAutors: Carlos Yanguas, Mario Fernandez, Vivek Reddy"))
    is_int      = 0
    end_program = 0
    while not end_program:
        is_int = 0
        print(print_msg_box("Please select one of the following options: "
                            "\n1-Show available stations"
                            "\n2-Download one year of data for specific station"
                            "\n3-Download all data for specific station"
                            "\n4-Download all data for all stations"
                            "\n5-Customize time lapse and stations "
                            "\n6-Download Solar bursts from 01/01/2020 to 15/02/2022"
                            "\n7-Exit"))
        option = input(">>>>> ")
        while not is_int:
            try:
                option = int(option)
                if 0 < option < 8: is_int = True
                else:option = input('Please, choose one posible option \'' + str(option) + '\' is not one of them: ')
            except ValueError:
                option = input('Please, choose one posible option \'' + str(option) + '\' is not one of them: ')

        if   option == 1: print_stations_available()
        elif option == 7: end_program = 1
        else:
            print("Please choose one posible extension for the data"
                  "\n1- .npy if you want to download image 2D representation as npy array (Size around 2813KB/file)"
                  "\n2- .fit if you want to download whole metadata as frequency or time  (Size around  732KB/file)"
                  "\n3- .gz  if you want to unzip with other script later                 (Size around  200KB/file)"
                  "\n4- .png if you want to download images with high contrast            (Size around  100KB/file)")
            extension = input('>>>>> ')
            try:
                extension = int(extension)
                if 0 < extension < 5:
                    is_int = True
                    if extension == 1:   extension = '.npy'
                    elif extension == 2: extension = '.fit'
                    elif extension == 3: extension = '.gz'
                    elif extension == 4: extension = '.png'
                else: extension = input('Please, choose one posible extension \'' + str(extension) + '\' is not one of them: ')

            except ValueError: extension = input('Please, choose one posible option \'' + str(option) + '\' is not one of them: ')

            if   option == 2: download_year_one_station(extension)
            elif option == 3: download_all_data_one_station(extension)
            elif option == 4: download_all_data_all_stations(extension)
            elif option == 5: download_customize(extension)
            elif option == 6: download_solar_burst(extension)








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
