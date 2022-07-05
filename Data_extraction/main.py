"""
AUTHOR: Carlos Yanguas
GITHUB: https://github.com/c-yanguas
"""
from yolo_data import yolo
import CallistoDownloader as cd
import BurstDownloader as BD
from multiprocessing import Pool
import os
from itertools import repeat
import pandas as pd
import numpy as np
import time
from datetime import date, timedelta, datetime
import shutil

#----------------------------------------------------------STATIONS AVAILABLE----------------------------------------------------------
#If you want to add a new station you only have to append the original name of that station to this list.
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
TEST_PATH   = ''
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
        print('In order to get the file burst names, we will execute option 7')
        update_sb_database()
    df = pd.read_excel(GLOBAL_PATH + 'solar_burst_file_names.xlsx')
    df.drop('Unnamed: 0', axis=1, inplace=True)
    df = df[df.solar_bursts_file_names.str.contains(instrument)]
    return df.solar_bursts_file_names.values


def get_all_file_burst_names():
    df = pd.read_excel(GLOBAL_PATH + 'solar_burst_file_names.xlsx')
    df.drop('Unnamed: 0', axis=1, inplace=True)
    return df.solar_bursts_file_names.values




def print_start_download():
    start = time.time()
    now = datetime.now()
    print('Starting download (' + str(now.hour) + ':' + str(now.minute) + ':' + str(now.second) + ')')
    return start


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
    # msg   = "\n"*100 + get_stations_available()
    msg     = get_stations_available()
    station = ask_for_int_option(0, 48, msg)
    return station


def ask_for_year():
    msg  = 'Introduce the start year which you would like to download the data [1989-2022]: '
    year = ask_for_int_option(1989, 2022, msg)
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


def ask_for_dates():
    valid_date = 0
    while not valid_date:
        try:
            start_date = input('Introduce the start date (DD-MM-YYYY): ')
            end_date   = input('Introduce the end date (DD-MM-YYYY): ')

            # start_date = '01-01-2020'
            # end_date   = '02-02-2022'

            start_date = start_date.split('-')
            start_date = [int(x) for x in start_date]
            end_date   = end_date.split('-')
            end_date   = [int(x) for x in end_date]

            if start_date[2] < 1989 or end_date[2] > date.today().year:
                print('Data is only available in the lapse 1989-' + str(date.today().year))
            else:
                start_date = date(start_date[2], start_date[1], start_date[0])
                end_date   = date(end_date[2],   end_date[1],   end_date[0])
                valid_date = 1
        except:
            print('Please introduce a valid start and end date, for example 1-1-2020 and 2-2-2022')
            pass

        if valid_date:
            if start_date > end_date:
                valid_date = 0
                print('The start date should be earlier than the end date!')

    return start_date, end_date


def get_customize_dates(start_date, end_date):
    start_date = start_date.split('-')
    start_date = [int(x) for x in start_date]
    end_date = end_date.split('-')
    end_date = [int(x) for x in end_date]

    start_date = date(start_date[2], start_date[1], start_date[0])
    end_date   = date.today() if end_date[2] == date.today().year else date(end_date[2], end_date[1], end_date[0])

    return start_date, end_date


def get_dates(start_date, end_date):
    delta        = end_date - start_date  # returns timedelte
    unique_dates = []

    for i in range(delta.days + 1):
        next_day   = start_date + timedelta(days=i)
        day        = '0' + str(next_day.day)   if next_day.day   < 10 else str(next_day.day)
        month      = '0' + str(next_day.month) if next_day.month < 10 else str(next_day.month)
        year       = str(next_day.year)
        day_format = year + month + day
        unique_dates.append(day_format)

    return unique_dates



def describe_download(option, station, extension, num_splits, start_date, end_date, path, bursts_15_min=0):
    data = {'Option':    option,
            'Station':   station,
            'Extension': extension,
            'Splits': str(num_splits),
            'Start date': str(start_date),
            'End date': str(end_date),
            'Path_data': path
            }

    if option == 4: data['Burst > 15 min'] = bursts_15_min
    description = pd.DataFrame(data, index=['Option Description'])
    with pd.option_context('expand_frame_repr', False):
        print(description)

#----------------------------------------------------------DOWNLOAD FUNCTIONS----------------------------------------------------------
def download_year_one_station(extension):
    if not DEBUG:
        year                 = ask_for_year()
        station              = ask_for_station()
        start_date, end_date = get_customize_dates('1-1-' + str(year), '31-12-' + str(year))
        unique_dates         = get_dates(start_date, end_date)
        tasks_per_thread     = threads_managements(unique_dates)
        files_burst          = ask_download_solar_burst(name_stations[station])
        num_splits           = ask_for_splits() if extension == 4 else 0
        path                 = GLOBAL_PATH + 'Instruments/' + name_stations[station] + TEST_PATH +  '_WSB_' + str(num_splits) + 'splits_' + str(extension)[1:] + '/' \
                              if len(files_burst) == 0 else\
                              GLOBAL_PATH + 'Instruments/' + name_stations[station] + TEST_PATH +  '_NSB_' + str(num_splits) + 'splits_' + str(extension)[1:] + '/'

        describe_download(2, name_stations[station], extension, num_splits, start_date, end_date, path)
        threads_id = list(range(os.cpu_count()))
        with Pool(os.cpu_count()) as executor:
            executor.starmap(cd.download, zip(tasks_per_thread, repeat(name_stations[station]),
                                              repeat(extension), repeat(files_burst), repeat(path), repeat(num_splits), threads_id))
    else:
        station             = 11 # AUSTRALIA-LMRO
        threads_id          = 1
        star_date, end_date = get_customize_dates('1-1-' + str(2020), '31-12-' + str(2020))
        unique_dates        = get_dates(star_date, end_date)
        num_splits          = 0
        files_burst         = ask_download_solar_burst(name_stations[station])
        path                = GLOBAL_PATH + 'Instruments/' + name_stations[station] + TEST_PATH +  '_WSB_' + str(num_splits) + 'splits_' + str(extension)[1:] + '/' \
                              if len(files_burst) == 0 else\
                              GLOBAL_PATH + 'Instruments/' + name_stations[station] + TEST_PATH +  '_NSB_' + str(num_splits) + 'splits_' + str(extension)[1:] + '/'
        cd.download(unique_dates, name_stations[station], extension, files_burst, path, num_splits, threads_id)


def download_customize(extension, make_predictions=0):
    start_date, end_date = ask_for_dates()
    unique_dates         = get_dates(start_date, end_date)
    tasks_per_thread     = threads_managements(unique_dates)
    station              = ask_for_station()
    if not make_predictions:
        files_burst          = ask_download_solar_burst(name_stations[station])
        num_splits           = ask_for_splits()
        path                 = GLOBAL_PATH + 'Instruments/' + name_stations[station] + TEST_PATH +  '_WSB_' + str(num_splits) + 'splits_' + str(extension)[1:] + '/' \
                               if len(files_burst) == 0 else\
                               GLOBAL_PATH + 'Instruments/' + name_stations[station] + TEST_PATH +  '_NSB_' + str(num_splits) + 'splits_' + str(extension)[1:] + '/'
    else:
        path = 'yolo_data/files_to_predict/'
        if os.path.isdir(path): os.shutil.rmtree(path)
        os.makedirs(path)


    describe_download(3, name_stations[station], extension, num_splits, start_date, end_date, path)
    if DEBUG:
        threads_id = 1
        cd.download(unique_dates, name_stations[station], extension, files_burst, path, num_splits, threads_id)
    else:
        threads_id = list(range(os.cpu_count()))
        with Pool(os.cpu_count()) as executor:
            executor.starmap(cd.download, zip(tasks_per_thread, repeat(name_stations[station]), repeat(extension),
                                              repeat(files_burst), repeat(path), repeat(num_splits), threads_id))
    yolo.make_predictions(path)


def download_solar_burst(extension):
    if not os.path.isfile(GLOBAL_PATH + 'solar_burst_data.xlsx'):

        BD.get_file_burst_data(GLOBAL_PATH)
    url                 = 'http://soleil80.cs.technik.fhnw.ch/solarradio/data/2002-20yy_Callisto/'
    data_burst_data     = pd.read_excel(GLOBAL_PATH + 'solar_burst_data.xlsx', dtype='object')
    # data_burst_data     = data_burst_data.tail(1000)
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
        path = GLOBAL_PATH + 'Solar_bursts_files/' + TEST_PATH + extension[1:] + 's_' + str(num_splits) + 'splits/' if download_all else\
               GLOBAL_PATH + 'Solar_bursts_files/' + TEST_PATH + extension[1:] + 's_15min_' + str(num_splits) + 'splits/'


    if not os.path.isdir(path): os.makedirs(path)
    current_files = ['_'.join(file.split('_')[:-1]) + '.fit.gz' for file in os.listdir(path)]

    start = time.time()
    now = datetime.now()

    describe_download(4, 'ALL', extension, num_splits,
                      str(unique_dates[0][:4]) + '-'  + str(unique_dates[0][4:6])  + '-' + str(unique_dates[0][6:]),
                      str(unique_dates[-1][:4]) + '-' + str(unique_dates[-1][4:6]) + '-' + str(unique_dates[-1][6:]),
                      path, bursts_15_min=download_all)
    # DEBUG ONE THREAD
    if threads == 1:
        BD.download_solar_burst_concurrence(data_burst_stations, data_burst_dates, data_burst_starts, data_burst_ends,
                                            data_burst_types, unique_dates[:], path, url, extension, current_files, download_all, num_splits, threads_id)
    # # OPTIMIZING MULTIPLE THREADS
    else:
        threads_id = list(range(threads))
        with Pool(threads) as executor:
            executor.starmap(BD.download_solar_burst_concurrence,
                             zip(repeat(data_burst_stations), repeat(data_burst_dates), repeat(data_burst_starts),
                                 repeat(data_burst_ends),     repeat(data_burst_types), tasks_per_thread,
                                 repeat(path),                repeat(url),              repeat(extension),
                                 repeat(current_files),       repeat(download_all),     repeat(num_splits),
                                 threads_id)
                            )
    now = datetime.now()
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


def download_all_data_one_station(extension):
    """DOWNLOAD ALL YEARS OF SPECIFIC STATION"""
    num_splits           = ask_for_splits() if extension == 4 else 0
    start_date, end_date = get_customize_dates('1-1-1989', '31-12-' + str(date.today().year))
    station              = ask_for_station()
    files_burst          = ask_download_solar_burst(name_stations[station])
    unique_dates         = get_dates(start_date, end_date)
    tasks_per_thread     = threads_managements(unique_dates)
    path                 = GLOBAL_PATH + 'Instruments/' + name_stations[station] + TEST_PATH + '_WSB_' + str(num_splits) + 'splits_' + str(extension)[1:] + '/' \
                           if len(files_burst) == 0 else \
                           GLOBAL_PATH + 'Instruments/' + name_stations[station] + TEST_PATH + '_NSB_' + str(num_splits) + 'splits_' + str(extension)[1:] + '/'

    describe_download(5, name_stations[station], extension, num_splits, start_date, end_date, path)
    with Pool(os.cpu_count()) as executor:
        threads_id = list(range(os.cpu_count()))
        executor.starmap(cd.download, zip(tasks_per_thread, repeat(station), repeat(extension), repeat(files_burst),
                                          repeat(path), repeat(num_splits), threads_id))



def download_all_stations_customize(extension):
    """ DOWNLOAD ALL THE DATA FROM 1989 TO 2022 """
    num_splits           = ask_for_splits() if extension == 4 else 0
    start_date, end_date = ask_for_dates()
    download_bursts      = ask_for_int_option(0, 1, 'Would you also like to download solar bursts? 0/1')
    files_burst          = [] if download_bursts else get_all_file_burst_names()
    unique_dates         = get_dates(start_date, end_date)
    tasks_per_thread     = threads_managements(unique_dates)

    for station in name_stations:
        path             = GLOBAL_PATH + 'Instruments/' + station + TEST_PATH + '_WSB_' + str(num_splits) + 'splits_' + str(extension)[1:] + '/' \
                           if len(files_burst) == 0 else \
                           GLOBAL_PATH + 'Instruments/' + station + TEST_PATH + '_NSB_' + str(num_splits) + 'splits_' + str(extension)[1:] + '/'
        files_burst_i    = [file for file in files_burst if file.startswith(station)]
        print('\n')
        describe_download(6, station, extension, num_splits, start_date, end_date, path)


        if DEBUG:
            cd.download(unique_dates, station, extension, files_burst_i, path, num_splits, 1)
        else:
            with Pool(os.cpu_count()) as executor:
                threads_id = list(range(os.cpu_count()))
                # download(unique_dates, instrument, extension, file_burst_names, path, num_splits, thread_id)
                executor.starmap(cd.download, zip(tasks_per_thread, repeat(station), repeat(extension), repeat(files_burst_i),
                                                  repeat(path), repeat(num_splits), threads_id))





#----------------------------------------------------------MAIN MENU----------------------------------------------------------
def print_menu():
    print(print_msg_box("Welcome to E-Callisto data downloader\nAuthors: Carlos Yanguas and Mario Fernandez"))
    is_int      = 0
    end_program = 0
    main_msg    = print_msg_box("Please select one of the following options: "
                            "\n1-Show available stations"
                            "\n2-Download one year of data for specific station"
                            "\n3-Download customize time lapse and station"
                            "\n4-Download Solar bursts reported since 01/01/2020"
                            "\n5-Download all data for specific station"
                            "\n6-Download customize for all stations"
                            "\n7-Update Solar burst database"
                            "\n8-Make detections with YOLO V4"
                            "\n9-Exit")
    msg_extension = "Please choose one posible extension for the data"\
                    "\n1- .npy if you want to download image 2D representation as npy array (Size around 2813KB/file)"\
                    "\n2- .fit if you want to download whole metadata as frequency or time  (Size around  732KB/file)"\
                    "\n3- .gz  if you want to unzip with other script later                 (Size around  200KB/file)"\
                    "\n4- .png if you want to download images with high contrast            (Size around  100KB/file)"

    while not end_program:
        main_option = ask_for_int_option(1, 8, main_msg)

        if   main_option == 1: print(get_stations_available())
        elif main_option == 7: update_sb_database()
        elif main_option == 8: download_customize('.png', make_predictions=1)
        elif main_option == 9: end_program = 1
        else :
            extension = ask_for_int_option(1, 4, msg_extension)
            if   extension == 1: extension = '.npy'
            elif extension == 2: extension = '.fit'
            elif extension == 3: extension = '.gz'
            elif extension == 4: extension = '.png'

            if   main_option == 2: download_year_one_station(extension)
            elif main_option == 3: download_customize(extension)
            elif main_option == 4: download_solar_burst(extension)
            elif main_option == 5: download_all_data_one_station(extension)
            elif main_option == 6: download_all_stations_customize(extension)

def main():
    yolo.make_predictions('yolo_data/files_to_predict/')
#     print_menu()

if __name__ == '__main__':
    main()
