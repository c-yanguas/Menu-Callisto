from tqdm import tqdm
import numpy as np
import cv2
import re
import os
from datetime import datetime, timedelta


def get_datetime_from_fname(fname):
    'Extrat file date time from file name'
    data = fname.split('_')
    station = data[0]
    date_pos = 1
    time_pos = 2
    # FILE DATE
    file_year = int(data[date_pos][:4])
    file_month = int(data[date_pos][4:6])
    file_day = int(data[date_pos][6:])
    # FILE TIME
    file_hour = int(data[time_pos][:2])
    file_mins = int(data[time_pos][2:4])
    file_sec = int(data[time_pos][4:])

    # FILE DATETIME
    file_date_time = datetime(file_year, file_month, file_day, file_hour, file_mins, file_sec)

    return file_date_time


def obtain_times_from_lines(lines, width_img, fname, bursts_dates, bursts_times, stations):
    """
    INITIAL FORMAT
    Burst: 24%	(left_x:  148   top_y:  -25   width:   37   height:  351)
    return: [HH:MM-HH:MM, HH:MM-HH:MM, ...]
    fname: ALASKA-ANCHORAGE_20220331_183024_01_IV
    ES NECESARIO CONOCER EL TAMAÑO DE LA IMAGEN ORIGINAL
    """
    next_line_is_burst = True
    next_line = 1
    while next_line_is_burst:
        # Update condition to break while
        next_line = next_line + 1
        next_line_is_burst = lines[next_line].startswith('Burst')
        # Get data from line
        current_line = lines[next_line - 1]
        confidence = int(re.search('Burst: ' + '(.*)%', current_line).group(1))
        left = int(re.search('left_x:  ' + '(.*)   top_y', current_line).group(1))
        width = int(re.search('width:  ' + '(.*)   height', current_line).group(1))
        right = left + width
        # Format data to get start and end of bursts
        time_start = left / width_img * 15  # left/width = [0-1], [0-1] * 15 = pos temporal
        time_end = right / width_img * 15

        if time_start < 0: time_start = 0
        if time_end > 15:  time_end = 15
        # Create datetime from file name
        file_datetime = get_datetime_from_fname(fname)
        # Get datetimes sbs
        sb_start = file_datetime + timedelta(minutes=time_start)
        sb_end = file_datetime + timedelta(minutes=time_end)

        # Get dates sbs
        sb_start_year = str(sb_start.year)
        sb_start_month = '0' + str(sb_start.month) if sb_start.month < 10 else str(sb_start.month)
        sb_start_day = '0' + str(sb_start.day) if sb_start.day < 10 else str(sb_start.day)
        sb_start_date = sb_start_year + sb_start_month + sb_start_day

        sb_end_year = str(sb_end.year)
        sb_end_month = '0' + str(sb_end.month) if sb_end.month < 10 else str(sb_end.month)
        sb_end_day = '0' + str(sb_end.day) if sb_end.day < 10 else str(sb_end.day)
        sb_end_date = sb_end_year + sb_end_month + sb_end_day

        # Get times sbs
        sb_start_hour = '0' + str(sb_start.hour) if sb_start.hour < 10 else str(sb_start.hour)
        sb_start_minute = '0' + str(sb_start.minute) if sb_start.minute < 10 else str(sb_start.minute)
        sb_start_time = sb_start_hour + ':' + sb_start_minute
        sb_end_hour = '0' + str(sb_end.hour) if sb_end.hour < 10 else str(sb_end.hour)
        sb_end_minute = '0' + str(sb_end.minute) if sb_end.minute < 10 else str(sb_end.minute)
        sb_end_time = sb_end_hour + ':' + sb_end_minute

        # station name
        station = fname.split('/')[-1].split('_')[0]

        if sb_start_date == sb_end_date:
            bursts_times.append(sb_start_time + '-' + sb_end_time)
            bursts_dates.append(sb_start_date)
            stations.append(station)

        else:
            bursts_times.append(sb_start_time + '-23:59')
            bursts_dates.append(sb_start_date)
            stations.append(station)

            bursts_times.append('00:00-' + sb_end_time)
            bursts_dates.append(sb_end_date)
            stations.append(station)

    return bursts_dates, bursts_times, stations


def generate_report_from_prediction_file(yolo_prediction_file, path_save_report, img_width, init_path_files):
    # col_0, col_1, col_2, col_3 = 0, 16, 32, 40
    col_0, col_1, col_2 = 16, 16, 8
    bursts_times = []
    stations = []
    bursts_dates = []
    report_name = path_save_report + 'general_report.txt'
    if os.path.isfile(report_name): os.remove(report_name)

    # OBTAIN LIST OF DATETIMES AND ANOTHER ONE WITH LINKED STATION TO THE SB DETECTED
    with open(yolo_prediction_file, 'r') as nasty_file:
        lines = nasty_file.read().splitlines()
        for i in tqdm(range(len(lines)), desc='Parsing file to generate report '):
            if lines[i].startswith(init_path_files):
                fname = (init_path_files + re.search(init_path_files + '(.*).png', lines[i]).group(1)).split('/')[-1]
                if lines[i + 1].startswith('Burst'):
                    # Obtain start and ends bursts
                    bursts_dates, bursts_times, stations = obtain_times_from_lines(lines[i:], img_width, fname,
                                                                                   bursts_dates, bursts_times, stations)
    # Convert list to np
    bursts_datetimes = np.array([bursts_dates[i] + bursts_times[i] for i in range(len(bursts_dates))])
    bursts_dates = np.array(bursts_dates)
    bursts_times = np.array(bursts_times)
    stations = np.array(stations)
    unique_bursts_datetimes = np.unique(bursts_datetimes)
    # WRITE REPORT
    with open(report_name, mode='w') as report:
        # WRITE HEADER
        report.write(f'{"#Date": <{col_0}} {"Time": <{col_1}} {"Type": <{col_2}} Stations\n')
        report.write('#-------------------------------------------------------------------------------\n')
        # WRITE GROUPED DATA
        for burst_datetime in tqdm(unique_bursts_datetimes, desc='Generating report'):
            indexes = np.array(np.where(bursts_datetimes == burst_datetime)[0])
            current_staions = (', ').join(np.unique(stations[indexes]))
            ##              Date                                 Time                                 Type            Stations
            report.write(
                f'{str(bursts_dates[indexes[0]]): <{col_0}} {str(bursts_times[indexes[0]]): <{col_1}} {"?": <{col_2}} {current_staions}\n')


# init_path_files = '/content/drive/MyDrive/E-Callisto/YOLO/e-callisto/tests_imgs/test_dataset_all_stations/'
# w, h = (496, 369)  # Images Width and Height REESCALATED BY YOLO, NOT ORIGINAL ONE
# PATH_RESULTS = 'results/'
# yolo_prediction_file = 'yolo_predictions_files/pred_all_stations.txt'
# path_save_report = PATH_RESULTS
# generate_report_from_prediction_file(yolo_prediction_file, path_save_report, w)