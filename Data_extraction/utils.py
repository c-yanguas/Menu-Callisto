"""
AUTHOR: Carlos Yanguas, Mario Fernandez
GITHUB: https://github.com/c-yanguas
"""

# REQUESTS AND FILE MANAGEMENT
import os
import gzip
from astropy.io import fits
import numpy as np
np.seterr(divide='ignore', invalid='ignore')                                 # Warning for 0 division on std=0
np.warnings.filterwarnings('ignore', category=np.VisibleDeprecationWarning)  # Warning for ragged array [ [1], [1,2] ] when splitting the array
import shutil

# Dates manipulation
from datetime import datetime, timedelta

# SOLVE THE "Fail to create pixmap with Tk_GetPixmap in TkImgPhotoInstanceSetSize" ERROR with Agg backend
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
from matplotlib import cm

#get_indexes func
import BurstDownloader as BD




def format_file_name(file, increment, solar_burst):
    """
    file_format: STATION_DATE_TIME_FOCUSCODE_TYPESB.png
    EXAMPLE
        input  --> 'Australia-ASSA_20210922_224506_01_VI.png', 16
        output --> 'Australia-ASSA_20210922_230106_01_VI.png'
    """
    data = file.split('_')
    station = data[0]
    if data[0] == 'Malaysia' and data[1] == 'Banting':  # Malaysia_Banting name of the station... error handler
        station  = data[0] + '-' + data[1]
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

    # DATETIMES CREATION
    file_date_time = datetime(file_year, file_month, file_day, file_hour, file_mins, file_sec)
    file_date_time = file_date_time + timedelta(minutes=increment)

    #File_name formated
    elements_to_format = [file_date_time.year, file_date_time.month, file_date_time.day, file_date_time.hour, file_date_time.minute, file_date_time.second]
    elements_formated  = ['0' + str(element) if element < 10 else str(element) for element in elements_to_format]
    file_name          = station + '_' +\
                            elements_formated[0] + elements_formated[1] + elements_formated[2] + '_' +\
                            elements_formated[3] + elements_formated[4] + elements_formated[5] + '_' +\
                            data[3]
    file_name          = file_name + '_' + data[4] if solar_burst else file_name

    return file_name



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


def gz_to_png(file_name, num_splits, solar_burst, file=None, start_burst=None, end_burst=None):
    with gzip.open(file_name + '.fit.gz', 'rb') as fin:
        with fits.open(fin) as fitfile:
            try:
                # READ AND GET FILE INFORMATION
                img   = fitfile['PRIMARY'].data.astype(np.uint8)
                img   = img[:-10]  # REMOVE WHITE MARKS AT THE BOTTOM OF THE IMG
                freqs = fitfile[1].data['Frequency'][0]
                times = fitfile[1].data['Time'][0]
                # img   = (img - img.mean(axis=1, keepdims=True)) / img.std(axis=1, keepdims=True)
                img   = img - img.mean(axis=1, keepdims=True)
                # img = img - img.mean(axis=1, keepdims=True)
                fitfile.close()

                if num_splits !=0:
                #IF SPLIT IMGS
                    # GET IMGS TO PLOT
                    split_img = np.array(np.array_split(img, axis=1, indices_or_sections=num_splits))
                    if solar_burst:
                        indexes_to_plot = BD.get_indexes(file, start_burst, end_burst, num_splits)

                    imgs_to_plot = split_img[indexes_to_plot] if solar_burst else split_img

                    # CREATE PNG WITHOUT PLOTING ON WINDOW
                    for i, img in enumerate(imgs_to_plot):
                        plt.ioff()  # Avoid plotting on window, so we save resources
                        plt.axis('off')
                        plt.imshow(img, aspect='auto', extent=(times[0], times[-1], freqs[-1], freqs[0]), cmap=cm.CMRmap,  vmin=0, vmax=12)
                        aux_file_name = '/'.join(file_name.split('/')[:-1]) + '/' + format_file_name(file_name.split('/')[-1], i, solar_burst)
                        plt.savefig(aux_file_name + '.png', bbox_inches='tight', pad_inches=0.0)
                        plt.close()

                else:
                #IF FULL IMG
                    plt.ioff()  # Avoid plotting on window, so we save resources
                    plt.axis('off')
                    # plt.imshow(img, aspect='auto', extent=(times[0], times[-1], freqs[-1], freqs[0]), cmap=cm.CMRmap, vmin=0, vmax=12)
                    plt.imshow(img, aspect='auto', extent=(times[0], times[-1], freqs[-1], freqs[0]), cmap=cm.CMRmap, vmin=0, vmax=12)
                    plt.savefig(file_name + '.png', bbox_inches='tight', pad_inches=0.0)
                    plt.close()

                os.remove(file_name + '.fit.gz')


            except Exception as e:
                print(e)
                os.remove(file_name + '.fit.gz')
