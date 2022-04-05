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
from tqdm import tqdm

url = 'http://soleil80.cs.technik.fhnw.ch/solarradio/data/2002-20yy_Callisto/'



def download(unique_dates, instrument, extension, file_burst_names, path, num_splits, thread_id):
    """
    MAIN FUNCTION
    lo suyo seria pasar unique dates aqui tambien
    """

    # initializing
    for date in tqdm(unique_dates, desc='THREAD ' + str(thread_id)):
        try:
            url_day   = url + date[:4] + '/' + date[4:6] + '/' + date[6:8] + '/'
            page      = requests.get(url_day)
            soup      = BeautifulSoup(page.content, 'html.parser')
            if not os.path.isdir(path): os.makedirs(path)
            if '404 Not Found' not in soup:
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
                        gz_to_png(fname_disk, num_splits)
            else:
                print('No data available for', date)
        except:
            print('No data available for', date)
            continue


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

def gz_to_png(file_name, num_splits):
    with gzip.open(file_name + '.fit.gz', 'rb') as fin:
        with fits.open(fin) as fitfile:
            try:
                # READ AND GET FILE INFORMATION
                img   = fitfile['PRIMARY'].data.astype(np.uint8)
                img   = img[:-10]  # REMOVE WHITE MARKS AT THE BOTTOM OF THE IMG
                freqs = fitfile[1].data['Frequency'][0]
                times = fitfile[1].data['Time'][0]
                img   = (img - img.mean(axis=1, keepdims=True)) / img.std(axis=1, keepdims=True)
                fitfile.close()

                if num_splits !=0:
                #IF SPLIT IMGS
                    split_img    = np.array_split(img, axis=1, indices_or_sections=num_splits)
                    imgs_to_plot = split_img

                    # CREATE PNG WITHOUT PLOTING ON WINDOW
                    for i, img in enumerate(imgs_to_plot):
                        plt.ioff()  # Avoid plotting on window, so we save resources
                        plt.axis('off')
                        plt.imshow(img, aspect='auto', extent=(times[0], times[-1], freqs[-1], freqs[0]), cmap=cm.CMRmap, vmin=0, vmax=12)
                        plt.savefig(file_name + '_' + str(i+1) + '.png', bbox_inches='tight', pad_inches=0.0)
                        plt.close()


                else:
                #IF FULL IMG
                    plt.ioff()  # Avoid plotting on window, so we save resources
                    plt.axis('off')
                    plt.imshow(img, aspect='auto', extent=(times[0], times[-1], freqs[-1], freqs[0]), cmap=cm.CMRmap,  vmin=0, vmax=12)
                    # plt.imshow(img, aspect='auto', extent=(times[0], times[-1], freqs[-1], freqs[0]), cmap=cm.CMRmap)
                    plt.savefig(file_name + '.png', bbox_inches='tight', pad_inches=0.0)
                    plt.close()
                os.remove(file_name + '.fit.gz')
            except:
                print('Error downloading ', file_name)
