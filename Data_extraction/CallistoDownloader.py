"""
AUTHOR: Carlos Yanguas
GITHUB: https://github.com/c-yanguas
"""

import requests
import os
import urllib.request
from bs4 import BeautifulSoup
from tqdm import tqdm
import utils

url = 'http://soleil80.cs.technik.fhnw.ch/solarradio/data/2002-20yy_Callisto/'





def download(unique_dates, instrument, extension, file_burst_names, path, num_splits, thread_id):
    """
    MAIN FUNCTION
    lo suyo seria pasar unique dates aqui tambien
    """

    # initializing
    if not os.path.isdir(path): os.makedirs(path)
    for date in tqdm(unique_dates, desc='THREAD ' + str(thread_id)):
        try:
            url_day   = url + date[:4] + '/' + date[4:6] + '/' + date[6:8] + '/'
            page      = requests.get(url_day)
            soup      = BeautifulSoup(page.content, 'html.parser')
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
                        utils.gz_to_fit(fname_disk)
                    elif extension == '.npy':
                        utils.gz_to_npy(fname_disk)
                    elif extension == '.png':
                        utils.gz_to_png(file_name=fname_disk, num_splits=num_splits, solar_burst=0)
        except Exception as e:
            print(e)
            continue


