# Menu-Callisto #

**Contributors:** The main contributor to this project is [Carlos Yanguas](https://github.com/c-yanguas). Mario Fernández provide the high contrast png generation code.

**INSTALLATION:** Move to the directory where you have downloaded this repository, move to Data_extraction directory, open a terminal/cmd and run ```pip install -r requirements.txt```

**Description:** This is a menu that allows you to select multiple options for downloading data from [E-callisto](http://www.e-callisto.org/).
The CALLISTO spectrometer is a programmable heterodyne receiver designed 2006 in the framework of IHY2007 and ISWI by Christian Monstein (PI) as member of the former Radio Astronomy Group (RAG) at ETH Zurich, Switzerland.


## Options Available ##
This menu offers the following possibilities:

1. ```Show available stations```

2. ```Download one year of data for specific station```

3. ```Customize time lapse and stations```

4. ```Download Solar bursts reported since 01/01/2020```

5. ```Download all data for specific station```

6. ```Download customize for all stations```

7. ```Update Solar burst database```

8. ```Exit```

## First steps ##
In order to execute the Menu, you should move to Data_extraction directory and execute main.py, if you want to do it with a terminal/cmd you can just write python main.py.
Next, the first recommended step is to select option 7 to update the solar_burst_data.xlsx and solar_burst_file_names.xlsx files. In this way, the data related to the solar events will be updated, so that the requests to download them or to avoid them in case of wanting to download only data without solar events will be properly handled.

## Extended description ##
If [2, 3, 5, 6] option is selected it offers the following file formats for downloading: ```.fit, .gz, .npy``` and ```.png``` with high contrast for downloading.
Next, depending on the option selected, you will need to specify the instrument/station for which you wish to obtain the data and a start and end date. Finally it will also ask if you want to download the solar events also for that station selected (If you are developing an AI project this is very interesting to create a dataset with events thanks to option 6, and others without events thanks to this option).


In case you choose option 4, you are asked if you want to download the solar events reported in [solar events list](http://soleil.i4ds.ch/solarradio/data/BurstLists/2010-yyyy_Monstein/) with duration longer than 15 minutes. This is because if the duration of the solar event is longer than 15 minutes, i.e. one image, there are usually intermediate images in which there are no solar events. Please also note that since splitting the images is also offered, only the split pertaining to the solar event will be downloaded. For example, if splitting by 15 is specified and a file starts at 01:30:00 and contains a solar event reported as 01:34-01:36, only the image fractions [4, 5, 6] will be downloaded. In case of not splitting the image, the whole image will be downloaded.

**Example original image vs downloaded image**
<p float="left" align="center">
  <img src="https://user-images.githubusercontent.com/95175204/158347712-93ff825f-b1e3-4df4-95ad-4361df7ec2bd.png" width=420 /> 
  <img src="https://user-images.githubusercontent.com/95175204/158347644-71f0cc70-d2ee-4035-8615-863779fc0f27.png" width=350  />
</p>

**NOTE**: Due to the white stripes at the bottom of the image, the last 10 frequencies are eliminated. Another thing to take into account is that the frequencies on the web page are inverted, that is to say, they are shown from more to less (see Y axis), however, at the time of downloading they are formed from less to more. 

<p float="left" align="center">
  <img src="https://user-images.githubusercontent.com/95175204/158347161-c0f9c491-9e21-437a-9f8f-573a00ba42e6.png"  />
  <img src="https://user-images.githubusercontent.com/95175204/158348403-429ff350-6ed8-493d-84fb-40947cddb700.png"  /> 
</p>


**Other considerations**: This program makes use of concurrency to optimize download speeds, since there are many read and write operations on disk. It is not recommended to use heavy programs while downloading data.



## Example of execution ##

![image](https://user-images.githubusercontent.com/95175204/160587095-0edd16e8-c970-447a-9200-c4e00e74155b.png)

**This work is part of [CELESTINA](https://celestina.web.uah.es), a project funded by the Junta de Comunidades de Castilla-La Mancha and the European Union (reference: SBPLY/19/180501/000237)**
