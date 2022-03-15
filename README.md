# Menu-Callisto
 ## Menu for downloading data from the E-Callisto network
This is a menu that allows you to select multiple options for downloading data from [E-callisto](http://www.e-callisto.org/).
The CALLISTO spectrometer is a programmable heterodyne receiver designed 2006 in the framework of IHY2007 and ISWI by Christian Monstein (PI) as member of the former Radio Astronomy Group (RAG) at ETH Zurich, Switzerland.
This menu offers the following possibilities:
1. Show available stations
2. Download one year of data for specific station
3. Download all data for specific station
4. Download all data for all stations
5. Customize time lapse and stations
6. Download Solar bursts from 01/01/2020 to 15/02/2022
7. Exit

If [2-6] option is selected it offers the following file formats for downloading: ```.fit, .gz, .npy``` and ```.png``` with high contrast for downloading.
Next, depending on the option selected, you will need to specify the instrument for which you wish to obtain the data and a start and end year. Finally it will also ask if you want to download the solar events also for that station (If you are developing an AI project this is very interesting to create a dataset with events thanks to option 6, and others without events thanks to option 5).

Next, depending on the option selected, you will need to specify the season for which you wish to obtain the data and a start and end year. Finally it will also ask if you want to download the solar events also for that station (If you are developing an AI project this is very interesting to create a dataset with events thanks to option 6, and others without events thanks to option 5).

In case you choose option 6, you are asked if you want to download the solar events reported in [solar events list](http://soleil.i4ds.ch/solarradio/data/BurstLists/2010-yyyy_Monstein/) with duration longer than 15 minutes. This is because if the duration of the solar event is longer than 15 minutes, i.e. one image, there are usually intermediate images in which there are no solar events.

**Example original image vs downloaded image**
<p float="left" align="center">
  <img src="https://user-images.githubusercontent.com/95175204/158347644-71f0cc70-d2ee-4035-8615-863779fc0f27.png" width=350  />
  <img src="https://user-images.githubusercontent.com/95175204/158347712-93ff825f-b1e3-4df4-95ad-4361df7ec2bd.png" width=420 /> 
</p>

**NOTE**: Due to the white stripes at the bottom of the image, the last 10 frequencies are eliminated. Another thing to take into account is that the frequencies on the web page are inverted, that is to say, they are shown from more to less (see Y axis), however, at the time of downloading they are formed from less to more. 

<p float="left" align="center">
  <img src="https://user-images.githubusercontent.com/95175204/158347161-c0f9c491-9e21-437a-9f8f-573a00ba42e6.png"  />
  <img src="https://user-images.githubusercontent.com/95175204/158348403-429ff350-6ed8-493d-84fb-40947cddb700.png"  /> 
</p>


**Other considerations**: This program makes use of concurrency to optimize download speeds, since there are many read and write operations on disk. It is not recommended to use heavy programs while downloading data.
