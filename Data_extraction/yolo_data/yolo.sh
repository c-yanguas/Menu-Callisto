#!/bin/sh

#1-PATH_SAVE_PREDICTIONS
#2-PATH_FILES
#3-PATH_WEIGHTS
#4-PATH_DATA
#5-PATH_CFG


cd ../darknet
./darknet detector test /yolo_data/image_data.data cfg/yolov4_test.cfg yolo_data/weights/sb.weights -dont_show -ext_output -save_labels < $1 >> predictions/last_prediction.txt




