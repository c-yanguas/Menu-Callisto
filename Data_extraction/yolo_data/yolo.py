import os
import shutil
import subprocess
from yolo_data import yolo_image_generator
from yolo_data import yolo_report_generator


# 1-Generate txt from dir with imgs
def clean_dir(path_imgs):
    fnames = [f for f in os.listdir(path_imgs) if f.endswith('.txt')]
    for fname in fnames:
        os.remove(path_imgs + fname)


def generate_test_dataset_files_from_dir(path_imgs, path_dest_txt):
    """Generate txt with paths to imgs from dir with imgs"""
    if os.path.isdir(path_dest_txt): shutil.rmtree(path_dest_txt)
    os.makedirs(path_dest_txt)
    fnames = [f for f in os.listdir(path_imgs) if f.endswith('.png')]
    with open(path_dest_txt + '.txt', mode='w') as test_file:
        for fname in fnames:
            test_file.write(path_imgs + fname[:-4] + '.png' + '\n')


def make_yolo_predictions(txt_with_path_to_imgs):
    subprocess.check_call(["./yolo.sh", txt_with_path_to_imgs], shell=True)


def make_predictions(dir_imgs, generate_imgs_with_boxes=1, generate_report=1):
    path_pred_file = '/predictions/probando/predictions'
    clean_dir(dir_imgs)
    generate_test_dataset_files_from_dir(dir_imgs, path_pred_file)
    make_yolo_predictions(path_pred_file)
    if generate_imgs_with_boxes:
        yolo_image_generator.generate_imgs_with_boxes(path_pred_file, dir_imgs, 'predictions/imgs/')
    if generate_report:
        yolo_report_generator.generate_report_from_prediction_file(path_pred_file, 'predictions/report/report.txt', img_width=496)

