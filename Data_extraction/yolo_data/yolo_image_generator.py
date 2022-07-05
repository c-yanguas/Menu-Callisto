import os
import shutil
import re
from multiprocessing.dummy import freeze_support

from tqdm import tqdm
from multiprocessing import Pool
from itertools import repeat
import cv2
import matplotlib
import matplotlib.pyplot as plt
matplotlib.use('Agg')

def threads_managements(tasks):
    threads = os.cpu_count()
    k, m    = divmod(len(tasks), threads)
    return list((tasks[i * k + min(i, m):(i + 1) * k + min(i + 1, m)] for i in range(threads)))


def coordinates_on_bounds(coordinate, width_image, height_image, is_label=0):
    """
    This function makes coordinates to stay on bounds
    """
    x, y = coordinate
    x = 0 if x < 0 else x
    x = width_image if x > width_image else x
    y = 0 if y < 0 else y
    y = height_image if y > height_image else y
    if is_label and y == 0:
        y = y + 15
    return (x, y)


def draw_text_with_background(img, text,
                              font=cv2.FONT_HERSHEY_PLAIN,
                              pos=(0, 0),
                              font_scale=1,
                              font_thickness=1,
                              text_color=(0, 255, 0),
                              text_color_bg=(0, 0, 0)
                              ):
    x, y = pos
    text_size, _ = cv2.getTextSize(text, font, font_scale, font_thickness)
    text_w, text_h = text_size
    cv2.rectangle(img, pos, (x + text_w, y + text_h), text_color_bg, -1)
    cv2.putText(img, text, (x, y + text_h + font_scale - 1), font, font_scale, text_color, font_thickness)

    # return text_size


def generate_img_with_boxes(boxes_coordinates, dir_original_imgs, path_dest_imgs_with_boxes, fname):
    img = cv2.imread(dir_original_imgs + fname)
    img = cv2.cvtColor(img, cv2.COLOR_BGR2RGB)
    height, width = img.shape[:-1]
    # print('IMAGE SIZE:', img.shape)
    # img = PIL.Image.open(dir_imgs + fname + '.png')
    for box in boxes_coordinates:
        c, l, t, r, b = map(int, box.split(' '))  # confidence, left, top, right, bottom
        point_1 = coordinates_on_bounds((l, t), width, height)
        point_2 = coordinates_on_bounds((r, b), width, height)
        label_pos = coordinates_on_bounds((l, t - 10), width, height, is_label=1)
        # Draw the rectangle to remark the object detected
        img = cv2.rectangle(img, (point_1), (point_2), (0, 255, 0), 1)
        # Draw the text description of the object detected with background to visualize it better
        draw_text_with_background(img, str(c) + '%', pos=(label_pos))
        # cv2.putText(img, str(c) + '%', (label_pos), cv2.FONT_HERSHEY_SIMPLEX, 0.4, (36,255,12), 1)
    plt.imshow(img)
    plt.axis('off')
    plt.savefig(path_dest_imgs_with_boxes + 'SB/' + fname, bbox_inches='tight', pad_inches=0.0, dpi=150)


def obtain_boxes(lines):
    """
    INITIAL FORMAT
    Burst: 24%	(left_x:  148   top_y:  -25   width:   37   height:  351)
    RECTANGLE FORMAT
                  IMG  PUNTO_1   PUNTO_2   COLOR      ANCHO_RECTANGULO
    cv2.rectangle(img, (x1, y1), (x2, y2), (255,0,0), 2)
    RETURN FORMAT
    ['CONFIDENCE LEFT TOP (LEFT + WIDTH) (TOP + HEIGHT)', 'CONFIDENCE LEFT TOP (LEFT + WIDTH) (TOP + HEIGHT)', ...]
    """
    boxes = []

    clear_lines = []
    next_line_is_box = True
    next_line = 1
    while next_line_is_box:
        clear_lines.append(lines[next_line])
        next_line = next_line + 1
        next_line_is_box = lines[next_line].startswith('Burst')
    for line in clear_lines:
        confidence = int(re.search('Burst: ' + '(.*)%', line).group(1))
        left = int(re.search('left_x:  ' + '(.*)   top_y', line).group(1))
        top = int(re.search('top_y:  ' + '(.*)   width', line).group(1))
        width = int(re.search('width:  ' + '(.*)   height', line).group(1))
        height = int(re.search('height:  ' + '(.*)\)', line).group(1))
        box = str(confidence) + ' ' + str(left) + ' ' + str(top) + ' ' + str(left + width) + ' ' + str(
            top + height) + '\n'
        boxes.append(box)
        # print('{} {} {} {} {}'.format(confidence, left, top, width, height))
    return boxes


def generate_imgs_from_pred_file(bursts_already_created, lines, dir_original_imgs, init_path_files, path_dest_imgs_with_boxes,
                                 thread_id):

    for i in tqdm(range(len(lines)), desc='Generating pngs from YOLO_V4 predictions file with thread ' + str(thread_id)):
        if lines[i].startswith(init_path_files):
            fname = (init_path_files + re.search(init_path_files + '(.*).png', lines[i]).group(1)).split('/')[
                        -1] + '.png'
            if lines[i + 1].startswith('Burst') and fname not in bursts_already_created:
                boxes = obtain_boxes(lines[i:])  # This transform to rectangle opencv coordinates
                generate_img_with_boxes(boxes, dir_original_imgs, path_dest_imgs_with_boxes, fname)

            # This is only for move normal data, only SB data is desired, so we generate less images
            # else:
            #     try:
            #         shutil.copy2(dir_original_imgs + fname + '.png', path_dest_imgs_with_boxes + 'NSB/')
            #     except Exception as e:
            #         print(e)

def generate_imgs_with_concurrence(yolo_prediction_file, dir_original_imgs, init_path_files, path_dest_imgs_with_boxes,
                                 new_execution=0):
    if os.path.isdir(path_dest_imgs_with_boxes):
        if new_execution:
            shutil.rmtree(path_dest_imgs_with_boxes)
    else:
        os.makedirs(path_dest_imgs_with_boxes + 'NSB/')
        os.makedirs(path_dest_imgs_with_boxes + 'SB/')

    bursts_already_created = os.listdir(path_dest_imgs_with_boxes + 'SB/')
    threads                = os.cpu_count()
    threads_id             = list(range(threads))

    with open(yolo_prediction_file, 'r') as nasty_file:
        lines = nasty_file.read().splitlines()

    lines_per_thread       = threads_managements(lines)

    with Pool(threads) as executor:
        executor.starmap(generate_imgs_from_pred_file, zip(bursts_already_created, lines_per_thread,
                                                           repeat(dir_original_imgs), repeat(init_path_files),
                                                           repeat(path_dest_imgs_with_boxes), threads_id)
                         )



def generate_imgs_with_boxes(prediction_file, dir_orig_imgs, dest_imgs_with_boxes):
    yolo_prediction_file      = prediction_file
    dir_original_imgs         = dir_orig_imgs
    init_path_files           = dir_orig_imgs
    path_dest_imgs_with_boxes = dest_imgs_with_boxes
    generate_imgs_with_concurrence(yolo_prediction_file, dir_original_imgs, init_path_files, path_dest_imgs_with_boxes,
                                   new_execution=0)


# if __name__ == '__main__':
#     """Generate pngs from original imgs and pred YOLO file obtained from Colab"""
#     freeze_support()
#     generate_img_with_boxes(yolo)
#     # yolo_prediction_file      = 'yolo_predictions_files/pred_all_stations.txt'
#     # dir_original_imgs         = '../../../Data/Data_all_stations_NSB_files/'
#     # init_path_files           = '/content/drive/MyDrive/E-Callisto/YOLO/e-callisto/tests_imgs/test_dataset_all_stations/'
#     # path_dest_imgs_with_boxes = 'results/imgs_with_boxes_test_all_stations/'
#     # generate_imgs_with_concurrence(yolo_prediction_file, dir_original_imgs, init_path_files, path_dest_imgs_with_boxes,
#     #                                new_execution=0)




