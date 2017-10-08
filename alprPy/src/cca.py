from skimage import measure
from skimage.measure import regionprops
import matplotlib.pyplot as plt
import matplotlib.patches as patches
import localization

label_image = measure.label(localization.binary_car_image)

# props to trace out the number plate region
plate_dimensions = (
0.10 * label_image.shape[0], 0.5 * label_image.shape[0], 0.15 * label_image.shape[1], 0.73 * label_image.shape[1])

min_height, max_height, min_width, max_width = plate_dimensions

width_height_ratio = 1.0

plate_objects_cordinates = []
plate_like_objects = []

fig, (ax1) = plt.subplots(1)
ax1.imshow(localization.gray_car_image, cmap="gray")

plate_rows = 99999
for region in regionprops(label_image):

    # ignore the small area regions
    if region.area < 50:
        continue

    min_row, min_col, max_row, max_col = region.bbox
    region_height = max_row - min_row
    region_width = max_col - min_col
    # ensuring that the region identified satisfies the condition of a typical license plate
    if region_height >= min_height and region_height <= max_height and region_width >= min_width and region_width <= max_width and region_width > region_height * width_height_ratio:



        plate_candidate = localization.binary_car_image[min_row:max_row, min_col:max_col]
        if len(plate_candidate) < plate_rows:
            plate = plate_candidate
            plate_rows = len(plate_candidate)

plate_like_objects.append(localization.binary_car_image[min_row:max_row,
                          min_col:max_col])

plate_objects_cordinates.append((min_row, min_col,
                                 max_row, max_col))
rectBorder = patches.Rectangle((min_col, min_row), max_col - min_col, max_row - min_row, edgecolor="red",
                               linewidth=2, fill=False)
print "adding rect"
ax1.add_patch(rectBorder)

plt.show()
