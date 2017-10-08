import pytesseract
# import segmentation
import cca
from PIL import Image
from skimage import io
from skimage import img_as_int

# print cca.plate_like_objects[0]
image_name = "tmp2.jpg"
io.imsave(image_name, img_as_int(cca.plate))
# Image.open('tmp1.png')
# pytesseract.pytesseract.tesseract_cmd = ''
print pytesseract.image_to_string(Image.open(image_name))
