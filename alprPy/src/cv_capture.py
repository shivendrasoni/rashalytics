import cv2


def capture_img(filepath):
    camera_port = 0
    ramp_frames = 30
    camera = cv2.VideoCapture(camera_port)

    retval, im = camera.read()

    for i in xrange(ramp_frames):
        temp = camera.read()

    # camera_capture = get_image()
    # filename = "cam_test.png"
    # cv2.imwrite(filename,camera_capture)
    del (camera)
