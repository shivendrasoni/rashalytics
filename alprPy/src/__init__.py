from flask import Flask
import cv_capture
import g_recognizer
import db_utils
import json
app = Flask(__name__)

@app.route("/capture")
def capture():
    img_file = "../images/test.png"
    cv_capture.capture_img(img_file)
    plate_text = g_recognizer.get_number_plate(img_file)
    plain_plate_text = plate_text.replace(" ","")
    car_reg_state = plain_plate_text[0:2]
    car_reg_num = plain_plate_text[-4:]
    db_utils.insert(car_reg_state, car_reg_num, plain_plate_text)
    resp = {'plate_text':plate_text}

    return json.dumps(resp)

if __name__=="__main__":
    app.run(debug=True, port=8687)