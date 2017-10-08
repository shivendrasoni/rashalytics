import requests
import json
import re

def encode_img(path):
    with open(path, "rb") as f:
        data = f.read()
        return data.encode("base64")

def get_number_plate(file_path):
    headers = {'Content-Type':'application/json'}
    API_KEY = "AIzaSyD31MtkgmXlwc9fFTIoEwNi6W2AzcrHEHc"
    url = "https://vision.googleapis.com/v1/images:annotate?key="+API_KEY
    enc_img = encode_img("../images/img2.png")
    data={}
    data['requests'] = [{'image':{'content':str(enc_img)},'features':[{'type':"TEXT_DETECTION"}]}]


    json_data = json.dumps(data)
    resp = requests.post(headers=headers, url=url, data=json_data)
    json_resp = resp.json()
    all_text = json_resp['responses'][0]['fullTextAnnotation']['text']
    text_strings = all_text.split("\n")
    plate_text = None
    plate_regex = "^[a-zA-Z0-9].+\d{4}$"
    print text_strings
    for i in range(len(text_strings)):
        found = re.search(plate_regex, text_strings[i])
        if found:
            plate_text = found.group()

    if plate_text == None:
        raise ValueError('Unable to find the number plate text')
    return plate_text