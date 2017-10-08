import pymysql

connection = pymysql.connect(host='10.1.26.127',
                             user='r2d2',
                             password='12345678',
                             db='safepath',
                             charset='utf8mb4',
                             cursorclass=pymysql.cursors.DictCursor)

def insert(car_reg_state, car_reg_num, vehicle_num):
    with connection.cursor() as cursor:
        # Create a new record
        sql = "INSERT INTO  incident_report_video (CAR_REG_STATE, CAR_REG_NUM, VEHICLE_NUM) VALUES (%s, %s, %s)"
        cursor.execute(sql, (car_reg_state, car_reg_num, vehicle_num))

    connection.commit()
    print ("DB entry success..")