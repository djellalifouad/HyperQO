from yoctopuce.yocto_power import *
from datetime import datetime, timedelta
import time

from algos.helper_functions import connect_bdd, disconnect_bdd


def findPowerSensor(power_sensor_identifier):
    errmsg = YRefParam()
    # Setup the API to use local USB devices
    if YAPI.RegisterHub("usb", errmsg) != YAPI.SUCCESS:
        print("init error " + errmsg.value)
        return None

    psensor = YPower.FindPower(power_sensor_identifier);
    if not psensor.isOnline():
        print("Power Sensor " + psensor.get_hardwareId() + " is not connected (check identification and USB cable)")
        return None

    return psensor


def clearPowerMeterCache(power_sensor):
    # print("Cleaning powermeter memory ...\n")
    power_sensor.get_dataLogger().forgetAllDataStreams()


def startDataRecording(power_sensor):
    # print("RECORDING MEAUSURES ...\n")
    if not power_sensor.isOnline():
        power_sensor = findPowerSensor("YWATTMK1-17B6C4.power")
    power_sensor.startDataLogger()
    data_logger = power_sensor.get_dataLogger()
    while data_logger.get_recording() != 1:
        pass


def stopDataRecording(power_sensor):
    # print("Stop RECORDING MEAUSURES ...\n")
    if not power_sensor.isOnline():
        power_sensor = findPowerSensor("YWATTMK1-17B6C4.power")
    power_sensor.stopDataLogger()
    data_logger = power_sensor.get_dataLogger()
    while data_logger.get_recording() != 0:
        pass



def getAveragePower(psensor, startTime, endTime, plStartTime=-1, plEndTime=-1):

    start_date_time = datetime.strptime(startTime, '%Y-%m-%d %H:%M:%S:%f')
    end_date_time = datetime.strptime(endTime, '%Y-%m-%d %H:%M:%S:%f')
    if plStartTime == -1:
        start_timestamp = time.mktime(start_date_time.timetuple())
        end_timestamp = time.mktime(end_date_time.timetuple())
    else:
        start_date_time += datetime.timedelta(seconds=plStartTime)
        start_timestamp = time.mktime(start_date_time.timetuple())
        end_date_time += datetime.timedelta(seconds=plEndTime)
        end_timestamp = time.mktime(end_date_time.timetuple())

    dataset = psensor.get_recordedData(start_timestamp, end_timestamp)

    # print("loading summary... ")
    dataset.loadMore()
    summary = dataset.get_summary()
    avg_power = summary.get_averageValue()
    print("avg_power : ", avg_power)
    if avg_power < 0:
        start_date_time += timedelta(seconds=1)
        start_timestamp = time.mktime(start_date_time.timetuple())
        end_date_time += timedelta(seconds=1)
        end_timestamp = time.mktime(end_date_time.timetuple())
        dataset = psensor.get_recordedData(start_timestamp, end_timestamp)
        dataset.loadMore()
        summary = dataset.get_summary()
        avg_power = summary.get_averageValue()
        print("avg_power : ", avg_power)

    exec_time = end_date_time - start_date_time
    exec_time_in_seconds = exec_time.total_seconds() / 1000
    exec_time_in_seconds = exec_time_in_seconds if exec_time_in_seconds > 0 else 1.0
    avg_energy = avg_power * exec_time_in_seconds

    print("Time(s): ", exec_time_in_seconds, " - AVG Power(w): ", avg_power, " - Energy(J): ", avg_energy)
    return (avg_power, exec_time_in_seconds, avg_energy)


def get_query_exec_energy(query):
    conn, cursor = connect_bdd("imdb")
    cursor.execute("load 'pg_hint_plan';")
    cursor.execute("SET statement_timeout = " + str(120000) + ";")
    try:
    # Prepare query
        query = "EXPLAIN Analyse " + query + ";"
    # Prepare sensor
        psensor = findPowerSensor("YWATTMK1-17B6C4.power")
        stopDataRecording(psensor)
        clearPowerMeterCache(psensor)
        tm = time.time()
        datalog = psensor.get_dataLogger()
        datalog.set_timeUTC(time.time())
        startDataRecording(psensor)  # Power Meter starts recording power per second
        time.sleep(2.0)
        psensor.get_dataLogger().get_recording()
    # execute query
        first_time = time.time()


        cursor.callproc('getQueryExecutionInfo', (query,))

        later_time = time.time()
        difference = later_time - first_time
        endExecTime = datetime.now().strftime("%Y-%m-%d %H:%M:%S:%f")
        result = cursor.fetchone()
        result = result[0].split(";")
        (startTime, executionPlan, endTime) = (result[0].replace(".", ":"), result[1], endExecTime)

    # print("4-4 - is recording: ", psensor.get_dataLogger().get_recording())

    #   YAPI.Sleep(2000)
        time.sleep(2.0)
        # print(datetime.now())
        stopDataRecording(psensor)
        psensor.get_dataLogger().get_recording()
        (power, exec_time, energy) = getAveragePower(psensor, startTime, endTime)
        disconnect_bdd(conn)
        return (power, difference, energy)
    except Exception as e:
        print(e)
        return float('inf'),float('inf'),float('inf')






