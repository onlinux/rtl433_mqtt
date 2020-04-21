#! /home/pi/python/rtl433venv/venv/bin/python
# -*- coding: utf-8 -*-
#
# Author: Eric Vandecasteele (c)2014
# http://blog.onlinux.fr
#
#
# {"time" : "2016-08-29 17:57:35", "model" : "Proove",
#   "id" : 19569639, "channel" : 3, "state" : "ON", "unit" : 2}
#
# Import required Python libraries
import os
import sys
import time
from subprocess import PIPE, Popen, STDOUT
import logging
import threading
import MySQLdb as mdb
import json
import requests
import paho.mqtt.client as mqtt
import ZiBase
#from socket import *
import socket
import signal
import ConfigParser

LOGFORMAT ="%(asctime)s %(levelname)s:%(message)s"
CONFIG_INI = "config.ini"
# os.path.realpath returns the canonical path of the specified filename,
# eliminating any symbolic links encountered in the path.
path = os.path.dirname(os.path.realpath(sys.argv[0]))
configPath = path + '/' + CONFIG_INI
config = ConfigParser.RawConfigParser()
config.read(configPath)

try:
        config.read(configPath)

except BaseException:
        config = None


# Read config.ini
DEBUG = config.getboolean("global", "debug")
MQTT_USER = config.get("secret","mqtt_user")
MQTT_PASSWORD = config.get('secret', "mqtt_password")
MQTT_BROKER_IP = config.get('secret', "mqtt_broker_ip")
MQTT_BROKER_PORT = config.get('secret', "mqtt_broker_port")
ZIBASE_IP = config.get('secret', "zibase_ip")

if DEBUG:
    logging.basicConfig(format=LOGFORMAT,
                    filename="/var/log/rtl433.log",
                    level=logging.DEBUG)
else:
    logging.basicConfig(format=LOGFORMAT,
                    filename="/var/log/rtl433.log",
                    level=logging.INFO)

logging.info(" MQTT_BROKER_IP is " + MQTT_BROKER_IP + " port " + str(MQTT_BROKER_PORT))
logging.info(" MQTT_USER is " + MQTT_USER )

cmd = ['/usr/local/bin/rtl_433','-M', 'newmodel', '-F', 'json', '-R',
       '3', '-R', '12', '-R', '19', '-R', '50', '-R', '4', '-R', '96']



def handler(signum=None, frame=None):
    logging.info(' Signal handler called with signal ' + str(signum))

for sig in [signal.SIGUSR1, signal.SIGUSR2]:
    signal.signal(sig, handler)

cs = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
cs.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
cs.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)

lastDate = 0
lastId = -1


class UpdateDb(threading.Thread):
    def __init__(self, data):
        threading.Thread.__init__(self)
        self.data = data

    def run(self):
        try:

            con = mdb.connect('localhost', 'user', 'nopassword', 'base')
            cur = con.cursor(mdb.cursors.DictCursor)

            if ('time' in data.keys() and 'temperature_C' in self.data.keys()):
                #x = true_value if condition else false_value
                hum = int(self.data['humidity']) if self.data.has_key(
                    'humidity') else 0

                cur.execute("INSERT INTO ors (rc, temperature, humidity) VALUES(%s, %s, %s)",
                            ("%x" % self.data['id'], self.data['temperature_C'], hum))
                #print self.data['time'], self.data['name'], self.data['id'], self.data['temperature_C'], self.data['humidity']

            if ('power' in self.data.keys() and 'energy_kWh' in self.data.keys() and float(self.data['energy_kWh']) < 5000.0 and int(self.data['power']) < 7000):

                cur.execute("INSERT INTO owl (chan1, cumul) VALUES( %s, %s)",
                            (self.data['power_W'], self.data['energy_kWh']))

            con.commit()
            con.close()
        except:
            logging.warning(' %s %s' %
                            (threading.current_thread(), sys.exc_info()[0]))


class UpdateCube(threading.Thread):
    def __init__(self, data):
        threading.Thread.__init__(self)
        self.data = data

    def run(self):
        try:
            if (self.data['id'] == 217 and 'time' in self.data.keys() and 'temperature_C' in self.data.keys() and 'humidity' in self.data.keys()):
                payload = {'temp': self.data['temperature_C'], 'hum': self.data['humidity'],
                           'id': self.data['id'], 'battery': self.data['battery']}
                #r = requests.get("https://localhost:3001/atmo", params=payload, timeout=5, config={'danger_mode': True}, verify=False)
                session = requests.Session()
                session.get("http://localhost:3000/atmo",
                            params=payload, verify=False)

        except ValueError:
            print "Oops!  UpdateCube Error"


class MqttPublish(threading.Thread):
    def __init__(self, mqttc, data):
        threading.Thread.__init__(self)
        self.data = data

    def run(self):
        try:
            mqttc.publish("onlinux/31830", self.data.rstrip('\r\n'), 0, True)

        except:
            mqttc.connect(MQTT_BROKER_IP, MQTT_BROKER_PORT)
            logging.warning("%s Trying to reconnect once %s" %
                            (threading.current_thread(), sys.exc_info()[0]))


class UpdateZibase(threading.Thread):
    def __init__(self, data):
        threading.Thread.__init__(self)
        self.data = data

    def run(self):
        try:
            power = int(round(int(self.data['power_W']) / 100.0))
            total = int(round(float(self.data['energy_kWh']) * 10.0))
            #zibase.setVirtualProbe( 491234567, total, power, 20, 0)
            zibase.setVirtualProbe(131076, total, power, 20, 0)
        except:
            logging.warning(' %s %s' %
                            (threading.current_thread(), sys.exc_info()[0]))


# class UpdateZibaseTemp(threading.Thread):
#     def __init__(self, data):
#         threading.Thread.__init__(self)
#         self.data = data
#
#     def run(self):
#         try:
#             #print 'UpdateZibaseTemp data: ', data
#             #temp = -190
#             temp = int(round(float(self.data['temperature_C']) * 10.0))
#             hum = int(self.data['humidity']) if data.has_key('humidity') else 0
#             batt = 0
#
#             #payload = '{"idx": "1", "svalue": "{};{};0" }'.format(self.data['temperature_C'], hum)
#             # logging.warning(payload)
#             #mqtt.publish("domoticz/in", payload, 0, True) ;
#
#             if ('battery' in self.data.keys() and self.data['battery'].upper() != 'OK'):
#                 batt = 1  # 1 means Low batt
#             #print 'temp: ', temp , 'hum: ' , hum, 'batt: ', batt
#             logging.debug('Probe Id: %d' % self.data['id'])
#
#             #if self.data['id'] == 5:  # freezer
#                 #zibase.setVirtualProbe(439204611, temp, hum, 17, batt)
#             #elif self.data['id'] == 131:  # sonde auriol ext - Id 0x83 OS3930897409
#                 #zibase.setVirtualProbe(3930897409, temp, hum, 17, batt)
#             #elif self.data['id'] == 63:  # sonde auriol ext - Id 63
#                 #zibase.setVirtualProbe(439204622, temp, hum, 17, batt)
#         except:
#             logging.warning(' %s %s' %
#                             (threading.current_thread(), sys.exc_info()[0]))

def on_log(mosq, obj, level, string):
    """
    What to do with debug log output from the MQTT library
    """
    logging.debug(string)

# -------------------------------------------------------------------------
#   We're using a queue to capture output as it occurs
try:
    from Queue import Queue, Empty
except ImportError:
    from queue import Queue, Empty  # python 3.x
ON_POSIX = 'posix' in sys.builtin_module_names


def enqueue_output(src, out, queue):
    for line in iter(out.readline, b''):
        queue.put((src, line))
    out.close()


p = Popen(cmd, stdout=PIPE, stderr=STDOUT, bufsize=1, close_fds=ON_POSIX)
q = Queue()

t = threading.Thread(target=enqueue_output, args=('stdout', p.stdout, q))
t.daemon = True  # thread dies with the program
t.start()

zibase = ZiBase.ZiBase(ZIBASE_IP)  # Indiquer l'adresse IP de la zibase

# Connection to mqtt broker
mqttc = mqtt.Client("python_pub")
mqttc.username_pw_set(MQTT_USER, MQTT_PASSWORD)
if DEBUG:
        mqttc.on_log = on_log
        
try:
    result = mqttc.connect(MQTT_BROKER_IP, int(MQTT_BROKER_PORT), keepalive=60)
    if result != 0:
        logging.info(" Connection failed with error code %s. Retrying", result)
        time.sleep(10)
    else :
        logging.info(" Connection to mqtt broker "+ MQTT_BROKER_IP + " OK");
except:
    logging.warning("Oops!  Mqtt connection Error")

pulse = 0
while 1:
    try:
        if not mqttc.socket():
            logging.warning("ERROR: connection failed. Please check user and password of mqtt broker, re-trying...")
            mqttc.connect(MQTT_BROKER_IP, int(MQTT_BROKER_PORT), keepalive=60)
    except:
        logging.warning("Oops!  Mqttconnect Error")
    try:
        src, line = q.get(timeout=10)
    except Empty:
        pulse += 1
        logging.debug(" q.get line empty pulse %s" % pulse)
    else:  # got line
        logging.debug(" Received " + line.strip())
        pulse -= 1
        if (line.find('exiting') != -1):
            logging.warning(' >>>---> ERROR, exiting...')
            exit(1)

        try:
            data = json.loads(line)

            if ('time' in data.keys()):

                if (lastDate != data['time'] or lastId != data['id']):

                    if ('power_W' in data.keys() and int(data['power_W']) > 6000):
                        logging.warning(
                            "Ouille! consommation électrique élévée!", line.strip())
                    else:
                        # broadcast line on LAN
                        cs.sendto(line, ('192.168.0.255', 5000))
                        # Arrêt alimentation base mysql le 6 sep 2019
                        # UpdateDb(data).start()
                        UpdateCube(data).start()
                        MqttPublish(mqttc, line).start()

                    lastDate = data['time']
                    lastId = data['id']

                #if ('power_W' in data.keys() and int(data['power_W']) < 7000 and 'energy_kWh' in data.keys()):
                    #UpdateZibase(data).start()

                ## Probe Auriol outdoor id#131
                ## Probe Nexus Freezer id#5
                #if (data['id'] == 5 or data['id'] == 131 or data['id'] == 63):
                    #UpdateZibaseTemp(data).start()

        except ValueError:
            logging.warning(
                " Oops!  That was no valid json line. " + line.strip())
