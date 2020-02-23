# -*- coding: utf-8 -*-
################################################################################
# Librairie Python pour la ZiBase
# Auteur : Benjamin GAREL
# Version : 1.6.0
# Mars 2011
################################################################################

from array import array
import socket
import urllib
import struct
import xml.dom.minidom
from datetime import datetime


def dec2bin(n):
    if n == 0:
        return ''
    else:
        return dec2bin(n / 2) + str(n % 2)


class ZbProtocol:
    """ Protocoles compatibles Zibase """
    PRESET = 0
    VISONIC433 = 1
    VISONIC868 = 2
    CHACON = 3
    DOMIA = 4
    X10 = 5
    ZWAVE = 6
    RFS10 = 7
    X2D433 = 8
    X2D868 = 9


class ZbAction:
    """ Action possible de la zibase """
    OFF = 0
    ON = 1
    DIM_BRIGHT = 2
    ALL_LIGHTS_ON = 4
    ALL_LIGHTS_OFF = 5
    ALL_OFF = 6
    ASSOC = 7


def createZbCalendarFromInteger(data):
    """ Créer un objet ZbCalendar à partir d'un entier venant de la zibase    
    """
    cal = ZbCalendar()
    for i in range(24):
        cal.hour[i] = (data & (1 << i)) >> i

    cal.day["lundi"] = (data & (1 << 24)) >> 24
    cal.day["mardi"] = (data & (1 << 25)) >> 25
    cal.day["mercredi"] = (data & (1 << 26)) >> 26
    cal.day["jeudi"] = (data & (1 << 27)) >> 27
    cal.day["vendredi"] = (data & (1 << 28)) >> 28
    cal.day["samedi"] = (data & (1 << 29)) >> 29
    cal.day["dimanche"] = (data & (1 << 30)) >> 30
    return cal


class ZbCalendar(object):
    """Représente une variable calendrier de la zibase"""

    def __init__(self):
        self.hour = [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]
        self.day = {"lundi": 0, "mardi": 0, "mercredi": 0, "jeudi": 0, "vendredi": 0, "samedi": 0, "dimanche": 0}

    def toInteger(self):
        """ Retourne l'entier 32bits représentant ce calendrier        
        """
        data = 0x00000000
        for i in range(24):
            data |= self.hour[i] << i

        data |= self.day["lundi"] << 24
        data |= self.day["mardi"] << 25
        data |= self.day["mercredi"] << 26
        data |= self.day["jeudi"] << 27
        data |= self.day["vendredi"] << 28
        data |= self.day["samedi"] << 29
        data |= self.day["dimanche"] << 30
        return data


class ZbRequest(object):
    """ Représente une requête à la zibase """

    def __init__(self):
        self.header = bytearray("ZSIG")
        self.command = 0
        self.reserved1 = ''
        self.zibaseId = ''
        self.reserved2 = ''
        self.param1 = 0
        self.param2 = 0
        self.param3 = 0
        self.param4 = 0
        self.myCount = 0
        self.yourCount = 0
        self.message = ''

    def toBinaryArray(self):
        """ Serialize la requête en chaine binaire """
        buffer = array('B')
        buffer = self.header
        buffer.extend(struct.pack('!H', self.command))
        buffer.extend(self.reserved1.rjust(16, chr(0)))
        buffer.extend(self.zibaseId.rjust(16, chr(0)))
        buffer.extend(self.reserved2.rjust(12, chr(0)))
        buffer.extend(struct.pack('!I', self.param1))
        buffer.extend(struct.pack('!I', self.param2))
        buffer.extend(struct.pack('!I', self.param3))
        buffer.extend(struct.pack('!I', self.param4))
        buffer.extend(struct.pack('!H', self.myCount))
        buffer.extend(struct.pack('!H', self.yourCount))
        if len(self.message) > 0:
            buffer.extend(self.message.ljust(96, chr(0)))
        return buffer


class ZbResponse(object):
    """ Représente une réponse de la zibase """

    def __init__(self, buffer):
        """ Construction de la réponse à partir de la chaine binaire reçue """
        self.header = buffer[0:4]
        self.command = struct.unpack("!H", buffer[4:6])[0]
        self.reserved1 = buffer[6:22]
        self.zibaseId = buffer[22:38]
        self.reserved2 = buffer[38:50]
        self.param1 = struct.unpack("!I", buffer[50:54])[0]
        self.param2 = struct.unpack("!I", buffer[54:58])[0]
        self.param3 = struct.unpack("!I", buffer[58:62])[0]
        self.param4 = struct.unpack("!I", buffer[62:66])[0]
        self.myCount = struct.unpack("!H", buffer[66:68])[0]
        self.yourCount = struct.unpack("!H", buffer[68:70])[0]
        self.message = buffer[70:]


class ZiBase(object):
    """ Classe principale permettant de communiquer avec la ZiBase """

    def __init__(self, ip):
        """ Indiquer l'adresse IP de la ZiBase """
        self.ip = ip
        self.port = 49999

    def sendRequest(self, request):
        """ Envoi la requete à la zibase à travers le réseau """
        buffer = request.toBinaryArray()
        response = None
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.settimeout(5)
        s.connect((self.ip, self.port))
        s.send(buffer)
        ack = s.recv(512)
        if len(ack) > 0:
            response = ZbResponse(ack)
        s.close()
        return response

    def sendCommand(self, address, action, protocol=ZbProtocol.PRESET, dimLevel=0, nbBurst=1):
        """ Envoi d'une commande à la zibase """
        if len(address) >= 2:
            address = address.upper()
            req = ZbRequest()
            req.command = 11
            if action == ZbAction.DIM_BRIGHT and dimLevel == 0:
                action = ZbAction.OFF
            req.param2 = action
            req.param2 |= (protocol & 0xFF) << 0x08
            if action == ZbAction.DIM_BRIGHT:
                req.param2 |= (dimLevel & 0xFF) << 0x10
            if nbBurst > 1:
                req.param2 |= (nbBurst & 0xFF) << 0x18
            req.param3 = int(address[1:]) - 1
            req.param4 = ord(address[0]) - 0x41
            self.sendRequest(req)

    def runScenario(self, numScenario):
        """ Lance le scenario spécifié par son numéro.
            Le numéro du scenario est indiqué entre parenthèse
            dans le suivi d'activité de la console de configuration.      
        """
        req = ZbRequest()
        req.command = 11
        req.param1 = 1
        req.param2 = numScenario
        self.sendRequest(req)

    def getVariable(self, numVar):
        """ Récupère la valeur d'une variable Vx de la Zibase
            Numéro de variable compris entre 0 et 19
        """
        req = ZbRequest()
        req.command = 11
        req.param1 = 5
        req.param3 = 0
        req.param4 = numVar
        res = self.sendRequest(req)
        if res != None:
            return res.param1
        else:
            return None

    def getState(self, address):
        """ Récupère l'état d'un actionneur.
            La zibase ne reçoit que les ordres RF et non les ordres CPL X10,
            donc l'état d'un actionneur X10 connu par la zibase peut être erronné.
        """
        if len(address) > 0:
            address = address.upper()
            req = ZbRequest()
            req.command = 11
            req.param1 = 5
            req.param3 = 4

            houseCode = ord(address[0]) - 0x41
            device = int(address[1:]) - 1
            req.param4 = device
            req.param4 |= (houseCode & 0xFF) << 0x04

            res = self.sendRequest(req)
            if res != None:
                return res.param1
            else:
                return None

    def setVariable(self, numVar, value):
        """ Met à jour une variable zibase avec la valeur spécifiée
            variable comprise entre 0 et 19        
        """
        req = ZbRequest()
        req.command = 11
        req.param1 = 5
        req.param3 = 1
        req.param4 = numVar
        req.param2 = value & 0x0000FFFF
        self.sendRequest(req)

    def setVirtualProbe(self, id, value1, value2=0, type=17, batt=1):
        """
        Cette commande permet au système HOST d'envoyer dans ZiBASE une information de sonde virtuelle
        comme si celle-ci était reçue sur la RF.
        batt:
            1: Low batt
        Probe type: 
            17 : Scientific Oregon Type
            20 : OWL Type
        """
        req = ZbRequest()
        req.command = 11  # Virtual Probe Event
        req.param1 = 6
        req.param2 = id  # Sensor ID (e.g; 4196984322
        req.param4 = type
        binStr = "{0:b}".format(batt << 2 & 0xFF).zfill(8) + "{0:b}".format(value2 & 0xFF).zfill(8) + "{0:b}".format(value1 & 0xFFFF).zfill(16)
        req.param3 = int(binStr, 2)
        #req.param3 = value1
        #print binStr, req.param3
        self.sendRequest(req)

    def getCalendar(self, numCal):
        """ Récupère la valeur d'un calendrier dynamique de la Zibase
            Numéro de calendrier compris entre 1 et 16
        """
        req = ZbRequest()
        req.command = 11
        req.param1 = 5
        req.param3 = 2
        req.param4 = numCal - 1
        res = self.sendRequest(req)
        if res != None:
            return createZbCalendarFromInteger(res.param1)
        else:
            return None

    def setCalendar(self, numCal, calendar):
        """ Met à jour le contenu d'un calendrier dynamique de la zibase
            Numéro du calendrier compris entre 1 et 16
        """
        req = ZbRequest()
        req.command = 11
        req.param1 = 5
        req.param3 = 3
        req.param4 = numCal - 1
        req.param2 = calendar.toInteger()
        self.sendRequest(req)

    def execScript(self, script):
        """ Lance l'exécution d'un script 
            Ex: lm [mon scenario] (= lance le scenarion "mon scenario")
            Ex: lm 2 aft 3200 (= lance le scenario 2 dans une heure)
            Ex : lm [test1].lm [test2] (= lance test1 puis test2)
        """
        if len(script) > 0:
            req = ZbRequest()
            req.command = 16
            req.message = "cmd: " + script
            self.sendRequest(req)

    def getSensorInfo(self, idSensor):
        """ Retourne les valeurs v1 et v2 du capteur spécifié
            ainsi que la date heure du relevé.
            Pour les sondes Oregon et TS10, il faut diviser v1 par 10.
            Tableau en retour :
            index 0 => date du relevé
            index 1 => v1
            index 2 => v2        
        """
        if len(idSensor) > 0:
            url = "http://" + self.ip + "/sensors.xml"
            handle = urllib.urlopen(url)
            xmlContent = handle.read()
            handle.close()
            type = idSensor[0:2]
            number = idSensor[2:]
            xmlDoc = xml.dom.minidom.parseString(xmlContent)
            nodes = xmlDoc.getElementsByTagName("ev")
            for node in nodes:
                if node.getAttribute("pro") == type and node.getAttribute("id") == number:
                    v1 = int(node.getAttribute("v1"))
                    v2 = int(node.getAttribute("v2"))
                    dateHeure = datetime.fromtimestamp(int(node.getAttribute("gmt")))
                    info = [dateHeure, v1, v2]
                    return info
