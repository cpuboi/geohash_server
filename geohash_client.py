#!/usr/bin/env python3

import json
import socket


class GeohashClient():
    def __init__(self, ip="127.0.0.1", port=9999):
        self.connected = False
        self.server_ip = ip
        self.server_port = port
        self.connect(ip=ip, port=port)

    def connect(self, ip="127.0.0.1", port=9999):
        self.connection = socket.socket(socket.AF_INET, socket.SOCK_STREAM)  # Set up TCP/IP Socket to server
        self.connection.connect((ip, port))
        self.reply = ""
        self.connected = True

    def disconnect(self):
        command = json.dumps({"cmd": "disconnect"})
        self.connection.send(command.encode())
        self.connected = False

    def query_geohash(self, _geohash):
        command = json.dumps({"cmd": "geohash",
                              "data": _geohash})
        self.connection.sendall(command.encode("utf8"))
        reply = self.connection.recv(4096).decode("utf8")
        return reply

    def query_lat_lon(self, lat, lon):
        """ Takes two input parameters, latitude and longitude, returns geohash"""
        string_latlon = str(lat) + "," + str(lon)
        command = json.dumps({"cmd": "latlon",
                              "data": string_latlon})
        self.connection.sendall(command.encode("utf8"))
        reply = self.connection.recv(4096).decode("utf8")
        return reply

    def __query_status(self):
        command = json.dumps({"cmd": "geohash",
                              "data": "gcpuvr71"})  # Is London still there?
        self.connection.sendall(command.encode("utf8"))
        reply = self.connection.recv(4096).decode("utf8")
        return reply

    def connected(self):
        """
        Checks if London exists, if the reply is correct then the server is up.
        """
        try:
            test_status = self.__query_status()
            if test_status:
                self.connected = True
                return True
            else:
                self.connected = False
                return False
        except BrokenPipeError:
            self.connected = False
            return False
