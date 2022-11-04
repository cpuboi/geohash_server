#!/usr/bin/env python3

"""
Will listen to a port for geohash strings and return json of information
Starts server, starts threaded client threads, listens to input geohash / lat-lon queries and returns closest city.

Please don't run as root/admin, this is not safe practice.

TODO: Shut down in a nicer way.
TODO: Use async
"""

import datetime
import json
import logging
import os
import socket
import sys

from geohash_tools import geohash, geohash_sqlite3

DEBUG_MESSAGES = True
daemon = True
queries = 0
__location__ = os.path.realpath(os.path.join(os.getcwd(), os.path.dirname(__file__)))
GEOHASH_SQLITE3_FILE = os.path.join(__location__, "./geohash_worldcities.db")

geo_dict = {}

logging.basicConfig(format='%(asctime)s %(message)s',
                    level=logging.INFO)
logger = logging.getLogger()


def geohash_tuple_to_json(geohash_tuple):
    try:
        geohash_dict = {"city": geohash_tuple[0][5],
                        "admin": geohash_tuple[0][6],
                        "country": geohash_tuple[0][7],
                        "precision": geohash_tuple[1],
                        "hits": geohash_tuple[2]}
    except:
        geohash_dict = {"city": None,
                        "admin": None,
                        "country": None,
                        "precision": geohash_tuple[1],
                        "hits": geohash_tuple[2]}
    return geohash_dict


def recieve_input_from_client(connection, MAX_BUFFER_SIZE):
    input_dict = {"cmd": None,
                  "status": None}
    try:
        input_data = connection.recv(MAX_BUFFER_SIZE)
        input_dict = json.loads(input_data.decode("utf8"))
        if not "cmd" in input_dict:
            input_dict["cmd"] = "disconnect"
            input_dict["status"] = "General error"
    except UnicodeDecodeError:
        if DEBUG_MESSAGES:
            logger.error("Input incorrectly formatted, closing connection.")
        input_dict["cmd"] = "disconnect"
        input_dict["status"] = "Input incorrectly formatted"
    except:
        input_dict["cmd"] = "disconnect"
        input_dict["status"] = "General error"
    return input_dict


def process_input(input_dict, cursor):
    try:
        if input_dict["cmd"] == "geohash":
            _geohash = input_dict["data"]
        elif input_dict["cmd"] == "latlon":
            ll_split = input_dict["data"].split(",")
            lat = float(ll_split[0])
            lon = float(ll_split[1])
            _geohash = geohash.encode(lat, lon)
        else:
            error_msg = "Server could not process geohash."
            logger.error(error_msg)
            return error_msg
        if len(_geohash) < 8:
            return "ERROR: Geohash shorter than 8 characters"
        geohash_city_tuple = geohash_sqlite3.query_geohash_sqlite3(cursor, _geohash)
        geohash_city_json = json.dumps(geohash_tuple_to_json(geohash_city_tuple))
        return geohash_city_json
    except json.JSONDecodeError as e:
        error_msg = f"Server could not process geohash: {e} "
        logger.error(error_msg)
        return error_msg
    except Exception as e:
        error_msg = f"Server could not process geohash {e}"
        logger.error(error_msg)
        return error_msg


def return_data_to_client(connection, output_data):
    connection.send(output_data.encode("utf8"))


def client_thread(connection, ip, port, sqlite3_cursor, MAX_BUFFER_SIZE=4096):
    global queries
    listening = True
    while listening:
        input_dict = recieve_input_from_client(connection, MAX_BUFFER_SIZE)
        if input_dict["cmd"] == "disconnect":  # Handle disconnects here
            logger.info("Terminating Connection.")
            connection.close()
            logger.info(f"Connection from {str(ip)} ended")
            listening = False
        else:
            geohash_json = process_input(input_dict, sqlite3_cursor)
            # loader(loader_state) # Prints nice thing, Super slow apparently
            if daemon:
                if queries % 3000 == 0:
                    logger.info(f"Queries: {queries}")
            else:
                if queries % 10 == 0:
                    print(str(datetime.datetime.now().isoformat()) + ": Queries={}".format(str(queries)), end="\r")
            queries += 1
            try:
                return_data_to_client(connection, geohash_json)
            except BrokenPipeError:
                logger.error(f"Client disconnected, processed {queries} queries.")
                listening = False
                # connection.close()


def start_server(ip, port, sqlite3_cursor):
    from threading import Thread  # Multithreaded listener
    server_running = True
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)  # Start TCP/IP socket
    server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)  # Allow reuse of socket

    try:
        server_socket.bind((ip, port))
        logger.info(f"Client {ip} connected.")
    except Exception as e:
        logger.error(f"Creating socket {e}")
        sys.exit(1)
    server_socket.listen(10)
    logger.info(f"Server listening on port {str(port)}")
    try:
        while server_running:
            connection, address = server_socket.accept()
            client_ip, client_port = str(address[0]), str(address[1])
            logger.info(f"Client {client_ip} {client_port} connected.")
            try:
                Thread(target=client_thread, args=(connection, ip, port, sqlite3_cursor)).start()
            except Exception as e:
                logger.error(f"Could not start socket to client {client_ip}, {e}")
    except KeyboardInterrupt:
        logger.info("SIGINT shutting down server.")
        server_socket.shutdown(socket.SHUT_RDWR)
        server_socket.close()
        sys.exit(0)


def main():
    global geo_dict
    logger.info(f"Starting geohash server, loading source file {GEOHASH_SQLITE3_FILE}")
    sqlite3_cursor = geohash_sqlite3.load_sqlite3_file(GEOHASH_SQLITE3_FILE)
    start_server("127.0.0.1", 9999, sqlite3_cursor)


if __name__ == "__main__":
    main()
