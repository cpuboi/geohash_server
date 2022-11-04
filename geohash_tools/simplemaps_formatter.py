#!/usr/bin/env python3

"""
This script converts the simplemap csv file to a csv file that is adapted to the geohash reader.
Source: https://simplemaps.com/data/world-cities """

def simplemaps_generator(simplemaps_file):
    """
    Header: 'city,city_ascii,lat,lng,country,iso2,iso3,admin_name,capital,population,id'
     """
    with open(simplemaps_file, 'rb') as __simplemaps_file:
        __simplemaps_file.readline()  # Skip header
        for line in __simplemaps_file:
            clean_line_list = line.decode().strip().replace("\"", "").split(",")
            city = clean_line_list[0]
            lat = clean_line_list[2]
            lon = clean_line_list[3]
            cc = clean_line_list[5]
            admin = clean_line_list[7]
            name = ""
            city_list = ','.join([lat, lon, name, city, admin, cc])
            yield city_list

def write_simplemaps_file(__simplemaps_file, output_filename):
    simplemaps_gen = simplemaps_generator(__simplemaps_file)
    with open(output_filename, 'wb') as __output_filename:
        for item in simplemaps_gen:
            __output_filename.write((str(item) + "\n").encode())


simplemaps_file = "./worldcities.csv"
output_file = "./worldcities_formatted.csv"
write_simplemaps_file(simplemaps_file, output_file)
