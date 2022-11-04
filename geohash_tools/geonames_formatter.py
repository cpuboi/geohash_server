#!/usr/bin/env python3

""" This script converts geonames to the type of csv files geohasher wants.
Source:
geonameid         : integer id of record in geonames database
name              : name of geographical point (utf8) varchar(200)
asciiname         : name of geographical point in plain ascii characters, varchar(200)
alternatenames    : alternatenames, comma separated, ascii names automatically transliterated, convenience attribute from alternatename table, varchar(10000)
latitude          : latitude in decimal degrees (wgs84)
longitude         : longitude in decimal degrees (wgs84)
feature class     : see http://www.geonames.org/export/codes.html, char(1)
feature code      : see http://www.geonames.org/export/codes.html, varchar(10)
country code      : ISO-3166 2-letter country code, 2 characters
cc2               : alternate country codes, comma separated, ISO-3166 2-letter country code, 200 characters
admin1 code       : fipscode (subject to change to iso code), see exceptions below, see file admin1Codes.txt for display names of this code; varchar(20)
admin2 code       : code for the second administrative division, a county in the US, see file admin2Codes.txt; varchar(80)
admin3 code       : code for third level administrative division, varchar(20)
admin4 code       : code for fourth level administrative division, varchar(20)
population        : bigint (8 byte int)
elevation         : in meters, integer
dem               : digital elevation model, srtm3 or gtopo30, average elevation of 3''x3'' (ca 90mx90m) or 30''x30'' (ca 900mx900m) area in meters, integer. srtm processed by cgiar/ciat.
timezone          : the iana timezone id (see file timeZone.txt) varchar(40)
modification date : date of last modification in yyyy-MM-dd format

Dest:
lat,lon,name,admin1,admin2,cc
"""


def geonames2csv(geonames_line):
    split_line = geonames_line.rstrip().split("\t")

    lat = split_line[4]
    lon = split_line[5]
    name = split_line[2]
    admin1 = split_line[10]
    admin2 = split_line[11]
    admin3 = split_line[12]
    admin4 = split_line[13]
    cc = split_line[8]
    cc2 = split_line[9]
    timezone = split_line[17]
    #csv_list = ','.join([lat, lon, name, admin1, admin2, admin3, admin4, cc, cc2, timezone])
    csv_list2 = ','.join([lat, lon, name, cc, cc2, timezone])
    return csv_list2


def process_file(input_file, output_file):
    ok = 0
    err = 0
    lines = 0
    with open(input_file, 'rb') as __input_file:
        with open(output_file, 'ab') as __output_file:
            for line in __input_file:
                lines += 1
                try:
                    csv_line = geonames2csv(line.decode())
                    __output_file.write(f"{csv_line}\n".encode())
                    ok += 1
                except Exception as e:
                    print(e)
                    err += 1
    print(f"Of {lines} lines, {ok} were ok, {err} were erroneous.")


def main():
    infile = "./cities1000.txt"
    outfile = "./cities1000.csv"
    process_file(infile, outfile)


main()
