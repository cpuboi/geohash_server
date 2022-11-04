#!/usr/bin/env python3


"""
GNU LICENSE Affero General Public

Creates and reads Geohash data from a SQLite3 database.

Geohash is base32 encoded.
3 characters = 2 bytes

u6s = 26 6 24

The strucutre of the database is:
first_four_letters,fifth,sixth,seventh,eigth,city,country

sthlm = "u6sce14mqd"


"""
import sqlite3
import time

#  Note: the alphabet in geohash differs from the common base32
#  alphabet described in IETF's RFC 4648
#  (http://tools.ietf.org/html/rfc4648)
__base32 = '0123456789bcdefghjkmnpqrstuvwxyz'
__DECODEMAP = {}
for i in range(len(__base32)):
    __DECODEMAP[__base32[i]] = i
del i


def load_sqlite3_file(sqlite3_file):
    " Opens sqlite3 file, returns cursor. "
    # if os.path.isfile(sqlite3_file):
    db = sqlite3.connect(sqlite3_file,
                         check_same_thread=False)  # This is safe since only parallel reads are being done, not writes.
    cursor = db.cursor()

    return cursor


def create_sqlite3_database(cursor):
    "\"id\" INTEGER not null primary key,"

    creation_string = """ CREATE TABLE IF NOT EXISTS geohash (
    one INTEGER not null,
    five INTEGER,
    six INTEGER,
    seven INTEGER,
    eight INTEGER,
    city STRING,
    admin STRING,
    cc STRING
    );
    """
    try:
        cursor.execute(creation_string)

        # Create indexes
        cursor.execute("CREATE INDEX one ON geohash(one)")
        cursor.execute("CREATE INDEX five ON geohash(five)")
        cursor.execute("CREATE INDEX six ON geohash(six)")
        cursor.execute("CREATE INDEX seven ON geohash(seven)")
        cursor.execute("CREATE INDEX eight ON geohash(eight)")

        cursor.execute("PRAGMA journal_mode = OFF")  # Dont use journal
        cursor.execute("PRAGMA synchronous = 0")  # Dont flush to disk
        cursor.execute("PRAGMA cache_size = 100000")  # Pages in memory
        cursor.execute("PRAGMA locking_mode = EXCLUSIVE")  # Lock database
        cursor.execute("PRAGMA temp_store = MEMORY")  # In memory database

        print("Created database, don't forget to optimize it after running a couple of queries. pragma optimize;")
    except:
        print("Could not create new geohash database.")


def sqlite3_create_lite_database(cursor):
    "\"id\" INTEGER not null primary key,"

    creation_string = """ CREATE TABLE IF NOT EXISTS geohash (
    one INTEGER not null,
    five INTEGER,
    six INTEGER,
    seven INTEGER,
    eight INTEGER,
    city STRING,
    admin STRING,
    cc STRING
    );
    """
    try:
        cursor.execute(creation_string)

        # Create indexes
        cursor.execute("CREATE INDEX one ON geohash(one)")

        cursor.execute("PRAGMA journal_mode = OFF")  # Dont use journal
        cursor.execute("PRAGMA synchronous = 0")  # Dont flush to disk
        cursor.execute("PRAGMA cache_size = 100000")  # Pages in memory
        # cursor.execute("PRAGMA locking_mode = EXCLUSIVE")  # Lock database
        cursor.execute("PRAGMA temp_store = MEMORY")  # In memory database
    except:
        print("Could not create new geohash database.")


def batch_insert_sqlite3(cursor, data_tuple_generator, insert_string=None, batch_size=100_000):
    def smallgen(biggen):
        count = 0
        item_list = []
        for item in biggen:
            if item:
                count += 1
                item_list.append(item)
                if count > 1000:
                    return item_list

    itemcount = 0

    insert_string = f"INSERT OR IGNORE INTO geohash(one, five, six, seven, eight, city, admin, cc) VALUES(?, ?, ?, ?, ?, ?, ?, ?);"

    try:
        for item in data_tuple_generator:
            itemcount += 1
            cursor.executemany(insert_string, (item,))
            if itemcount % batch_size == 0:
                cursor.execute("COMMIT")
    except Exception as e:
        print(e)
    print(f"Inserted {itemcount} to SQLite db.")
    cursor.execute("COMMIT")


def latlon_to_geohash(latlon_string):
    """
    Format is assumed to be "50.321,20.1234"
    Splits the string in to latitude and longitude.
    Checks that the format is OK, if so, convert to geohash and return that value.
    """

    def validate_latlon(position_string):
        """
        latitude max values are +90 -90
        longitude max values are +180 -180

        Assuming that the values are in the format of lat,lon
        If correct, return tuple, if not return None
        """
        try:
            split_position_string = position_string.split(",")
            lat = float(split_position_string[0])
            lon = float(split_position_string[1])
            if -90 < lat < 90 and -180 < lon < 180:
                latlon_tuple = (lat, lon)
        except Exception as e:
            latlon_tuple = None
        return latlon_tuple

    def encode(latitude, longitude, precision=10):
        """
        This is code from geohash made by: Leonard Norrgard <leonard.norrgard@gmail.com>
        Encode a position given in float arguments latitude, longitude to
        a geohash which will have the character count precision.
        """
        lat_interval, lon_interval = (-90.0, 90.0), (-180.0, 180.0)
        geohash = []
        bits = [16, 8, 4, 2, 1]
        bit = 0
        ch = 0
        even = True
        while len(geohash) < precision:
            if even:
                mid = (lon_interval[0] + lon_interval[1]) / 2
                if longitude > mid:
                    ch |= bits[bit]
                    lon_interval = (mid, lon_interval[1])
                else:
                    lon_interval = (lon_interval[0], mid)
            else:
                mid = (lat_interval[0] + lat_interval[1]) / 2
                if latitude > mid:
                    ch |= bits[bit]
                    lat_interval = (mid, lat_interval[1])
                else:
                    lat_interval = (lat_interval[0], mid)
            even = not even
            if bit < 4:
                bit += 1
            else:
                geohash += __base32[ch]
                bit = 0
                ch = 0
        return ''.join(geohash)

    latlon_tuple = validate_latlon(latlon_string)

    if latlon_tuple:
        geohash = encode(latlon_tuple[0], latlon_tuple[1])
    else:
        print(f"Not a valid latlon string:\t{latlon_string}")
        geohash = None
    return geohash


def geohash_to_int_tuple(geohash_string):
    """
    Converts geohash to a tuple that will fit into the SQLite database.
    first_four
     * Converts first four to:
      0-31,0-31,0-31,0-31, 12,1,4,29 = 12010429
    five
    six
    seven
    eight
    :param geohash:
    :return:
    """

    global __DECODEMAP
    __decodemap = __DECODEMAP

    def validate_geohash(geohash, __decodemap):
        valid_geohash = True
        for character in geohash:
            if character not in __decodemap:
                valid_geohash = False
        return valid_geohash

    def geohash_to_tuple(geohash, __decodemap):
        def geohash_str_to_num(character, __decodemap):
            return int(__decodemap[character])

        def get_first_four(geohash, __decodemap):
            "Prune geohash"
            geohash = geohash[:8]

            four = geohash[:4]
            four_decoded = ""
            for i in four:
                try:
                    four_decoded += str(__decodemap[i]).zfill(2)
                except Exception as e:
                    print(e)
                    return None
            return int(four_decoded)

        geohash_length = len(geohash)
        first_four = get_first_four(geohash, __decodemap)
        five = 0
        six = 0
        seven = 0
        eight = 0
        if first_four == None:
            return None

        if validate_geohash(geohash, __decodemap):
            try:
                five = geohash_str_to_num(geohash[4], __decodemap)
                try:
                    six = geohash_str_to_num(geohash[5], __decodemap)
                    try:
                        seven = geohash_str_to_num(geohash[6], __decodemap)
                        try:
                            eight = geohash_str_to_num(geohash[7], __decodemap)
                        except IndexError:
                            pass
                    except IndexError:
                        pass
                except IndexError:
                    pass
            except IndexError:
                pass

            int_tuple = (first_four, five, six, seven, eight)

            return int_tuple
        else:
            print(f"Error: {geohash} is not valid.")
            return None

    if validate_geohash(geohash_string, __decodemap):
        geohash_tuple = geohash_to_tuple(geohash_string, __decodemap)
    else:
        print(f"[x] Not valid geohash: {geohash_string}")
        geohash_tuple = None
    return geohash_tuple


def geohash_csv_to_tuple(geohash_csv_file, file_contains_latlon=True):
    count = 0
    with open(geohash_csv_file, 'rb') as geohashfile:
        for line in geohashfile:
            if file_contains_latlon:  # Convert latlon to geohash
                split_line = line.decode().rstrip().split(",")
                lat = split_line[0]
                lon = split_line[1]
                geohash = latlon_to_geohash(','.join([lat, lon]))
                name = split_line[2]
                city = split_line[3]
                admin = split_line[4]
                cc = split_line[5]
                geohash_int_tuple = geohash_to_int_tuple(geohash)
            else:
                split_line = line.decode().rstrip().split(",")
                geohash = split_line[0]
                name = split_line[1]
                city = split_line[2]
                admin = split_line[3]
                cc = split_line[4]
                geohash_int_tuple = geohash_to_int_tuple(geohash)

            # Complete the tuple
            if geohash_int_tuple:
                count += 1
                if count % 10000 == 0:
                    print(f"Processed {count} items")
                complete_list = list(geohash_int_tuple)
                complete_list.append(city)
                complete_list.append(admin)
                complete_list.append(cc)
                complete_tuple = tuple(complete_list)
                del complete_list
                yield complete_tuple
            else:
                print(f"Error in line: {line}")


def query_geohash_sqlite3(cursor, geohash):
    geohash_tuple = geohash_to_int_tuple(geohash)
    data = None

    def get_location(select_query):
        cursor.execute(select_query)
        data = cursor.fetchall()
        if data == []:
            return None
        else:
            return data

    " Yes this might look ugly but it works. "
    precision = 8
    select_query = f"SELECT * FROM geohash WHERE one = {geohash_tuple[0]} AND five = {geohash_tuple[1]} AND six = {geohash_tuple[2]} AND seven = {geohash_tuple[3]} AND eight = {geohash_tuple[4]};"
    data = get_location(select_query)
    if not data:
        precision = 7
        select_query = f"SELECT * FROM geohash WHERE one = {geohash_tuple[0]} AND five = {geohash_tuple[1]} AND six = {geohash_tuple[2]} AND seven = {geohash_tuple[3]} ;"
        data = get_location(select_query)
        if not data:
            precision = 6
            select_query = f"SELECT * FROM geohash WHERE one = {geohash_tuple[0]} AND five = {geohash_tuple[1]} AND six = {geohash_tuple[2]};"
            data = get_location(select_query)
            if not data:
                precision = 5
                select_query = f"SELECT * FROM geohash WHERE one = {geohash_tuple[0]} AND five = {geohash_tuple[1]} ;"
                data = get_location(select_query)
                if not data:
                    precision = 4
                    select_query = f"SELECT * FROM geohash WHERE one = {geohash_tuple[0]} ;"
                    data = get_location(select_query)
    if data:
        hits = len(data)
        one_data_item = data[int(len(data) / 2)]
    else:
        precision = 0
        hits = 0
        one_data_item = None
    return one_data_item, precision, hits


def create_sqlite_from_csv(csv_file="./csv_data/worldcities_formatted.csv", sqlite3_file="./geohash_worldcities.db"):
    __gen = geohash_csv_to_tuple(csv_file)
    cursor = load_sqlite3_file(sqlite3_file)
    sqlite3_create_lite_database(cursor)
    now = time.time()

    batch_insert_sqlite3(cursor, __gen)
    print(f"Insertion took {time.time() - now} seconds")
