# GEOHASHER #

This is a reverse geohasher.  
It takes a geohash and returns the closest city or location.  

It consists of a server and a client.   
The SQLite database is designed to quickly provide lookups without using much RAM.  

The default database is very simple, the strength comes when you ingest your own custom data. 


### Server ###
* Loads a SQLite database and listens to requests on a port, default is 9999.  
* Can (soon) be used in standalone mode from the terminal.  


### Client ###
* Connects to the server, accepts "lat,lon" or a geohash, returns closest location.
  


  

### Geohash Tools ###
* geonames_formatter.py
  - Converts http://download.geonames.org/export/dump/ to csv files.  
* simplemaps_formatter.py
  - Converts https://simplemaps.com/data/world-cities to compatible csv file.
 
* geohash_sqlite3.create_sqlite_from_csv 
  - creates an sqlite3 from the previously generated csv files.
  - The following code assumes that a simplemaps csv file has been generated in the previous step.
 ```
import geohash_sqlite3
geohash_sqlite3.create_sqlite_from_csv(csv_file="./geohash_tools/worldcities_formatted.csv", sqlite3_file="./geohash_worldcities.db")
``` 
* After creating an SQLite database, run a couple of queries then optimize it to improve performance.  
```sqlite3 geohash_worldcities.db 'PRAGMA optimize;'```
