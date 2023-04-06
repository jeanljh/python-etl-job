import csv
import sqlite3

# Connect to the stores.db database via sqlite3
conn = sqlite3.connect('stores.db')
c = conn.cursor()

# Create dim_country table if does not exist
c.execute('''CREATE TABLE IF NOT EXISTS dim_country
             (country_id INTEGER PRIMARY KEY AUTOINCREMENT,
              country_code TEXT UNIQUE,
              country_name TEXT)''')

# Load the data from the dim_country CSV file
with open('dim_country.csv', newline='') as csvfile:
    reader = csv.DictReader(csvfile)
    for row in reader:
        # Look up for the country code (case insensitive)
        country_name = row['country_name']
        c.execute('SELECT country_code FROM dim_country WHERE LOWER(country_name) = LOWER(?)', (country_name.lower(),))
        result = c.fetchone()
        if result:
            country_code = result[0]
        else:
            country_code = row['country_code']
            c.execute('INSERT INTO dim_country (country_code, country_name) VALUES (?, ?)', (country_code, country_name))

# Create dim_stores table if does not exist
c.execute('''CREATE TABLE IF NOT EXISTS dim_stores
             (store_id INTEGER PRIMARY KEY AUTOINCREMENT,
              store_code TEXT UNIQUE,
              store_name TEXT,
              country_code TEXT,
              street_name TEXT,
              pin_code TEXT,
              lvl1_geog TEXT,
              lvl2_geog TEXT,
              lvl3_geog TEXT,
              FOREIGN KEY (country_code) REFERENCES dim_country(country_code))''')

# Load the data from the sample_data CSV file
with open('sample_data.csv', newline='') as csvfile:
    reader = csv.DictReader(csvfile)
    for row in reader:
        # # Look up for the country code (case insensitive)
        country_name = row['country_name']
        c.execute('SELECT country_code FROM dim_country WHERE LOWER(country_name) = LOWER(?)', (country_name.lower(),))
        result = c.fetchone()
        # print log if record not found and continue to next data row
        if result is None:
            print (f'FAILED - dim_country table does not have country_name: {country_name}')
            continue
        country_code = result[0]
        
        # Get all data row
        store_name = row['store_name']
        street_name = row['street_name']
        pin_code = row['pin_code']
        lvl1_geog = row['lvl1_geog']
        lvl2_geog = row['lvl2_geog']
        lvl3_geog = row['lvl3_geog']
        
        # Get the current store_id
        c.execute('SELECT MAX(store_id) + 1 FROM dim_stores')
        result = c.fetchone()
        store_id = result[0]
        if store_id is None:
            store_id = 1
        
        # Construct store_code
        store_code = country_code + str(store_id)
        
        # Construct the expected array of data from input data
        expected = [store_id, store_code, store_name, country_code, street_name, pin_code, lvl1_geog, lvl2_geog, lvl3_geog]
        
        # Insert data into stores.db
        c.execute('''INSERT INTO dim_stores
                     (store_code, store_name, country_code, street_name, pin_code, lvl1_geog, lvl2_geog, lvl3_geog)
                     VALUES(?, ?, ?, ?, ?, ?, ?, ?)''',
                  (store_code, store_name, country_code, street_name, pin_code, lvl1_geog, lvl2_geog, lvl3_geog))
        
        # Get the newly inserted record from stores.db
        c.execute('SELECT * from dim_stores WHERE store_id = ?', (store_id,))
        result = c.fetchone()
        
        # print log if record not found
        if result is None:
            print(f'FAILED - data with store_id: {store_id} | store_code: {store_code} does not exist in database')
            continue

        # check that the record matches the input data and print status and record data
        for id, res in enumerate(result):
            if (res == expected[id]):
                state = 'PASSED'
            else:
                state = 'FAILED'
            print(state, result)
            break

# Commit the changes and close the connection
conn.commit()
conn.close()
