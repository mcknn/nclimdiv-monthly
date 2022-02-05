import numpy as np
import pandas as pd
import sqlite3
from datetime import datetime

DB = '/home/amckann/DataGripProjects/nclimdiv/clim.sqlite'


def create_connection(db_file):
    conn = None
    try:
        conn = sqlite3.connect(db_file)
    except sqlite3.Error as e:
        print(e)

    return conn


def main():
    t0 = datetime.now()
    load_states()
    print(f'completed in {datetime.now()-t0}')
    t0 = datetime.now()
    load_counties()  # patience...
    print(f'completed in {datetime.now()-t0}')


def load_states():
    print('collecting state data...')
    f_dict = {
        'cdd':'climdiv-cddcst-v1.0.0-20211104',
        'hdd':'climdiv-hddcst-v1.0.0-20211104',
        'pcp':'climdiv-pcpnst-v1.0.0-20211104',
        'pdsi':'climdiv-pdsist-v1.0.0-20211104',
        'phdi':'climdiv-phdist-v1.0.0-20211104',
        'pmdi':'climdiv-pmdist-v1.0.0-20211104',
        'tmax':'climdiv-tmaxst-v1.0.0-20211104',
        'tmin':'climdiv-tminst-v1.0.0-20211104',
        'tavg':'climdiv-tmpcst-v1.0.0-20211104',
        'zndx':'climdiv-zndxst-v1.0.0-20211104'
    }

    for k, v in f_dict.items():
        df = pd.DataFrame(np.loadtxt(F'/home/amckann/PycharmProjects/nclimdiv/data/{v}.txt'))
        df[0] = df[0].astype(str)
        '''
            001 0 26 1895, fips, region, var code, year
        '''
        df[0] = df[0].str.partition('.', expand=True)[0].str.zfill(10)
        d = {'fips':[], 'variable':[], 'year':[]}
        for s in df[0]:
            d['fips'].append(s[:3])
            d['variable'].append(s[4:6])
            d['year'].append(s[-4:])

        for name, arr in d.items(): df[name] = arr

        df = df.drop(0, axis=1)
        df.to_pickle(f'st_{k}')

    # get state codes and names. NOT actually fips
    states = pd.read_csv('/home/amckann/PycharmProjects/nclimdiv/data/fips', sep=' ')
    states['code'] = states['code'].astype(str).str.zfill(3)
    print('done.')
    print('inserting data...')
    # insert tables
    conn = create_connection(DB)
    cur = conn.cursor()
    # states
    for state in states.iterrows():
        code, name = state[1][0], state[1][1]
        sql = f"INSERT INTO STATES VALUES('{code}', '{name}')"
        cur.execute(sql)
    conn.commit()

    # insert state record rows
    for state in states.iterrows():
        code = state[1][0]
        for year in range(1895, 2021):
            y = str(year)
            for month in range(1, 13):
                m = str(month).zfill(2)
                sql = f"INSERT INTO STATE_RECORDS VALUES('{y}', '{m}', '{code}', 'NULL', 'NULL', 'NULL', 'NULL', 'NULL','NULL','NULL','NULL','NULL','NULL')"
                try:
                    cur.execute(sql)

                except sqlite3.IntegrityError as e:
                    print(f'{e}, {y}, {m}, {code}') # should not happen if table is empty
    conn.commit()

    for var_name in f_dict.keys():
        df = pd.read_pickle(f'st_{var_name}')
        for row in df.iterrows():
            for month in range(1, 13):
                m = str(month).zfill(2)
                try:
                    value, state, year = row[1][month], row[1]['fips'], row[1]['year']
                    if int(state) < 99:  # exclude regions
                        sql = f"UPDATE STATE_RECORDS SET {var_name} = {value} where yr = '{year}' AND mo = '{m}' AND state = '{state}'"
                        cur.execute(sql)

                except Exception as e:
                    print(e)
                    print(var_name)
                    print(m)
    conn.commit()
    conn.close()
    print('done.')


def load_counties():
    print('collecting county data...')
    conn = create_connection(DB)
    cur = conn.cursor()

    f_dict = {
        'pcp':'climdiv-pcpncy-v1.0.0-20211104',
        'tmax':'climdiv-tmaxcy-v1.0.0-20211104',
        'tmin':'climdiv-tmincy-v1.0.0-20211104',
        'tavg':'climdiv-tmpccy-v1.0.0-20211104',
    }

    for k, v in f_dict.items():
        df = pd.DataFrame(np.loadtxt(f'/home/amckann/PycharmProjects/nclimdiv/data/{v}.txt'))
        df[0] = df[0].astype(str)

        df[0] = df[0].str.partition('.', expand=True)[0].str.zfill(11)
        d = {
            'state fips': [],
            'county fips': [],
            'variable': [],
            'year': []
        }
        for s in df[0]:
            d['state fips'].append(s[:2])
            d['county fips'].append(s[2:5])
            d['variable'].append(s[5:7])
            d['year'].append(s[-4:])
        for name, arr in d.items(): df[name] = arr

        df = df.drop(0, axis=1)
        df.to_pickle(k)
    # load db
    print('done')
    print('inserting data...')
    df = pd.read_pickle('tavg')
    codes = list(df['county fips'])
    states = list(df['state fips'])
    combined = []
    for i, ele in enumerate(codes):
        combined.append(states[i].zfill(3) + ',' + ele)
    combined = set(list(combined))
    # first create a row for each county, year, month combination
    for i in combined:
        state, county = i.split(',')
        for year in range(1895, 2021):
            y = str(year)
            for month in range(1, 13):
                m = str(month).zfill(2)
                sql = f"INSERT INTO COUNTY_RECORDS VALUES('{y}', '{m}', '{county}', '{state}', NULL, NULL, NULL, NULL)"
                try:
                    cur.execute(sql)

                except sqlite3.IntegrityError as e:
                    print(e)

    conn.commit()

    # now load variables
    count = 0
    cur = conn.cursor()
    for k in ['tavg', 'pcp', 'tmin', 'tmax']:
        df = pd.read_pickle(k)
        for row in df.iterrows():
            for month in range(1,13):
                m = str(month).zfill(2)
                try:
                    value, year, county, state = row[1][month], row[1]['year'], row[1]['county fips'].zfill(3), row[1]['state fips'].zfill(3)
                    if int(state) < 99:
                        sql = f"UPDATE COUNTY_RECORDS SET {k} = {value} where yr = '{year}' AND mo = '{m}' AND county = '{county}' AND state = '{state}'"
                        cur.execute(sql)
                        count += 1

                except Exception as e:
                    print(e)
                    print(k)
                    print(m)
    conn.commit()
    print(count)
    print(cur.rowcount)
    conn.close()
    print('done.')


if __name__ == '__main__':
    main()
