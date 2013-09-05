import psycopg2
from cStringIO import StringIO
import gzip
import os
import csv

DB_HOST = os.environ.get('DB_HOST')

WORK_AREA_CREATE = """
    CREATE TABLE work_area_detail (
        geocode VARCHAR(15) NOT NULL,
        age_29_under INTEGER,
        age_30_54 INTEGER,
        age_55_over INTEGER,
        earnings_1250_under INTEGER,
        earnings_1251_3333 INTEGER,
        earnings_3333_over INTEGER,
        cns01 INTEGER,
        cns02 INTEGER,
        cns03 INTEGER,
        cns04 INTEGER,
        cns05 INTEGER,
        cns06 INTEGER,
        cns07 INTEGER,
        cns08 INTEGER,
        cns09 INTEGER,
        cns10 INTEGER,
        cns11 INTEGER,
        cns12 INTEGER,
        cns13 INTEGER,
        cns14 INTEGER,
        cns15 INTEGER,
        cns16 INTEGER,
        cns17 INTEGER,
        cns18 INTEGER,
        cr19 INTEGER,
        cr20 INTEGER,
        cr01 INTEGER,
        cr02 INTEGER,
        cr03 INTEGER,
        cr04 INTEGER,
        cr05 INTEGER,
        cr06 INTEGER,
        cr07 INTEGER,
        ct01 INTEGER,
        ct02 INTEGER,
        cd01 INTEGER,
        cd02 INTEGER,
        cd03 INTEGER,
        cd04 INTEGER,
        cs01 INTEGER,
        cs02 INTEGER,
        cfa01 INTEGER,
        cfa02 INTEGER,
        cfa03 INTEGER,
        cfa04 INTEGER,
        cfa05 INTEGER,
        cfs01 INTEGER,
        cfs02 INTEGER,
        cfs03 INTEGER,
        cfs04 INTEGER,
        cfs05 INTEGER,
        createdate DATE,
        area_type VARCHAR,
        data_year INTEGER,
        job_type VARCHAR,
        segment VARCHAR,
        PRIMARY KEY (geocode, area_type, data_year, job_type, segment)
    )
"""

AREA_CREATE = """
    CREATE TABLE area_detail (
        geocode VARCHAR(15) NOT NULL,
        age_29_under INTEGER,
        age_30_54 INTEGER,
        age_55_over INTEGER,
        earnings_1250_under INTEGER,
        earnings_1251_3333 INTEGER,
        earnings_3333_over INTEGER,
        cns01 INTEGER,
        cns02 INTEGER,
        cns03 INTEGER,
        cns04 INTEGER,
        cns05 INTEGER,
        cns06 INTEGER,
        cns07 INTEGER,
        cns08 INTEGER,
        cns09 INTEGER,
        cns10 INTEGER,
        cns11 INTEGER,
        cns12 INTEGER,
        cns13 INTEGER,
        cns14 INTEGER,
        cns15 INTEGER,
        cns16 INTEGER,
        cns17 INTEGER,
        cns18 INTEGER,
        cr19 INTEGER,
        cr20 INTEGER,
        cr01 INTEGER,
        cr02 INTEGER,
        cr03 INTEGER,
        cr04 INTEGER,
        cr05 INTEGER,
        cr06 INTEGER,
        cr07 INTEGER,
        ct01 INTEGER,
        ct02 INTEGER,
        cd01 INTEGER,
        cd02 INTEGER,
        cd03 INTEGER,
        cd04 INTEGER,
        cs01 INTEGER,
        cs02 INTEGER,
        createdate DATE,
        area_type VARCHAR,
        data_year INTEGER,
        job_type VARCHAR,
        segment VARCHAR,
        PRIMARY KEY (geocode, area_type, data_year, job_type, segment)
    )
"""

OD_CREATE = """
    CREATE TABLE origin_destination (
        h_geocode VARCHAR(15) NOT NULL,
        w_geocode VARCHAR(15) NOT NULL,
        S000 INTEGER,
        SA01 INTEGER,
        SA02 INTEGER,
        SA03 INTEGER,
        SE01 INTEGER,
        SE02 INTEGER,
        SE03 INTEGER,
        SI01 INTEGER,
        SI02 INTEGER,
        SI03 INTEGER,
        createdate DATE,
        data_year INTEGER,
        job_type VARCHAR,
        PRIMARY KEY (h_geocode, w_geocode, data_year, job_type)
    )
"""

AREAS = {
    'rac': 'residence_area',
    'wac': 'work_area',
}

def pick_table(uh, dirname, names):
    conn = psycopg2.connect('host=%s dbname=census user=census' % DB_HOST)
    cursor = conn.cursor()
    try:
        cursor.execute(OD_CREATE)
        conn.commit()
    except psycopg2.ProgrammingError, e:
        conn.rollback()
    try:
        cursor.execute(AREA_CREATE)
        conn.commit()
    except psycopg2.ProgrammingError, e:
        conn.rollback()
    try:
        cursor.execute(WORK_AREA_CREATE)
        conn.commit()
    except psycopg2.ProgrammingError, e:
        conn.rollback()
    loaded = [l[:-1] for l in open('loaded.txt', 'rb')]
    for name in names:
        fpath = os.path.join(dirname, name)
        if fpath in loaded:
            print 'Skipping %s ' % fpath
        else:
            if os.path.isfile(fpath):
                if '_od_' in fpath:
                    load_od(conn, cursor, fpath)
                else:
                    load_area(conn, cursor, fpath)

def load_od(conn, cursor, fpath):
    fname = os.path.basename(fpath)
    job_type, data_year = fname[-16:-12], fname[-11:-7]
    out = StringIO()
    with gzip.GzipFile(fpath) as f:
        reader = csv.reader(f)
        reader.next()
        for row in reader:
            row.extend([data_year, job_type])
            out.write('\t'.join(row))
            out.write('\n')
    out.seek(0)
    try:
        cursor.copy_from(out, 'origin_destination')
        conn.commit()
        print 'Loaded %s' % fpath
    except psycopg2.IntegrityError, e:
        print 'Looks like %s was already loaded: %s' % (fpath, e)
        conn.rollback()
    return None

def load_area(conn, cursor, fpath):
    fname = os.path.basename(fpath)
    area_type, segment, job_type, data_year = fname[3:6], fname[7:11], fname[-16:-12], fname[-11:-7]
    out = StringIO()
    with gzip.GzipFile(fpath) as f:
        reader = csv.reader(f)
        headers = reader.next()
        for row in reader:
            row.extend([AREAS[area_type], data_year, job_type, segment])
            out.write('\t'.join(row))
            out.write('\n')
    out.seek(0)
    try:
        if area_type == 'wac':
            cursor.copy_from(out, 'work_area_detail')
        else:
            cursor.copy_from(out, 'area_detail')
        conn.commit()
        print 'Loaded %s' % fpath
    except psycopg2.IntegrityError, e:
        print 'Looks like %s was already loaded: %s' % (fpath, e)
        conn.rollback()
    return None

if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    parser.add_argument('--directory', required=True, type=str,
        help="""
            Relative path to directory where your gzipped csv files are located.
        """)
    args = parser.parse_args()
    base_dir = args.directory
    os.path.walk(base_dir, pick_table, None)
