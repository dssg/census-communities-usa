from celery import Celery
import pymongo, os
from pymongo.read_preferences import ReadPreference

celery = Celery('tasks')
celery.config_from_object('celeryconfig')

MONGO_HOST = os.environ.get('MONGO_HOST')
if not MONGO_HOST:
    MONGO_HOST = 'localhost'

WRITE_CONN = pymongo.MongoReplicaSetClient(MONGO_HOST, replicaSet='rs0')
WRITE_DB = WRITE_CONN['census']

READ_DB = pymongo.MongoReplicaSetClient('%s:27017' % MONGO_HOST, replicaSet='rs0').census
READ_DB.read_preference = ReadPreference.SECONDARY_PREFERRED

@celery.task
def geocode(coll, obj_ids):
    for obj_id in obj_ids:
        read_from = READ_DB[coll]
        write_to = WRITE_DB[coll]
        row = read_from.find_one({'_id': obj_id})
        update = {'$set': {}}
        if coll in ['origin_destination', 'residence_area']:
            home_geo_xwalk = READ_DB['geo_xwalk'].find_one({'tabblk2010': row['h_geocode']})
            update['$set']['home_state_abrv'] =         home_geo_xwalk['stusps']
            update['$set']['home_state_name'] =         home_geo_xwalk['stname']
            update['$set']['home_county_name'] =        home_geo_xwalk['ctyname']
            update['$set']['home_census_tract_name'] =  home_geo_xwalk['trctname']
            update['$set']['home_census_bgrp_name'] =   home_geo_xwalk['bgrpname']
            update['$set']['home_zcta_code'] =          home_geo_xwalk['zcta']
            update['$set']['home_zcta_name'] =          home_geo_xwalk['zctaname']
            update['$set']['home_place_code'] =         home_geo_xwalk['stplc']
            update['$set']['home_place_name'] =         home_geo_xwalk['stplcname']
            update['$set']['home_cong_dist_code'] =     home_geo_xwalk['stcd113']
            update['$set']['home_cong_dist_name'] =     home_geo_xwalk['stcd113name']
            update['$set']['home_st_leg_lower_code'] =  home_geo_xwalk['stsldl']
            update['$set']['home_st_leg_lower_name'] =  home_geo_xwalk['stsldlname']
            update['$set']['home_st_leg_upper_code'] =  home_geo_xwalk['stsldu']
            update['$set']['home_st_leg_upper_name'] =  home_geo_xwalk['stslduname']
        if coll in ['origin_destination', 'work_area']
            work_geo_xwalk = READ_DB['geo_xwalk'].find_one({'tabblk2010': row['w_geocode']})
            update['$set']['work_state_abrv'] =         work_geo_xwalk['stusps']
            update['$set']['work_state_name'] =         work_geo_xwalk['stname']
            update['$set']['work_county_name'] =        work_geo_xwalk['ctyname']
            update['$set']['work_census_tract_name'] =  work_geo_xwalk['trctname']
            update['$set']['work_census_bgrp_name'] =   work_geo_xwalk['bgrpname']
            update['$set']['work_zcta_code'] =          work_geo_xwalk['zcta']
            update['$set']['work_zcta_name'] =          work_geo_xwalk['zctaname']
            update['$set']['work_place_code'] =         work_geo_xwalk['stplc']
            update['$set']['work_place_name'] =         work_geo_xwalk['stplcname']
            update['$set']['work_cong_dist_code'] =     work_geo_xwalk['stcd113']
            update['$set']['work_cong_dist_name'] =     work_geo_xwalk['stcd113name']
            update['$set']['work_st_leg_lower_code'] =  work_geo_xwalk['stsldl']
            update['$set']['work_st_leg_lower_name'] =  work_geo_xwalk['stsldlname']
            update['$set']['work_st_leg_upper_code'] =  work_geo_xwalk['stsldu']
            update['$set']['work_st_leg_upper_name'] =  work_geo_xwalk['stslduname']
        write_to.update({'_id': obj_id}, update)
    return 'Geocoded %s docs' % len(obj_ids)
