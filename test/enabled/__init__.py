from os import environ
import os
import json
import datetime
from configparser import ConfigParser
from kb_Metrics.metricsdb_controller import MetricsMongoDBController
from bson.objectid import ObjectId
from pymongo import MongoClient

DEBUG = False

def print_debug(msg):
    if not DEBUG:
        return
    t = str(datetime.datetime.now())
    print ("{}:{}".format(t, msg))

def setupModule():
    print_debug ('MODULE - setup')

    token = environ.get('KB_AUTH_TOKEN', None)

    # Deploy Config
    config_file = environ.get('KB_DEPLOYMENT_CONFIG', None)
    cfg = {}
    config = ConfigParser()
    config.read(config_file)
    for nameval in config.items('kb_Metrics'):
        cfg[nameval[0]] = nameval[1]

    # Test Config
    test_cfg_file = '/kb/module/work/test.cfg'
    test_cfg_text = "[test]\n"
    with open(test_cfg_file, "r") as f:
        test_cfg_text += f.read()
    config = ConfigParser()
    config.read_string(test_cfg_text)
    test_cfg_dict = dict(config.items("test"))
    test_cfg = test_cfg_dict

    # Start and populate the test database.
    
    init_mongodb()
    
def teardownModule():
    print_debug('MODULE - teardown')
    client = MongoClient(port=27017)
    dbs = ['workspace', 'exec_engine', 'userjobstate', 'auth2', 'metrics']
    for db in dbs:
        try:
            client[db].command("dropUser", "admin")
            client.drop_database(db)
        except Exception as ex:
            print('ERROR dropping db: ' + str(ex))
    try:
        os.system("sudo service mongodb stop")
    except Exception as ex:
        print('ERROR stopping db: ' + str(ex))
    

def init_mongodb():
    print_debug("MONGO - starting")
    
    client = MongoClient(port=27017)

    print_debug('starting to build local mongoDB')

    os.system("sudo service mongodb start")
    os.system("mongod --version")
    os.system("cat /var/log/mongodb/mongodb.log "
                "| grep 'waiting for connections on port 27017'")

    print_debug("MONGO - ready")

    insert_data(client, 'workspace', 'workspaces')
    insert_data(client, 'exec_engine', 'exec_tasks')
    insert_data(client, 'userjobstate', 'jobstate')
    insert_data(client, 'workspace', 'workspaceObjects')
    insert_data(client, 'auth2', 'users')
    insert_data(client, 'metrics', 'users')
    insert_data(client, 'metrics', 'daily_activities')
    insert_data(client, 'metrics', 'narratives')
    db_names = client.database_names()

    # updating created to timstamp field for userjobstate.jobstate
    for jrecord in client.userjobstate.jobstate.find():
        created_str = jrecord.get('created')
        updated_str = jrecord.get('updated')
        client.userjobstate.jobstate.update_many(
            {"created": created_str},
            {"$set": {"created": datetime.datetime.utcfromtimestamp(
                                    int(created_str) / 1000.0),
                        "updated": datetime.datetime.utcfromtimestamp(
                                    int(updated_str) / 1000.0)}
            }
        )
    # updating data fields from timstamp to datetime.datetime format
    db_coll1 = client.workspace.workspaceObjects
    for wrecord in db_coll1.find():
        moddate_str = wrecord.get('moddate')
        if type(moddate_str) not in [datetime.date, datetime.datetime]:
            moddate = datetime.datetime.utcfromtimestamp(
                                    int(moddate_str) / 1000.0)
            db_coll1.update_many(
                {"moddate": moddate_str},
                {"$set": {"moddate": moddate}},
                upsert=False
            )

    db_coll2 = client.workspace.workspaces
    for wrecord in db_coll2.find():
        moddate_str = wrecord.get('moddate')
        if type(moddate_str) not in [datetime.date, datetime.datetime]:
            moddate = datetime.datetime.utcfromtimestamp(
                                    int(moddate_str) / 1000.0)
            db_coll2.update_many(
                {"moddate": moddate_str},
                {"$set": {"moddate": moddate}},
                upsert=False
            )

    db_coll3 = client.metrics.users
    for urecord in db_coll3.find():
        signup_at_str = urecord.get('signup_at')
        last_signin_at_str = urecord.get('last_signin_at')
        if type(signup_at_str) not in [datetime.date, datetime.datetime]:
            signup_date = datetime.datetime.utcfromtimestamp(
                                int(signup_at_str) / 1000.0)
            signin_date = datetime.datetime.utcfromtimestamp(
                                int(last_signin_at_str) / 1000.0)
            db_coll3.update_many(
                {"signup_at": signup_at_str,
                    "last_signin_at": last_signin_at_str},
                {"$set": {"signup_at": signup_date,
                            "last_signin_at": signin_date}},
                upsert=False
            )

    db_coll4 = client.metrics.narratives
    for urecord in db_coll4.find():
        first_acc_str = urecord.get('first_access')
        last_saved_at_str = urecord.get('last_saved_at')
        if type(first_acc_str) not in [datetime.date, datetime.datetime]:
            first_acc_date = datetime.datetime.utcfromtimestamp(
                                int(first_acc_str) / 1000.0)
            last_saved_date = datetime.datetime.utcfromtimestamp(
                                int(last_saved_at_str) / 1000.0)
            db_coll4.update_many(
                {"first_access": first_acc_str,
                    "last_saved_at": last_saved_at_str},
                {"$set": {"first_access": first_acc_date,
                            "last_saved_at": last_saved_date}},
                upsert=False
            )

    db_coll_au = client.auth2.users
    for urecord in db_coll_au.find():
        create_str = urecord.get('create')
        login_str = urecord.get('login')
        if type(create_str) not in [datetime.date, datetime.datetime]:
            db_coll_au.update_many(
                {"create": create_str, "login": login_str},
                {"$set": {"create": datetime.datetime.utcfromtimestamp(
                                        int(create_str) / 1000.0),
                            "login": datetime.datetime.utcfromtimestamp(
                                        int(login_str) / 1000.0)}},
                upsert=False
            )

    db_names = client.database_names()
    for db in db_names:
        if db != 'local':
            client[db].command("createUser", "admin",
                                    pwd="password", roles=["readWrite"])

def insert_data(client, db_name, table):
    db = client[db_name]

    record_file = os.path.join('db_files',
                                f'ci_{db_name}.{table}.json')
    json_data = open(record_file).read()
    records = json.loads(json_data)

    if table == 'jobstate':
        for record in records:
            record['_id'] = ObjectId(record['_id'])

    db[table].drop()
    db[table].insert_many(records)
    print_debug(f'Inserted {len(records)} records for {db_name}.{table}')