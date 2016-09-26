__author__ = 'vishi'
from celery import Celery
from celery.signals import worker_process_init, worker_process_shutdown
from lifxlan import *
from sqlalchemy.ext.automap import automap_base
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from datetime import timedelta
import MySQLdb

# BROKER_URL = 'sqla+mysql://root:homeautomation@127.0.0.1/celery'
BROKER_URL = 'sqla+mysql://root@127.0.0.1/celery'
app = Celery('tasks', broker=BROKER_URL)  # amqp://guest@localhost//
app.conf.update(
    CELERYBEAT_SCHEDULE={

        'update-every-30-seconds': {
            'task': 'tasks.schedule_bulb_check',
            'schedule': timedelta(seconds=30),
        },
    },
    CELERY_TIMEZONE='Asia/Kolkata',
    CELERY_TASK_SERIALIZER='json'
)


# DB Connection Optimization
###################################################################
session = None
bulbs = None
db = None
conn = None


def dict_factory(cursor, row):
    d = {}
    for idx, col in enumerate(cursor.description):
        d[col[0]] = row[idx]
    return d


@worker_process_init.connect
def init_worker(**kwargs):
    global session
    global bulbs
    global db
    global conn
    print('Initializing database connection for worker.')
    # db = create_engine('sqlite:///db.sqlite')
    # db.echo = True  # We want to see the SQL we're creating
    # Base = automap_base()
    # Base.prepare(db, reflect=True)
    # bulbs = Base.classes.bulbs
    # session = sessionmaker()
    # session.configure(bind=db)


    conn = MySQLdb.connect(host="127.0.0.1",    # your host, usually localhost
                     user="root",         # your username
                     #passwd="homeautomation",  # your password
                     db="homeautomation")
    conn.row_factory = dict_factory

    # import sqlite3
    # import sys
    #
    # ##########################################
    # sqlite_file = 'db.sqlite'
    # conn = sqlite3.connect(sqlite_file)
    # conn.row_factory = dict_factory

    #########################################


# @worker_process_shutdown.connect
# def shutdown_worker(**kwargs):
#     global bulbs_session
#     if bulbs_session:
#         print('Closing database connection for worker.')

###################################################################
@app.task(bind=True, max_retries=None)
def schedule_bulb_check(self):
    # bulbs_session = session()
    print 'Scheduling Bulb Status Update Tasks'
    # our_bulbs = bulbs_session.query(bulbs)
    # bulbs_list_result = bulbs_session.execute(our_bulbs).fetchall()
    # bulbs_session.commit()

    # print '::::::',bulbs_list_result
    # my_bulbs = []
    # for row in bulbs_session.query(bulbs).all():
    #     print '###########################'
    #     print type(row)
    #     #d = dict(row.items())
    #     #my_bulbs.append({'bulb_id': row[0], 'mac': row[1], 'name': row[2], 'ip': row[3], 'port': row[4], 'power': row[5], 'reachable': row[6]})
    #     print 'Setting up individual bulb update tasks...'
    #     updateBulbStatus.apply_async(args=[row])
    #     print "Task scheduled for :",row[0]," : ",row[3]
    # return 'success'
    print "Opened database successfully";
    cursor = conn.cursor();
    cursor.execute('''SELECT bulb_id, mac, name, ip, port, power, reachable, h, s , b, k FROM bulbs''')
    # bulbs =[]
    for row in cursor.fetchall():
        # bulb_id, mac, name, ip, port, power, reachable = row
        print row
        # bulbs.append(row)
        updateBulbStatus.apply_async(args=[row])
    cursor.close()


@app.task(bind=True, max_retries=None)
def updateBulbStatus(self, our_bulb):
    # bulbs_session = session()
    cursor = conn.cursor();
    try:
        # our_bulb = bulbs_session.query(bulbs).filter(bulbs.mac == str(row[1])).first()
        # our_bulb=row

        # print '>>>', our_bulb
        # print type(our_bulb)
        light = Light(our_bulb['mac'], our_bulb['ip'])
        hsbk = light.get_color()
        our_bulb['port'] = light.get_port()
        our_bulb['power'] = light.get_power()
        our_bulb['reachable'] = 1
        our_bulb['h'] = hsbk[0]
        our_bulb['s'] = hsbk[1]
        our_bulb['b'] = hsbk[2]
        our_bulb['k'] = hsbk[3]

        query = "update bulbs set port={0}, power={1}, reachable={2}, h={3}, s={4}, b={5}, k={6} where mac='{7}'".format(
            our_bulb['port'], our_bulb['power'], our_bulb['reachable'],
            our_bulb['h'], our_bulb['s'], our_bulb['b'], our_bulb['k'], our_bulb['mac'])
        print query


        # print 'Bulb found, updating' + row[2] + ' : ' + row[1] + ' : ' + row[3]
        # print light
    except Exception:
        # pass
        our_bulb['reachable'] = 0
        query = "update bulbs set reachable={0} where mac='{1}'".format(0, our_bulb['mac'])
        print query
        # cursor.execute(query);

        # print 'Bulb '+row[2]+' : '+row[1]+' : '+row[3]+' not reachable'
    finally:
        cursor.execute(query);
        cursor.close()
        # bulbs_session.commit()
    return 'success'
