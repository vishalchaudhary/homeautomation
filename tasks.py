__author__ = 'vishi'
from celery import Celery
from celery.signals import worker_process_init, worker_process_shutdown
from lifxlan import *
from sqlalchemy.ext.automap import automap_base
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from datetime import timedelta

BROKER_URL = 'sqla+sqlite:///celerydb.sqlite'
app = Celery('tasks', broker=BROKER_URL)  #amqp://guest@localhost//
app.conf.update(
    CELERYBEAT_SCHEDULE = {

        'update-every-30-seconds': {
            'task': 'tasks.schedule_bulb_check',
            'schedule': timedelta(seconds=30),
        },
    },
    CELERY_TIMEZONE = 'Asia/Kolkata'
)
CELERY_TASK_SERIALIZER = 'json'

# DB Connection Optimization
###################################################################
session = None
bulbs = None
db = None

@worker_process_init.connect
def init_worker(**kwargs):
    global session
    global bulbs
    global db
    print('Initializing database connection for worker.')
    db = create_engine('sqlite:///db.sqlite')
    db.echo = True  # We want to see the SQL we're creating
    Base = automap_base()
    Base.prepare(db, reflect=True)
    bulbs = Base.classes.bulbs
    session = sessionmaker()
    session.configure(bind=db)



# @worker_process_shutdown.connect
# def shutdown_worker(**kwargs):
#     global bulbs_session
#     if bulbs_session:
#         print('Closing database connection for worker.')

###################################################################
@app.task(bind=True, max_retries = None)
def schedule_bulb_check(self):
    bulbs_session = session()
    print 'Scheduling Bulb Status Update Tasks'
    #our_bulbs = bulbs_session.query(bulbs)
    #bulbs_list_result = bulbs_session.execute(our_bulbs).fetchall()
    #bulbs_session.commit()

    #print '::::::',bulbs_list_result
    #my_bulbs = []
    for row in bulbs_session.query(bulbs).all():
        print '###########################'
        print type(row)
        #d = dict(row.items())
        #my_bulbs.append({'bulb_id': row[0], 'mac': row[1], 'name': row[2], 'ip': row[3], 'port': row[4], 'power': row[5], 'reachable': row[6]})
        print 'Setting up individual bulb update tasks...'
        updateBulbStatus.apply_async(args=[row])
        print "Task scheduled for :",row[0]," : ",row[3]
    return 'success'
    # print "Opened database successfully";
    # cursor.execute('''SELECT bulb_id, mac, name, ip, port, power, reachable FROM bulbs''')
    # bulbs =[]
    # for row in cursor.fetchall():
    #     bulb_id, mac, name, ip, port, power, reachable = row
    #     bulbs.append(row)
    # for b in my_bulbs:
    #      print b['bulb_id'],b['mac'],b['name'],b['ip'],b['port'],b['power'],b['reachable']
    #bulbs_session.close_all()

@app.task(bind=True, max_retries = None)
def updateBulbStatus(self, our_bulb):
    bulbs_session = session()
    try:
        #our_bulb = bulbs_session.query(bulbs).filter(bulbs.mac == str(row[1])).first()
        #our_bulb=row
        print '>>>', our_bulb
        print type(our_bulb)
        light = Light(str(row[1]),str(row[3]))
        hsbk = light.get_color()
        our_bulb.port = light.get_port()
        our_bulb.power = light.get_power()
        our_bulb.reachable = 1
        our_bulb.h = hsbk[0]
        our_bulb.s = hsbk[1]
        our_bulb.b = hsbk[2]
        our_bulb.k = hsbk[3]
        print 'Bulb found, updating' + row[2] + ' : ' + row[1] + ' : ' + row[3]
        print light
    except Exception:
        #pass
        our_bulb.reachable=0
        print 'Bulb '+row[2]+' : '+row[1]+' : '+row[3]+' not reachable'
    finally:
        bulbs_session.commit()
    return 'success'
