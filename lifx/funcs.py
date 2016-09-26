#from homeautomation import send_message

__author__ = 'vishi'
from lifxlan import *
from sqlalchemy.ext.automap import automap_base
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

def discovery():
    import sys
    # def dict_factory(cursor, row):
    #     d = {}
    #     for idx, col in enumerate(cursor.description):
    #         d[col[0]] = row[idx]
    #     return d
    ###########################################
    # sqlite_file = 'db/db.sqlite'
    # conn = sqlite3.connect(sqlite_file)
    # conn.row_factory = dict_factory
    # cursor = conn.cursor()
    ##########################################

    db = create_engine('mysql+pymysql://root@127.0.0.1/homeautomation?host=127.0.0.1?port=3306')
    db.echo = True  # We want to see the SQL we're creating
    Base = automap_base()
    Base.prepare(db, reflect=True)

    bulbs = Base.classes.bulbs

    session = sessionmaker()
    session.configure(bind=db)
    bulbs_session = session()
    our_bulb = bulbs_session.query(bulbs)
    bulbs_list_result = bulbs_session.execute(our_bulb).fetchall()
    bulbs_session.commit()

    print '::::::',bulbs_list_result
    my_bulbs = []
    for row in bulbs_list_result:
        my_bulbs.append({'bulb_id': row[0], 'mac': row[1], 'name': row[2], 'ip': row[3], 'port': row[4], 'power': row[5], 'reachable': row[6]})

    # print "Opened database successfully";
    # cursor.execute('''SELECT bulb_id, mac, name, ip, port, power, reachable FROM bulbs''')
    # bulbs =[]
    # for row in cursor.fetchall():
    #     bulb_id, mac, name, ip, port, power, reachable = row
    #     bulbs.append(row)

    for b in my_bulbs:
         print b['bulb_id'],b['mac'],b['name'],b['ip'],b['port'],b['power'],b['reachable']

    num_lights = None
    if len(sys.argv) != 2:
        print "\nDiscovery will go much faster if you provide the number of lights on your LAN:"
        print "  python {} <number of lights on LAN>\n".format(sys.argv[0])
    else:
        num_lights = int(sys.argv[1])

    # instantiate LifxLAN client, num_lights may be None (unknown).
    # In fact, you don't need to provide LifxLAN with the number of bulbs at all.
    # lifx = LifxLAN() works just as well. Knowing the number of bulbs in advance
    # simply makes initial bulb discovery faster.
    print "Discovering lights..."
    lifx = LifxLAN(num_lights)

    # get devices
    devices = lifx.get_lights()
    print "\nFound {} light(s):\n".format(len(devices))
    for d in devices:
        our_bulb = bulbs_session.query(bulbs).filter(bulbs.mac == d.get_mac_addr()).first()
        light = Light(d.get_mac_addr(), d.get_ip_addr())
        hsbk = light.get_color()
        print hsbk
        data=our_bulb
        if data is None:
            print 'There is no bulb with mac %s'%d.get_mac_addr()
            print "Inserting: ",d.get_mac_addr(),d.get_label(),d.get_ip_addr(),d.get_port(),d.get_power(),1
            bulb=bulbs(mac=d.get_mac_addr(),
                        name=d.get_label(),
                        ip=d.get_ip_addr(),
                        port=d.get_port(),
                        power=d.get_power(),
                        reachable=1,
                        h=hsbk[0],
                        s=hsbk[1],
                        b=hsbk[2],
                        k=hsbk[3]
                        )
            bulbs_session.add(bulb)
            #send_message("Bulb Added")
            #bulbs_session.commit()
        else:
            print 'Bulb found, updating',(d.get_mac_addr())
            our_bulb.name=d.get_label()
            our_bulb.ip=d.get_ip_addr()
            our_bulb.port=d.get_port()
            our_bulb.power=d.get_power()
            our_bulb.reachable=1
            our_bulb.h=hsbk[0]
            our_bulb.s=hsbk[1]
            our_bulb.b=hsbk[2]
            our_bulb.k=hsbk[3]
            #send_message('Bulb updated HHHHHHHHH')
        bulbs_session.commit()

    bulbs_session.close_all()
            # cursor.execute('''UPDATE bulbs set name=?,ip=?,port=?,power=?,reachable=? WHERE mac=?''',
            #               (d.get_label(),d.get_ip_addr(),d.get_port(),d.get_power(),1,d.get_mac_addr()))
        #print d
    # conn.commit()
    # cursor.close()