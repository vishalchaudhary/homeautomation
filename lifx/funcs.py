__author__ = 'vishi'
from lifxlan import *
def discovery():
    import sqlite3
    import os
    import sys

    def dict_factory(cursor, row):
        d = {}
        for idx, col in enumerate(cursor.description):
            d[col[0]] = row[idx]
        return d
    sqlite_file = 'db/db.sqlite'
    conn = sqlite3.connect(sqlite_file)
    conn.row_factory = dict_factory
    cursor = conn.cursor()
    print "Opened database successfully";
    cursor.execute('''SELECT bulb_id, mac, name, ip, port, power, reachable FROM bulbs''')
    bulbs =[]
    for row in cursor.fetchall():
        bulb_id, mac, name, ip, port, power, reachable = row
        bulbs.append(row)

    for b in bulbs:
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
        cursor.execute("SELECT mac FROM bulbs WHERE mac = ?", (d.get_mac_addr(),))
        data=cursor.fetchone()
        if data is None:
            print 'There is no bulb with mac %s'%d.get_mac_addr()
            print "Inserting: ",d.get_mac_addr(),d.get_label(),d.get_ip_addr(),d.get_port(),d.get_power(),1
            cursor.execute('''INSERT INTO bulbs(mac,name,ip,port,power,reachable)
                          VALUES(?,?,?,?,?,?)''', (d.get_mac_addr(),d.get_label(),d.get_ip_addr(),d.get_port(),d.get_power(),1))
        else:
            print 'Bulb found, updating',(d.get_mac_addr())
            cursor.execute('''UPDATE bulbs set name=?,ip=?,port=?,power=?,reachable=? WHERE mac=?''',
                          (d.get_label(),d.get_ip_addr(),d.get_port(),d.get_power(),1,d.get_mac_addr()))
        print d
    conn.commit()
    cursor.close()