import aerospike
from aerospike import predicates as p
import sys
import logging

def add_customer(customer_id, phone_number, lifetime_value, client):
    key = ('test', 'demo', customer_id)
    bins = {
        'phone': phone_number,
        'ltv': lifetime_value
    }
    client.put(key, bins)

def get_ltv_by_id(customer_id, client):
    key = ('test', 'demo', customer_id)
    return client.get(key)

def get_ltv_by_phone(phone_number, client):
    query = client.query('test', 'demo')
    query.select('phone', 'ltv')
    query.where(p.equals('phone', phone_number) )
    return query.results( {'total_timeout':2000})

def print_result(key, metadata, record):
    print(key, metadata, record)        

try:
    config = {
        'hosts': [ ('127.0.0.1', 3000) ]
    }
    client = aerospike.client(config).connect()
except Exception as e:
    print("error: {0}".format(e), file=sys.stderr)
    sys.exit(1)

try:
    client = aerospike.client(config).connect()

    add_customer('2', '+79990000000', '100', client)
    get_ltv_by_id('1', client)
    get_ltv_by_phone('+79990000000', client)
    
    client.close()

except Exception as e:
    print("error: {0}".format(e), file=sys.stderr)
    client.close()
    sys.exit(2)