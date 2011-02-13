from mysqlpartialdump import Pk, From

def clean_email(row):
    row['email'] = "%s%d"%(row['email'][:3], hash(row['email']))
    return row

pks = {
    'Customer': Pk(['id']).in_batches(1),
    'Order': Pk(['id']),
    'OrderLine': Pk(['id']),
    'Product': Pk(['id']),
}

relationships = [
    From('Customer', 'id').to('Order', 'customer_id').bidirectional(),
    From('Order', 'id').to('OrderLine', 'order_id').bidirectional(),
    From('OrderLine', 'product_id').to('Product', 'id').bidirectional(),
]

callbacks = {
    'Customer': clean_email,
}

end_sql = ""

start_table = 'Product'
start_where = '1=1'
start_args = []
