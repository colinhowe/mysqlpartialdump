from mysqlpartialdump import Pk, From

pks = {
    'Customer': Pk(['id']),
    'Order': Pk(['id']),
    'OrderLine': Pk(['id']),
    'Product': Pk(['id']),
}

relationships = [
    From('Customer', 'id').to('Order', 'customer_id'),
    From('Order', 'id').to('OrderLine', 'order_id'),
    From('OrderLine', 'product_id').to('Product', 'id'),
]

callbacks = {
}

end_sql = ""

start_table = 'Customer'
start_where = 'id=%s'
start_args = ['1']
