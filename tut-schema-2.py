from mysqlpartialdump import Pk, From

pks = {
    'Customer': Pk(['id']),
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
}

end_sql = ""

start_table = 'Product'
start_where = '1=1'
start_args = []
