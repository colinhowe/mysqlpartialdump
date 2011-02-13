================
MySQLPartialDump
================
:Info: MySQLPartialDump is a tool for MySQL that makes it easier to dump a section of a database in a meaningful way.
:Author: Colin Howe (http://github.com/colinhowe)

About
=====
MySQLPartialDump is a tool for MySQL that makes it easier to dump a 
section of a database in a meaningful way. A full tutorial and explanation
is below.

Dependencies
============
python-MySQLdb

Tutorial
========

Imagine you run an online store and you have Customers and Orders. Each
customer can have multiple orders. You want to get all the data for a customer
as a SQL dump so that you can import it locally and do some testing with real
data.

For the example, the customer table has these columns:
* id - The unique ID for each customer
* email address - The e-mail address of the customer

The order table has these columns:
* id - The ID of the order
* customer_id - A reference back to the customer table for the customer that
  placed this order

Each order can have multiple items on it. These are represented as OrderLines:
* id - ID of the line
* order_id - The owning order
* product_id - The product for this line
* quantity - The amount of product

The product table has these columns:
* id - ID of the product
* name - Name of the product

Create these tables in your local database with the following SQL::
    CREATE DATABASE `dumper_tutorial` CHARACTER SET utf8 COLLATE utf8_general_ci;
    USE `dumper_tutorial`;
    CREATE TABLE `dumper_tutorial`.`Customer`(
        `id` INT NOT NULL AUTO_INCREMENT, 
        `email` VARCHAR(320), 
        PRIMARY KEY(`id`));
    CREATE TABLE `dumper_tutorial`.`Order`(
        `id` INT NOT NULL AUTO_INCREMENT,
        `customer_id` INT,
        PRIMARY KEY(`id`));
    CREATE TABLE `dumper_tutorial`.`OrderLine`(
        `id` INT NOT NULL AUTO_INCREMENT,
        `order_id` INT,
        `product_id` INT,
        `quantity` INT,
        PRIMARY KEY(`id`));
    CREATE TABLE `dumper_tutorial`.`Product`(
        `id` INT NOT NULL AUTO_INCREMENT,
        `name` VARCHAR(200),
        PRIMARY KEY(`id`));

To get you started here is some SQL to populate the database with two customers
and some orders orders::
    USE `dumper_tutorial`;
    INSERT INTO `Customer`(`id`, `email`) VALUES
        (1, 'colin@mailinator.com'),
        (2, 'bob@mailinator.com');
    INSERT INTO `Order`(`id`, `customer_id`) VALUES
        (1, 1),
        (2, 2),
        (3, 2),
        (4, 1),
        (5, 1);
    INSERT INTO `OrderLine`(`id`, `order_id`, `product_id`, `quantity`) VALUES
        (1, 1, 1, 2),
        (2, 2, 2, 1),
        (3, 3, 3, 1),
        (4, 3, 2, 2),
        (5, 4, 2, 1),
        (6, 5, 3, 1);
    INSERT INTO `Product`(`id`, `name`) VALUES
        (1, 'Peopleware'),
        (2, 'Alice in Wonderland'),
        (3, 'Scrum in Practice');

MySQLPartialDump needs you to tell it how to crawl the database. You do this
with a dump schema written in Python.

A simple schema for the above would be::
    from mysqlpartialdump import Pk, From

    pks = {
        'Customer':Pk(['id']),
        'Order':Pk(['id']),
        'OrderLine':Pk(['id']),
        'Product':Pk(['id']),
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

This is in the git repo as tut-schema-1.py.

You will need to set your database details at the top of the schema. You 
can then run a dump like so::
    python mysqlpartialdump.py -u <username> -s <password> -d dumper_tutorial tut-schema-1.py

This will create an SQL dump called dump.sql.0 that contains only the
information related to customer 1.

Selecting the start points
--------------------------

The start point for a crawl of the database is controlled by three variables:
* start_table
* start_where
* start_args

These can be used together to get any set of rows from a single table. 
Try changing them to be::
    start_where = '1=1'
    start_args = []

This will output all customers in the database.

Specifying relationships
------------------------

Relationships are all stored in the relationships variable and are written 
using a simple DSL. By default all relationships go in one direction. Try
this::
    start_table = 'Product'
    start_where = '1=1'
    start_args = []

This will give you a table of all the products but won't give you any orders
for the products. To make this work you have to make the relationships
bidirectional::
    relationships = [
        From('Customer', 'id').to('Order', 'customer_id').bidirectional(),
        From('Order', 'id').to('OrderLine', 'order_id').bidirectional(),
        From('OrderLine', 'product_id').to('Product', 'id').bidirectional(),
    ]

This schema is saved in tut-schema-2.py. Doing a dump with this schema will
give the whole database. It is easy to change it to give you all orders
(and the customers who placed the order) for a single product.

Cleansing Data
--------------

We've just been a little naughty and taken a copy of our customers - complete
with e-mail addresses. This is the sort of thing that can lead to disaster!

This is where the callbacks section comes in handy. You can create a callback
to make the e-mail addresses safe to distribute. Add the following to your
schema (a full copy is in tut-schema-3.py)::
    def clean_email(row):
        row['email'] = "%s%d"%(row['email'][:3], hash(row['email']))
        return row

Then alter callbacks to be::
    callbacks = {
        'Customer': clean_email,
    }

This will call clean_email for every single row in the Customer table. This
will give us a copy of the database that is safer to distribute as it now has
no e-mail addresses in it.

Batch sizes
-----------

Some tables can be quite wide and doing bulk inserts to these tables may need
fine tuning. To do this you specify a batch size when creating the primary
keys::
    pks = {
        'Customer': Pk(['id']).in_batches(1),
        'Order': Pk(['id']),
        'OrderLine': Pk(['id']),
        'Product': Pk(['id']),
    }

If you run this (tut-schema-4.py) and look at dump.sql.0 you will see that the
Customer table has two inserts instead of one.

Large datasets and cycles
-------------------------

MySQLPartialDump will, by default, keep a record of all the primary keys of rows 
it has seen. It uses this information to prevent duplicate rows being inserted.
This is why the example using bidirectional relationships doesn't loop forever.

You can disable this behaviour when you create the primary keys. However, this
can lead to duplicate inserts into the database (which may fail) or, worse, a
dump that never ends.

You can create a dump schema (tut-schema-5.py) that won't import by changing the 
primary keys as follows::
    from mysqlpartialdump import NO_KEY_CACHE
    pks = {
        'Customer': Pk(['id'], NO_KEY_CACHE),
        'Order': Pk(['id']).in_batches(1),
        'OrderLine': Pk(['id']),
        'Product': Pk(['id']),
    }

Here we have used NO_KEY_CACHE as an option to the primary key. This option
turns off the key caching described above. By combining this with batching
Order in batches of 1 we will get a single Customer row insert for each Order::
    INSERT  INTO Order(`id`,`customer_id`) VALUES(2,2);
    INSERT  INTO Customer(`id`,`email`) VALUES(2,'bob-3439811783597610316');
    INSERT  INTO Order(`id`,`customer_id`) VALUES(3,2);
    INSERT  INTO Customer(`id`,`email`) VALUES(2,'bob-3439811783597610316');

This will fail on the second insert to Customer due to a primary key conflict.
To solve this we can specify that duplicates can be ignored (tut-schema-6.py)::
    from mysqlpartialdump import NO_KEY_CACHE, ALLOW_DUPLICATES
    pks = {
        'Customer': Pk(['id'], NO_KEY_CACHE, ALLOW_DUPLICATES),
        'Order': Pk(['id']).in_batches(1),
        'OrderLine': Pk(['id']),
        'Product': Pk(['id']),
    }

This generates SQL like the following:
    INSERT  INTO Order(`id`,`customer_id`) VALUES(2,2);
    INSERT IGNORE INTO Customer(`id`,`email`) VALUES(2,'bob-3439811783597610316');
    INSERT  INTO Order(`id`,`customer_id`) VALUES(3,2);
    INSERT IGNORE INTO Customer(`id`,`email`) VALUES(2,'bob-3439811783597610316');

The use of INSERT IGNORE instructs MySQL to ignore duplicate rows.

Arbitrary SQL
-------------

You may have noticed the end_sql variable in the dump schemas shown so far.
This is used to add any arbitrary SQL at the end of a dump - such as
recalculating tables that store calculated values for quick lookup.

Chunking
--------

Importing a big dump can be time consuming. It can be done quicker if the dump
is split in to multiple files and each imported simultaneously. This can be
achieved with the command line option chunks::
    python mysqlpartialdump.py -u <username> -s <password> -d dumper_tutorial --chunks=2 tut-schema-1.py

Each chunk will be output with a number at the end. In this case: dump.sql.0
and dump.sql.1 will be created.

Complex relationships
---------------------

Some databases have complex relationships where a row may depend on a row from
a table that is determined by some value in the row. For example:
* our Product table could have a type column that is either 'book', 'dvd' or
  'other'
* If the type is 'book' then there is an associated row in the Book table
* If the type is 'dvd' then there is an associated row in Dvd table
* If the type is 'other' then there is no associated row in any table
  
This cannot be modelled with a simple static relationship. Instead you must use
a callback::
    def get_product_rel(row):
        if row['type'] == 'book':
            return ('Book', ('product_id', row['id']))
        elif row['type'] == 'dvd':
            return ('Dvd', ('product_id', row['id']))
        else:
            return None

Controlling the output prefix
-----------------------------

By default all output goes to a set of files starting with 'dump.sql'. This can
be changed with the command line option --output.

Gotchas
=======

Foreign keys are disabled
-------------------------

Foreign keys are disabled in the dumps. This is to prevent errors if you have 
foreign key constrains enabled.

No transactions
---------------

The dumps can get very large. For this reason transactions are NOT used in the
dumps.

Where to get help
=================

I'm on Twitter @colinhowe and also on github at http://github.com/colinhowe/
