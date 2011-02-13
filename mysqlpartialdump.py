import MySQLdb
from MySQLdb import cursors
import sys
import argparse
from sys import stderr
from datetime import datetime
from collections import defaultdict
import codecs

BULK_INSERT_SIZE = 5000

LOG_NONE = 0
LOG_INFO = 1
LOG_DEBUG = 2
DEBUG_LEVEL = LOG_NONE

BIDIRECTIONAL = 'bidirectional'
ALLOW_DUPLICATES = 'allow duplicates'
NO_KEY_CACHE = 'no key cache'

def get_schema(cursor, name):
    cursor.execute("DESCRIBE `%s`"%name)
    return cursor.fetchall()

def debug(msg):
    if DEBUG_LEVEL >= LOG_DEBUG:
        stderr.write('DEBUG: %s %s\n'%(datetime.now(), msg))

def info(msg):
    if DEBUG_LEVEL >= LOG_INFO:
        stderr.write('INFO: %s %s\n'%(datetime.now(), msg[:100]))

def make_safe(value):
    if value is None:
        return 'NULL'
    if not isinstance(value, basestring):
        return str(value)
    value = value.replace("'", "''").replace("\\", "\\\\")
    return "'%s'"%value

class Pk(object):
    def __init__(self, columns, *options):
        self.columns = columns
        self.options = set(options)
        self.batch_size = BULK_INSERT_SIZE

    def in_batches(self, batch_size):
        self.batch_size = batch_size
        return self

    def __repr__(self):
        return "%s (%s) in batches of %d"%(
                self.columns, ", ".join(self.options), self.batch_size)

class CustomRelationship(object):
    """Defines a custom relationship from one table using a custom callback.
    The callback will be passed a row and must return a tuple of the form:
       (table_name, (column_1, value_1), (column_2, value_2), ...)
    If no relationship should be returned then None can be returned. E.g.

    >>> def callback(row):
    ...     if row['has_pet']:
    ...          return ('pet', ('owner_id', row['id']))
    ...     return None
    """
    def __init__(self, from_table, callback):
        self.from_table = from_table
        self.callback = callback

    def create_callbacks(self):
        return [(self.from_table, self.callback)]

class Relationship(object):
    """Defines a relationship from one table to another. Should not be used
    directly but instead From should be used:
    >>> From('source_table', 'id').to('to_table', 'some_id')
    <mysqlpartialdump.Relationship object at 0x7fe9b32a5ad0>
    """
    def __init__(self, 
            from_table, from_columns, 
            to_table=None, to_columns=None):
        self.from_table = from_table
        self.from_columns = from_columns
        self.to_table = to_table
        self.to_columns = to_columns
        self.options = set()

    def to(self, to_table, *to_columns):
        self.to_table = to_table
        self.to_columns = to_columns
        return self

    def bidirectional(self):
        self.options.add(BIDIRECTIONAL)
        return self

    def create_callbacks(self):
        callbacks = []
        def create_callback(from_columns, to_table, to_columns):
            def callback(row):
                col_pairs = zip(from_columns, to_columns)
                target = [(to_col, row[src_col]) 
                          for (src_col, to_col) in col_pairs]
                return (to_table, target)
            return callback

        callback = create_callback(self.from_columns, 
                                   self.to_table, self.to_columns)
        callbacks.append((self.from_table, callback))
        
        if BIDIRECTIONAL in self.options:
            callback = create_callback(self.to_columns, 
                                       self.from_table, self.from_columns)
            callbacks.append((self.to_table, callback))

        return callbacks

    def __str__(self):
        return "%s %s -> %s %s [%s]"%(
                self.from_table, self.from_columns,
                self.to_table, self.to_columns,
                self.options)


def From(table, *columns):
    """Starting point for a DSL to create relationships. Usage:
    >>> From('source_table', 'id').to('to_table', 'some_id')
    <mysqlpartialdump.Relationship object at 0x7fe9b32a5ad0>
    """
    return Relationship(table, columns)

class Dumper(object):
    def __init__(
            self,
            relationships,
            pks,
            callbacks,
            db_address,
            db_port,
            db_username,
            db_password,
            db_name,
            start_table,
            start_where,
            start_args=[],
            end_sql='',
            chunks=1,
            output_prefix='dump.sql'
            ):
        self.relationships = relationships
        self.pks = pks
        self.callbacks = callbacks
        self.db_address = db_address
        self.db_port = db_port
        self.db_username = db_username
        self.db_password = db_password
        self.db_name = db_name
        self.start_table = start_table
        self.start_where = start_where
        self.start_args = start_args
        self.end_sql = end_sql
        self.chunks = chunks
        self.output_prefix = output_prefix

        self.cached_schemas = {}

    def _get_writer(self):
        '''Gets the writer with the least data in it. This helps keep files
        balanced if using multiple chunks for output'''
        writers_with_size = []
        for writer in self.writers:
            writers_with_size.append((writer, writer.tell()))
        return sorted(writers_with_size, key=lambda t: t[1])[0][0]

    def _create_writers(self):
        self.writers = []
        for chunk in range(self.chunks):
            writer = open("%s.%d"%(self.output_prefix, chunk), 'w')
            writer = codecs.getwriter('utf8')(writer)
            self.writers.append(writer)
            writer.write('SET FOREIGN_KEY_CHECKS=0;\n')

    def _connect_to_db(self):
        self.db = MySQLdb.connect(
                user=self.db_username,
                passwd=self.db_password,
                db=self.db_name,
                host=self.db_address,
                port=self.db_port,
                charset='utf8',
                cursorclass=cursors.SSCursor)
        self.cursor = self.db.cursor()
        self.cursor.execute('SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ')
        self.cursor.execute('START TRANSACTION')

    def _close_db(self):
        self.cursor.execute('ROLLBACK')
        self.cursor.close()
        self.db.close()

    def _close_writers(self):
        for writer in self.writers:
            writer.write('SET FOREIGN_KEY_CHECKS=1;\n')
            writer.close()

    def _create_callbacks(self):
        # Storing the relationships as:
        #   { table_name: callback }
        # Is a lot quicker than keeping it in a list
        rels = defaultdict(set)
        for relationship in self.relationships:
            for (table, callback) in relationship.create_callbacks():
                rels[table].add(callback)
        self.relationships = rels

    def go(self):
        self.pks_seen = dict([(name, set()) for name in self.pks.keys()])
        
        self._create_writers()
        self._create_callbacks()

        self._connect_to_db()
        self._get_table(self.start_table, where=self.start_where, where_args=self.start_args)
        self._close_db()

        self._get_writer().write(self.end_sql)

        self._close_writers()

    def _get_schema(self, table_name):
        '''Gets the schema of the given table. Will call to the database to
        get the schema if it hasn't been explored before'''
        if table_name not in self.cached_schemas:
            schema = get_schema(self.cursor, table_name)
            safe_col_names = ["`%s`"%row[0] for row in schema]
            unsafe_col_names = [row[0] for row in schema]
            col_offsets = dict([(row[0], i) for i, row in enumerate(schema)])
            self.cached_schemas[table_name] = (
                    safe_col_names,
                    unsafe_col_names,
                    col_offsets)

        return self.cached_schemas[table_name]
       
    def _do_follows(self, to_follow):
        debug('PKs seen: %s'%self.pks_seen)
        debug('To follow: %s'%to_follow)
        for table, follow_sets in to_follow.iteritems():
            follow_sets_keys = list(follow_sets.keys())
            for col_names in follow_sets_keys:
                value_sets = follow_sets[col_names]
                if col_names == tuple(self.pks[table].columns):
                    values = []
                    for value_tuple in value_sets:
                        if value_tuple not in self.pks_seen[table]:
                            values.append(value_tuple)
                else:
                    info('Not killing follows for %s %s'%(col_names, table))
                    values = list(value_sets)

                batch_size = self.pks[table].batch_size

                while len(values) > 0:
                    values_to_follow = values[:batch_size]
                    del(values[:batch_size])
                    clauses = []
                    args = []
                    clause = " AND ".join(["%s = %%s"%col for col in col_names])
                    clauses = [clause] * len(values_to_follow)
                    for value in values_to_follow:
                        args += [val for val in value]
                    debug('Clauses to follow: %s'%clauses)
                    info('Following %s with %s'%(table, values_to_follow))
                    where = " OR ".join(clauses)
                    self._get_table(table, where, args)
                del(follow_sets[col_names])

    def _get_pk_value(self, table_name, row):
        (_, _, offsets) = self._get_schema(table_name)
        pk_columns = self.pks[table_name].columns
        return tuple([row[offsets[col]] for col in pk_columns])

    def is_row_seen(self, table_name, row):
        pk = self._get_pk_value(table_name, row)
        if pk in self.pks_seen[table_name]:
            debug('PK %s seen in %s'%(pk, table_name))
            return True
        else:
            debug('PK %s not seen in %s'%(pk, table_name))
            return False

    def add_row(self, table_name, row):
        pk = self._get_pk_value(table_name, row)
        if NO_KEY_CACHE in self.pks[table_name].options:
            return True
        if pk in self.pks_seen[table_name]:
            return False
        self.pks_seen[table_name].add(pk)
        return True

    def _remove_seen_rows(self, table_name, rows):
        if table_name not in self.pks:
            raise Exception('PK not created for %s'%table_name)
        rows = [row for row in rows if self.add_row(table_name, row)]
        return rows

    def _row_dict(self, row, col_offsets):
        return dict([(col, row[i]) for col, i in col_offsets.items()])

    def _calculate_follows(self, table_name, rows, to_follow):
        (safe_col_names, unsafe_col_names, col_offsets) = \
                self._get_schema(table_name)
        for callback in self.relationships[table_name]:
            for row in rows:
                row = callback(self._row_dict(row, col_offsets))
                if row is None:
                    continue

                target_name = row[0]
                keys = row[1]

                (col_names, values) = zip(*keys)
                to_follow[target_name][col_names].add(values)

    def _write_rows(self, table_name, rows):
        (safe_col_names, unsafe_col_names, col_offsets) = \
                self._get_schema(table_name)
        allow_duplicates = ALLOW_DUPLICATES in self.pks[table_name].options

        result = self._get_writer()
        result.write('INSERT %s INTO %s(%s) VALUES'%(
            "IGNORE" if allow_duplicates else "",
            table_name, 
            ",".join(safe_col_names)))

        row_strings = []
        for row in rows:
            callback = self.callbacks.get(table_name, None)
            if callback:
                row_dict = callback(self._row_dict(row, col_offsets))
                row = [row_dict[col] for col in unsafe_col_names]
            row_strings.append(
                '(%s)'%",".join([make_safe(value) for value in row]))
        result.write(",\n".join(row_strings))
        result.write(';\n')

    def _get_table(self, table_name, where=None, where_args=[]):
        info('Exploring %s with where %s and args %s'%(table_name, where, where_args))
        
        (safe_col_names, _, _) = self._get_schema(table_name)
        self.cursor.execute(
                "SELECT %s FROM `%s` WHERE %s"%( 
                    ",".join(safe_col_names),
                    table_name,
                    where
                ), where_args)

        to_follow = defaultdict(lambda : defaultdict(set))
        while True:
            rows = list(self.cursor.fetchmany(self.pks[table_name].batch_size))
            if not rows:
                break

            rows = self._remove_seen_rows(table_name, rows)
            if not rows:
                continue

            self._write_rows(table_name, rows)
            self._calculate_follows(table_name, rows, to_follow)

        self._do_follows(to_follow)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('-c', '--chunks', metavar="chunks", type=int, default=1, 
                        help='the number of chunks to output. Default 1')
    parser.add_argument('-p', '--port', metavar="port", type=int, default=3306,
                        help='the port MySQL is listening on. Default 3306')
    parser.add_argument('-a', '--address', metavar="address", default='localhost',
                        help='the address of the MySQL server')
    parser.add_argument('-u', '--username', metavar="username", required=True,
                        help='the username to connect to MySQL')
    parser.add_argument('-s', '--password', metavar="password", required=True,
                        help='the password to connect to MySQL')
    parser.add_argument('-d', '--database', metavar="database", required=True,
                        help='the name of the database to use')
    parser.add_argument('-o', '--output', metavar="output prefix", 
                        default='dump.sql',
                        help='the prefix for the output. Default dump.sql')
    parser.add_argument('--debug', metavar='level', choices=['info', 'debug'],
                        help='Level of debug to apply: info or debug')
    parser.add_argument('dumpschema',
                        help='the python dumpschema to use')
    args = parser.parse_args()

    if args.debug == 'debug':
        DEBUG_LEVEL = LOG_DEBUG
    elif args.debug == 'info':
        DEBUG_LEVEL = LOG_INFO

    dumpschema = args.dumpschema
    dumpschema = dumpschema[:dumpschema.rfind('.')]

    try:
        m = __import__(dumpschema)
        Dumper(
                m.relationships, 
                m.pks, 
                m.callbacks,
                args.address,
                args.port,
                args.username,
                args.password,
                args.database,
                m.start_table,
                m.start_where,
                m.start_args,
                m.end_sql,
                args.chunks,
                args.output).go()
    except ImportError, e:
        print 'Failed to import %s:'%dumpschema
        print e
