import MySQLdb
from MySQLdb import cursors
import sys
import getopt
from sys import stderr
from datetime import datetime
import codecs

BULK_INSERT_SIZE = 5000
FOLLOW_SIZE = 5000

LOG_NONE = 0
LOG_INFO = 1
LOG_DEBUG = 2
DEBUG_LEVEL = LOG_NONE

BIDIRECTIONAL = 'bidirectional'
ALLOW_DUPLICATES = 'allow duplicates'
NO_KEY_CACHE = 'no key cache'

def get_schema(cursor, name):
    cursor.execute("DESCRIBE %s"%name)
    return cursor.fetchall()

def debug(msg):
    if DEBUG_LEVEL >= LOG_DEBUG:
        stderr.write('DEBUG: %s %s\n'%(datetime.now(), msg))

def info(msg):
    if DEBUG_LEVEL >= LOG_INFO:
        stderr.write('INFO: %s %s\n'%(datetime.now(), msg[:100]))

def escape(value):
    if not isinstance(value, basestring):
        return str(value)
    return value.replace("'", "''").replace("\\", "\\\\")

class Pk(object):
    def __init__(self, columns, *options):
        self.columns = columns
        self.options = set(options)

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
        self.batch_sizes = {}
        self.chunks = chunks
        self.output_prefix = output_prefix

        self.cached_schemas = {}

    def get_writer(self):
        # Get the smallest writer
        writers_with_size = []
        for writer in self.writers:
            writers_with_size.append((writer, writer.tell()))
        return sorted(writers_with_size, key=lambda t: t[1])[0][0]

    def go(self):
        self.writers = []
        for chunk in range(self.chunks):
            writer = open("%s.%d"%(self.output_prefix, chunk), 'w')
            writer = codecs.getwriter('utf8')(writer)
            self.writers.append(writer)
            writer.write('SET FOREIGN_KEY_CHECKS=0;\n')

        result = self.get_writer()
        self.pks_seen = dict([(name, set()) for name in self.pks.keys()])

        # The relationships are stored as:
        #   { (table_name, col): (table_name, col) }
        # This isn't convenient for quick lookup based on table. So, create a
        # new dictionary that looks like:
        #   { table_name: { (col): (table_name, col) } }
        rels = {}
        for relationship in self.relationships:
            src = relationship[0]
            target = relationship[1]
            if isinstance(src, basestring):
                rels[src] = rels.get(src, set())
                rels[src].add(target)
            else:
                def create_callback(target_table, target_col, src_col):
                    target_col = '%s'%target_col
                    src_col = '%s'%src_col
                    def callback(row):
                        return (target_table, (target_col, row[src_col]))
                    return callback

                src_name = src[0]
                target_name = target[0]
                
                rels[src_name] = rels.get(src_name, set())
                callback = create_callback(target_name, target[1], src[1])
                rels[src_name].add(callback)

                if len(relationship) == 4:
                    callback.batch_size = relationship[3]

                # The back link must also be setup for bidirectional links
                if len(relationship) != 2 and relationship[2] == BIDIRECTIONAL:
                    rels[target_name] = rels.get(target_name, set())
                    rels[target_name].add(create_callback(src_name, src[1], target[1]))
        self.relationships = rels

        db = MySQLdb.connect(
                user=self.db_username,
                passwd=self.db_password,
                db=self.db_name,
                host=self.db_address,
                port=self.db_port,
                charset='utf8',
                cursorclass=cursors.SSCursor)
        self.cursor = db.cursor()
        self.cursor.execute('SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ')
        self.cursor.execute('START TRANSACTION')
     
        self.get_table(self.start_table, where=self.start_where, where_args=self.start_args)
        
        self.cursor.execute('ROLLBACK')
        self.cursor.close()
        db.close()

        result.write(self.end_sql)

        for writer in self.writers:
            writer.write('SET FOREIGN_KEY_CHECKS=1;\n')
            writer.close()

    def _get_schema(self, table_name):
        '''Gets the schema of the given table. Will call to the database to
        get the schema if it hasn't been explored before'''
        if table_name not in self.cached_schemas:
            schema = get_schema(self.cursor, table_name)
            safe_field_names = ["`%s`"%row[0] for row in schema]
            unsafe_field_names = [row[0] for row in schema]
            field_offsets = dict([(row[0], i) for i, row in enumerate(schema)])
            self.cached_schemas[table_name] = (
                    safe_field_names,
                    unsafe_field_names,
                    field_offsets)

        return self.cached_schemas[table_name]
       
    def do_follows(self, to_follow):
        debug('PKs seen: %s'%self.pks_seen)
        debug('To follow: %s'%to_follow)
        for table, follow_sets in to_follow.iteritems():
            follow_sets_keys = list(follow_sets.keys())
            for field_names in follow_sets_keys:
                value_sets = follow_sets[field_names]
                if field_names == tuple(self.pks[table].columns):
                    values = []
                    for value_tuple in value_sets:
                        if value_tuple not in self.pks_seen[table]:
                            values.append(value_tuple)
                else:
                    info('Not killing follows for %s %s'%(field_names, table))
                    values = list(value_sets)

                batch_size = self.batch_sizes[(table, field_names)]

                while len(values) > 0:
                    values_to_follow = values[:batch_size]
                    del(values[:batch_size])
                    clauses = []
                    args = []
                    clause = " AND ".join(["%s = %%s"%col for col in field_names])
                    clauses = [clause] * len(values_to_follow)
                    for value in values_to_follow:
                        args += [val for val in value]
                    debug('Clauses to follow: %s'%clauses)
                    info('Following %s with %s'%(table, values_to_follow))
                    where = " OR ".join(clauses)
                    self.get_table(table, where, args)
                del(follow_sets[field_names])

    def get_pk(self, table_name, row):
        (_, _, field_offsets) = self._get_schema(table_name)
        return tuple([row[field_offsets[field]] for field in self.pks[table_name].columns])

    def is_row_seen(self, table_name, row):
        pk = self.get_pk(table_name, row)
        if pk in self.pks_seen[table_name]:
            debug('PK %s seen in %s'%(pk, table_name))
            return True
        else:
            debug('PK %s not seen in %s'%(pk, table_name))
            return False

    def add_row(self, table_name, row):
        pk = self.get_pk(table_name, row)
        if NO_KEY_CACHE in self.pks[table_name].options:
            return True
        if pk in self.pks_seen[table_name]:
            return False
        self.pks_seen[table_name].add(pk)
        return True

    def get_table(self, table_name, where=None, where_args=[]):
        info('Exploring %s with where %s and args %s'%(table_name, where, where_args))
        result = self.get_writer()
        
        (safe_field_names, unsafe_field_names, field_offsets) = self._get_schema(table_name)

        self.cursor.execute(
                "SELECT %s FROM %s WHERE %s"%( 
                    ",".join(safe_field_names),
                    table_name,
                    where
                ), where_args)

        to_follow = {}
        options = self.pks[table_name].options
        allow_duplicates = ALLOW_DUPLICATES in options
        while True:
            rows = list(self.cursor.fetchmany(BULK_INSERT_SIZE))
            if not rows:
                break

            # Only process rows we have not already processed
            if table_name not in self.pks:
                raise Exception('PK not created for %s'%table_name)
            rows = [row for row in rows if self.add_row(table_name, row)]

            if not rows:
                continue
            
            result.write('INSERT %s INTO %s(%s) VALUES'%(
                "IGNORE" if allow_duplicates else "",
                table_name, 
                ",".join(safe_field_names)))

            row_strings = []
            for row in rows:
                callback = self.callbacks.get(table_name, None)
                if callback:
                    row_dict = dict([(field, row[offset]) for field, offset in field_offsets.iteritems()])
                    row_dict = callback(row_dict)
                    row = [row_dict[unsafe_field_names[i]] for i in range(len(row))]
                row_strings.append(
                    '(%s)'%",".join(["'%s'"%escape(value) if value is not None else 'NULL' for value in row]))
            result.write(",\n".join(row_strings))
            result.write(';\n')

            for callback in self.relationships.get(table_name, set()):
                for row in rows:
                    row_dict = {}
                    for i, field in enumerate(unsafe_field_names):
                        row_dict[field] = row[i]
                    r = callback(row_dict)
                    if r is None:
                        continue
                    target_name = r[0]
                    keys = r[1:]
                    to_follow[target_name] = to_follow.get(target_name, dict())
                    field_names = tuple([field_name for (field_name, _) in keys])
                    follow_set = to_follow[target_name].get(field_names, set())
                    values = tuple([value for (_, value) in keys])
                    follow_set.add(values)
                    if hasattr(callback, 'batch_size'):
                        self.batch_sizes[(target_name, field_names)] = callback.batch_size
                    else:
                        self.batch_sizes[(target_name, field_names)] = FOLLOW_SIZE
                    to_follow[target_name][field_names] = follow_set

        self.do_follows(to_follow)

if __name__ == "__main__":

    optlist, args = getopt.getopt(sys.argv[1:], 'dic:o:')

    for o, a in optlist:
        if o == '-d':
            DEBUG_LEVEL = LOG_DEBUG
        if o == '-i':
            DEBUG_LEVEL = LOG_INFO
        if o == '-c':
            chunks = int(a)
        if o == '-o':
            output_prefix = a

    configuration_file = args[0]
    try:
        m = __import__(configuration_file)
        Dumper(
                m.relationships, 
                m.pks, 
                m.callbacks,
                m.DB_ADDRESS,
                m.DB_PORT,
                m.DB_USERNAME,
                m.DB_PASSWORD,
                m.DB_NAME,
                m.start_table,
                m.start_where,
                m.start_args,
                m.end_sql,
                chunks,
                output_prefix).go()
    except ImportError, e:
        print 'Failed to import %s:'%configuration_file
        print e
