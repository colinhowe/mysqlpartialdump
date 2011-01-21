import MySQLdb
import sys

BULK_INSERT_SIZE = 500
FOLLOW_SIZE = 500

LOG_NONE = 0
LOG_INFO = 1
LOG_DEBUG = 2
DEBUG_LEVEL = LOG_NONE

UNIDIRECTIONAL = 'unidirectional'
ALLOW_DUPLICATES = 'allow duplicates'

def get_schema(cursor, name):
    cursor.execute("DESCRIBE %s"%name)
    return cursor.fetchall()

def debug(msg):
    if DEBUG_LEVEL >= LOG_DEBUG:
        print msg

def info(msg):
    if DEBUG_LEVEL >= LOG_INFO:
        print msg

def escape(value):
    return str(value).replace("'", "''").replace("\\", "\\\\")

class Dumper(object):
    def __init__(
            self,
            result,
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
            start_args=[]
            ):
        self.result = result
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

        self.cached_schemas = {}

    def go(self):
        # PKs can be passed in as either a list of columns or a tuple containing a
        # list of columns and a set of options.
        # To make things simpler later we sanitise the PKs now
        new_pks = {}
        for table_name, keys in self.pks.iteritems():
            if isinstance(keys, list):
                new_pks[table_name] = (keys, set())
            elif len(keys) == 1:
                new_pks[table_name] = (keys[0], set())
            else:
                new_pks[table_name] = keys

        self.pks = new_pks
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
                rels[src_name].add(create_callback(target_name, target[1], src[1]))

                # The back link must also be setup for bidirectional links
                if len(relationship) == 2 or relationship[2] != UNIDIRECTIONAL:
                    rels[target_name] = rels.get(target_name, set())
                    rels[target_name].add(create_callback(src_name, src[1], target[1]))
        self.relationships = rels

        db = MySQLdb.connect(
                user=self.db_username,
                passwd=self.db_password,
                db=self.db_name,
                host=self.db_address,
                port=self.db_port)
        self.cursor = db.cursor()
        self.cursor.execute('START TRANSACTION')
     
        self.result.write('START TRANSACTION;\n')
        self.result.write('SET FOREIGN_KEY_CHECKS=0;\n')
        self.get_table(self.start_table, where=self.start_where, where_args=self.start_args)
        self.result.write('SET FOREIGN_KEY_CHECKS=1;\n')
        self.result.write('COMMIT;\n')
        
        self.cursor.execute('ROLLBACK')
        self.cursor.close()
        db.close()

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
        for table, values in to_follow.iteritems():
            to_remove = []
            for value in values:
                fields = [col for col, _ in value]
                if fields == self.pks[table][0]:
                    pk = tuple([val for _, val in value])
                    debug('Checking for PK %s'%pk)
                    if pk in self.pks_seen[table]:
                        debug('PK %s seen in %s'%(pk, table))
                        to_remove.append(value)
            for value in to_remove:
                values.remove(value)

        for table, values in to_follow.iteritems():
            values = list(values)
            i = 0
            while i < len(values):
                values_to_follow = values[i:i+FOLLOW_SIZE]
                clauses = []
                args = []
                for value in values_to_follow:
                    clauses.append("(%s)"%(" AND ".join(["%s = %%s"%col for col,_ in value])))
                    args += [val for _, val in value]
                debug('Clauses to follow: %s'%clauses)
                info('Following %s with %s'%(table, values_to_follow))
                where = " OR ".join(clauses)
                self.get_table(table, where, args)
                i += FOLLOW_SIZE

    def get_pk(self, table_name, row):
        (safe_field_names, unsafe_field_names, field_offsets) = self._get_schema(table_name)
        return tuple([row[field_offsets[field]] for field in self.pks[table_name][0]])
 

    def is_row_seen(self, table_name, row):
        pk = self.get_pk(table_name, row)
        if pk in self.pks_seen[table_name]:
            debug('PK %s seen in %s'%(pk, table_name))
            return True
        else:
            debug('PK %s not seen in %s'%(pk, table_name))
            return False

    def add_row(self, table_name, row):
        if self.is_row_seen(table_name, row):
            return False
        pk = self.get_pk(table_name, row)
        self.pks_seen[table_name].add(pk)
        return True

    def get_table(self, table_name, where=None, where_args=[]):
        info('Exploring %s with where %s and args %s'%(table_name, where, where_args))
        
        (safe_field_names, unsafe_field_names, field_offsets) = self._get_schema(table_name)

        self.cursor.execute(
                "SELECT %s FROM %s WHERE %s"%( 
                    ",".join(safe_field_names),
                    table_name,
                    where
                ), where_args)
        if self.cursor.rowcount == 0:
            return

        to_follow = {}
        options = self.pks[table_name][1]
        allow_duplicates = ALLOW_DUPLICATES in options
        while True:
            rows = list(self.cursor.fetchmany(BULK_INSERT_SIZE))

            # Remove rows we have already processed
            if table_name not in self.pks:
                raise Exception('PK not created for %s'%table_name)
            rows_to_remove = [row for row in rows if not self.add_row(table_name, row)]
            for row in rows_to_remove:
                rows.remove(row)

            if not rows:
                break
            
            self.result.write('INSERT %s INTO %s(%s) VALUES'%(
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
            self.result.write(",\n".join(row_strings))
            self.result.write(';\n')

            for callback in self.relationships.get(table_name, set()):
                for row in rows:
                    row_dict = {}
                    for i, field in enumerate(unsafe_field_names):
                        row_dict[field] = row[i]
                    r = callback(row_dict)
                    if r is not None:
                        target_name = r[0]
                        keys = r[1:]
                        to_follow[target_name] = to_follow.get(target_name, set())
                        to_follow[target_name].add(frozenset(keys))

        self.do_follows(to_follow)

if __name__ == "__main__":
    import sys
    configuration_file = sys.argv[1]
    try:
        m = __import__(configuration_file)
        Dumper(
                sys.stdout, 
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
                m.start_args)
    except ImportError, e:
        print 'Failed to import %s:'%configuration_file
        print e
