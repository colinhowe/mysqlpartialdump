import MySQLdb
import sys

BULK_INSERT_SIZE = 500
FOLLOW_SIZE = 500

LOG_NONE = 0
LOG_INFO = 1
LOG_DEBUG = 2
DEBUG_LEVEL = LOG_DEBUG

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

def do_follows(pks_seen, result, cursor, relationships, pks, to_follow):
    debug('PKs seen: %s'%pks_seen)
    debug('To follow: %s'%to_follow)
    for table, values in to_follow.iteritems():
        to_remove = []
        for value in values:
            fields = [col for col, _ in value]
            if fields == pks[table][0]:
                pk = tuple([val for _, val in value])
                debug('Checking for PK %s'%pk)
                if pk in pks_seen[table]:
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
            get_table(pks_seen, result, cursor, relationships, pks, table, where, args)
            i += FOLLOW_SIZE

def get_table(pks_seen, result, cursor, relationships, pks, table_name, where=None, where_args=[]):
    info('Exploring %s with where %s and args %s'%(table_name, where, where_args))
    c = cursor

    schema = get_schema(c, table_name)
    field_names = ["`%s`"%row[0] for row in schema]
    unsafe_field_names = [row[0] for row in schema]
    field_offsets = dict([(row[0], i) for i, row in enumerate(schema)])

    c.execute(
            "SELECT %s FROM %s WHERE %s"%( 
                ",".join(field_names),
                table_name,
                where
            ), where_args)
    if c.rowcount == 0:
        return

    to_follow = {}
    options = pks[table_name][1]
    allow_duplicates = ALLOW_DUPLICATES in options
    while True:
        rows = list(c.fetchmany(BULK_INSERT_SIZE))

        # Remove rows we have already processed
        if table_name not in pks:
            raise Exception('PK not created for %s'%table_name)
        pks_seen[table_name] = pks_seen.get(table_name, set())
        rows_to_remove = []
        for row in rows:
            pk = tuple([row[field_offsets[field]] for field in pks[table_name][0]])
            if pk in pks_seen[table_name]:
                debug('PK %s seen in %s'%(pk, table_name))
                rows_to_remove.append(row)
            else:
                debug('Adding PK %s to %s'%(pk, table_name))
                pks_seen[table_name].add(pk)
        for row in rows_to_remove:
            rows.remove(row)

        if not rows:
            break
        result.write('INSERT %s INTO %s(%s) VALUES'%(
            "IGNORE" if allow_duplicates else "",
            table_name, 
            ",".join(field_names)))

        row_strings = []
        for row in rows:
            row_strings.append(
                '(%s)'%",".join(["'%s'"%escape(value) if value is not None else 'NULL' for value in row]))
        result.write(",\n".join(row_strings))
        result.write(';\n')

        for callback in relationships.get(table_name, set()):
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

    do_follows(pks_seen, result, cursor, relationships, pks, to_follow)

def partial_dump(result, relationships, pks, address, port, username, password, database, start_table, start_where, start_args=[]):
    # PKs can be passed in as either a list of columns or a tuple containing a
    # list of columns and a set of options.
    # To make things simpler later we sanitise the PKs now
    new_pks = {}
    for table_name, keys in pks.iteritems():
        if isinstance(keys, list):
            new_pks[table_name] = (keys, set())
        elif len(keys) == 1:
            new_pks[table_name] = (keys[0], set())
        else:
            new_pks[table_name] = keys

    pks = new_pks

    # The relationships are stored as:
    #   { (table_name, col): (table_name, col) }
    # This isn't convenient for quick lookup based on table. So, create a
    # new dictionary that looks like:
    #   { table_name: { (col): (table_name, col) } }
    rels = {}
    for relationship in relationships:
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
    relationships = rels

    db = MySQLdb.connect(
            user=username,
            passwd=password,
            db=database,
            host=address,
            port=port)
    c = db.cursor()
 
    result.write('START TRANSACTION;\n')
    result.write('SET FOREIGN_KEY_CHECKS=0;\n')
    pks_seen = dict([(name, set()) for name in pks.keys()])
    get_table(pks_seen, result, c, relationships, pks, start_table, where=start_where, where_args=start_args)
    result.write('SET FOREIGN_KEY_CHECKS=1;\n')
    result.write('COMMIT;\n')
    
    c.execute('ROLLBACK')
    c.close()
    db.close()

if __name__ == "__main__":
    import sys
    configuration_file = sys.argv[1]
    try:
        m = __import__(configuration_file)
        partial_dump(
                sys.stdout, 
                m.relationships, 
                m.pks, 
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
