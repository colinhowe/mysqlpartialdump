import MySQLdb
from cStringIO import StringIO

BULK_INSERT_SIZE = 50
FOLLOW_SIZE = 50

LOG_NONE = 0
LOG_INFO = 1
LOG_DEBUG = 2
DEBUG_LEVEL = LOG_DEBUG

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
    return str(value).replace("'", "''")

def get_table(pks_seen, result, cursor, relationships, pks, table_name, where=None):
    info('Exploring %s with where %s'%(table_name, where))
    c = cursor

    schema = get_schema(c, table_name)
    field_names = ["`%s`"%row[0] for row in schema]
    field_offsets = dict([(row[0], i) for i, row in enumerate(schema)])

    c.execute(
            "SELECT %s FROM %s WHERE %s"%( 
                ",".join(field_names),
                table_name,
                where
            ))
    if c.rowcount == 0:
        return

    to_follow = {}
    while True:
        rows = list(c.fetchmany(BULK_INSERT_SIZE))

        # Remove rows we have already processed
        if table_name not in pks:
            raise Exception('PK not created for %s'%table_name)
        pks_seen[table_name] = pks_seen.get(table_name, set())
        rows_to_remove = []
        for row in rows:
            pk = []
            for field in pks[table_name]:
                pk.append(row[field_offsets[field]])
            pk = tuple(pk)
            if pk in pks_seen[table_name]:
                rows_to_remove.append(row)
            else:
                pks_seen[table_name].add(pk)
        for row in rows_to_remove:
            rows.remove(row)

        if not rows:
            break
        result.write('INSERT INTO %s(%s) VALUES'%(table_name, ",".join(field_names)))

        row_strings = []
        for row in rows:
            row_strings.append(
                '(%s)'%",".join(["'%s'"%escape(value) if value else 'NULL' for value in row]))
        result.write(",".join(row_strings))
        result.write(';\n')

        for (src_col, target) in relationships.get(table_name, {}).iteritems():
            target_name = target[0]
            target_col = "`%s`"%target[1]
            src_col = "`%s`"%src_col
            for row in rows:
                row_dict = {}
                for i, field in enumerate(field_names):
                    row_dict[field] = row[i]

                to_follow[target_name] = to_follow.get(target_name, {})
                to_follow[target_name][target_col] = to_follow[target_name].get(target_col, {})
                to_follow[target_name][target_col][row_dict[src_col]] = True

    # Go over each table and relationship in the follow list and follow it
    for table, table_relationships in to_follow.iteritems():
        for relationship, values in table_relationships.iteritems():
            i = 0
            values = values.keys()
            while i < len(values):
                values_to_follow = values[i:i+FOLLOW_SIZE]
                values_to_follow = ["'%s'"%value for value in values_to_follow]
                info('Following %s with %s'%(table, values_to_follow))
                where = "%s IN (%s)"%(relationship, ",".join(values_to_follow))
                get_table(pks_seen, result, cursor, relationships, pks, table, where)
                i += FOLLOW_SIZE

    return

def partial_dump(relationships, pks, address, port, username, password, database, start_table, start_where):
    # The relationships are stored as:
    #   { (table_name, col): (table_name, col) }
    # This isn't convenient for quick lookup based on table. So, create a
    # new dictionary that looks like:
    #   { table_name: { (col): (table_name, col) } }
    rels = {}
    for (src, target) in relationships.iteritems():
        src_name = src[0]
        rels[src_name] = rels.get(src_name, {})
        rels[src_name][src[1]] = target

        # The back link must also be setup as links are bidirectional
        target_name = target[0]
        rels[target_name] = rels.get(target_name, {})
        rels[target_name][target[1]] = src 

    relationships = rels

    db = MySQLdb.connect(
            user=username,
            passwd=password,
            db=database,
            host=address,
            port=port)
    c = db.cursor()
    
    
    result = StringIO()
    result.write('START TRANSACTION;\n')
    get_table({}, result, c, relationships, pks, start_table, where=start_where)
    result.write('COMMIT;\n')
    
    c.execute('ROLLBACK')
    c.close()
    db.close()
    return result.getvalue()

