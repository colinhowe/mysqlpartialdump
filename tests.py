import MySQLdb
import unittest
import mysqlpartialdump as dumper

def init_connection():
    try:
        import test_config
    except ImportError:
        print '''Failed to import test_config. Please ensure test_config.py exists and is set up similar to the following:
DB_ADDRESS = '127.0.0.1'
DB_PORT = 3306
DB_USERNAME = someuser
DB_PASSWORD = somepassword
DB_NAME = somedatabase
'''
        raise 
    db = MySQLdb.connect(
            user=test_config.DB_USERNAME,
            passwd=test_config.DB_PASSWORD,
            db=test_config.DB_NAME,
            host=test_config.DB_ADDRESS,
            port=test_config.DB_PORT)
    return db

class TestImport(unittest.TestCase):

    def drop(self, cursor, name):
        try:
           cursor.execute('DROP TABLE %s'%name)
        except:
            # Ok, table already deleted
            pass

    def setUp(self):
        self.db = None
        self.db = init_connection()

        c = self.db.cursor()

        # Drop any existing tables
        self.drop(c, 'pet')
        self.drop(c, 'owner')
        self.drop(c, 'log')

        # Create a simple database
        c.execute('''
            CREATE TABLE pet (
            `id` INT NOT NULL AUTO_INCREMENT,
            `name` VARCHAR(30) NOT NULL,
            `parent_id` INT NULL,
            `owner_id` INT NOT NULL,
            PRIMARY KEY (`id`)
            );''')
        c.execute('''
            CREATE TABLE owner (
            `id` INT NOT NULL AUTO_INCREMENT,
            `name` VARCHAR(30) NOT NULL,
            PRIMARY KEY (`id`)
            );''')
        c.execute('''
            CREATE TABLE log (
            `id` INT NOT NULL AUTO_INCREMENT,
            `entity` VARCHAR(30) NOT NULL,
            `message` VARCHAR(50) NOT NULL,
            PRIMARY KEY (`id`)
            );''')


        c.close()

    def tearDown(self):
        if self.db is not None:
            self.db.close()

    def do_partial_dump(self, relationships, start_table, start_ids):
        '''Helper method to make running a dump a bit tidier in tests'''
        pks = {
                'owner':['id'],
                'pet':['id'],
                'log':['id'],
        }
        import test_config
        return dumper.partial_dump(
                relationships, 
                pks,
                test_config.DB_ADDRESS,
                test_config.DB_PORT,
                test_config.DB_USERNAME,
                test_config.DB_PASSWORD,
                test_config.DB_NAME,
                start_table,
                start_ids)

    def create_pet(self, id, name, parent_id, owner_id):
        sql = 'INSERT INTO pet(id, name, parent_id, owner_id) VALUES(%s, %s, %s, %s)'
        c = self.db.cursor()
        c.execute(sql, (id, name, parent_id, owner_id))
        self.db.commit()
        c.close()

    def create_owner(self, id, name):
        sql = 'INSERT INTO owner(id, name) VALUES(%s, %s)'
        c = self.db.cursor()
        c.execute(sql, (id, name))
        self.db.commit()
        c.close()
        
    def create_log(self, id, entity, message):
        sql = 'INSERT INTO log(id, entity, message) VALUES(%s, %s, %s)'
        c = self.db.cursor()
        c.execute(sql, (id, entity, message))
        self.db.commit()
        c.close()

    def import_dump(self, dump):
        c = self.db.cursor()
        c.execute('DELETE FROM pet')
        c.execute('DELETE FROM owner')
        self.db.commit()
        c.execute(dump)
        c.close()

    def get_owners(self):
        c = self.db.cursor()
        c.execute('SELECT id, name FROM owner')
        result = {}
        while True:
            row = c.fetchone()
            if row is None:
                break
            result[row[0]] = { 'id': row[0], 'name': row[1] }
        c.close()
        return result

    def get_logs(self):
        c = self.db.cursor()
        c.execute('SELECT id, entity, message FROM log')
        result = {}
        while True:
            row = c.fetchone()
            if row is None:
                break
            result[row[0]] = { 'id': row[0], 'entity': row[1], 'message': row[2] }
        c.close()
        return result

    def get_pets(self):
        c = self.db.cursor()
        c.execute('SELECT id, name, parent_id, owner_id FROM pet')
        result = {}
        while True:
            row = c.fetchone()
            if row is None:
                break
            result[row[0]] = { 
                    'id': row[0], 
                    'name': row[1],
                    'parent_id': row[2],
                    'owner_id': row[3]
            }
        c.close()
        return result
 
    def test_single_row(self):
        self.create_owner(1, 'Bob')
        result = self.do_partial_dump({}, 'owner', 'id=1')

        # Reimporting the result should give a single row that is the same as
        # the original input
        self.import_dump(result)
        
        owners = self.get_owners()
        self.assertEquals(1, len(owners))
        self.assertEquals('Bob', owners[1]['name'])

    def test_many_rows(self):
        # Importing many rows should work just the same
        # We need to test it though to ensure bulk inserts work
        for a in xrange(1, 101):
            self.create_owner(a, 'Bob')
        result = self.do_partial_dump({}, 'owner', '1=1')

        # Reimporting the result should give a single row that is the same as
        # the original input
        self.import_dump(result)
        
        owners = self.get_owners()
        self.assertEquals(100, len(owners))

    def test_forward_reference(self):
        # A reference from X to Y should cause Y be pulled in if X is pulled in
        self.create_owner(1, 'Bob')
        self.create_pet(1, 'Ginger', parent_id=None, owner_id=1)
        relations = {
            ('owner', 'id'): ('pet', 'owner_id')
        }
        result = self.do_partial_dump(relations, 'owner', '1=1')

        # Reimporting the result should give a single row that is the same as
        # the original input
        self.import_dump(result)
        
        owners = self.get_owners()
        self.assertEquals(1, len(owners))

        pets = self.get_pets()
        self.assertEquals(1, len(pets))
        self.assertEquals('Ginger', pets[1]['name'])
        self.assertEquals(None, pets[1]['parent_id'])
        self.assertEquals(1, pets[1]['owner_id'])
    
    def test_back_reference(self):
        # A reference from X to Y should cause X be pulled in if Y is pulled in
        self.create_owner(1, 'Bob')
        self.create_pet(1, 'Ginger', parent_id=None, owner_id=1)
        relations = {
            ('pet', 'owner_id'): ('owner', 'id')
        }
        result = self.do_partial_dump(relations, 'owner', '1=1')

        # Reimporting the result should give a single row that is the same as
        # the original input
        self.import_dump(result)
        
        owners = self.get_owners()
        self.assertEquals(1, len(owners))

        pets = self.get_pets()
        self.assertEquals(1, len(pets))
        self.assertEquals('Ginger', pets[1]['name'])
        self.assertEquals(None, pets[1]['parent_id'])
        self.assertEquals(1, pets[1]['owner_id'])
 
    def test_custom_relationship(self):
        # Relationships can be complicated. Callbacks can be used!
        self.create_owner(1, 'Bob')
        self.create_pet(1, 'Ginger', parent_id=None, owner_id=1)
        self.create_log(1, 'Pet1', 'Hello')
        def get_logs_relationship(row):
            return {'entity': 'Pet%s'%row['id']}
        relations = {
            ('pet'): get_logs_relationship
        }
        result = self.do_partial_dump(relations, 'owner', '1=1')

        # Reimporting the result should give a single row that is the same as
        # the original input
        self.import_dump(result)
        
        logs = self.get_logs()
        self.assertEquals(1, len(logs))
        self.assertEquals('Pet1', logs[1]['entity'])
        self.assertEquals('Hello', logs[1]['message'])
