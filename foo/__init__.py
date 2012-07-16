# -*- coding: utf-8 -*-

import os
import sqlite3

import logging
logger = logging.getLogger(__file__)

(INT, FLOAT, STR, UNICODE, BOOL, NIL, LIST, DICT, KEY) = DATA_TYPES = range(9)
SQL_INSERT_ROOT         = "insert into jsondata values(-1, -2, ?, ?, null)"
SQL_INSERT              = "insert into jsondata values(?, ?, ?, ?, null)"

class Foo:
    def __init__(self, filepath, *args, **kws):
        self.conn = None
        self.cursor = None
        self.dbpath = filepath

        overwrite = kws.get('overwrite', False)
        if overwrite or not os.path.exists(self.dbpath):
            try:
                conn = self.conn or self.get_connection()
                conn.execute('drop table jsondata')
            except sqlite3.OperationalError:
                pass

            self.create_tables()
            self.prepare()

        else:
            self.conn = self.get_connection()

    def create_tables(self):
        conn = self.get_connection()

        # create tables
        conn.execute("""create table if not exists jsondata
        (id     integer primary key,
         parent integer,
         type   integer,
         value  blob,
         link   text
        )""")

        conn.execute("create index if not exists jsondata_idx_composite on jsondata (parent, type)")
 
        conn.commit()
        self.conn = conn

    def get_connection(self, force=True):
        if force or not self.conn:
            try:
                self.conn.close()
            except:
                pass

            self.conn = sqlite3.connect(self.dbpath)
            self.conn.row_factory = sqlite3.Row
            self.conn.text_factory = str
            self.conn.execute('PRAGMA encoding = "UTF-8";')
            self.conn.execute('PRAGMA foreign_keys = ON;')
            self.conn.execute('PRAGMA synchronous = OFF;')
            self.conn.execute('PRAGMA page_size = 8192;')
            self.conn.execute('PRAGMA automatic_index = 1;')
            self.conn.execute('PRAGMA temp_store = MEMORY;')
            self.conn.execute('PRAGMA journal_mode = MEMORY;')

            def ancestors_in(id, candicates):
                # FIXME: Find a better way to do this.
                candicates = eval(candicates)
                while id > -2:
                    if id in candicates:
                        return True
                    row = self.conn.execute('select parent from jsondata where id = ?', (id,)).fetchone()
                    id = row['parent']
                return False

            self.conn.create_function('ancestors_in', 2, ancestors_in)

        return self.conn

    def get_cursor(self):
        if not self.cursor or not self.conn:
            conn = self.conn or self.get_connection()
            try:
                self.cursor.close()
            except:
                pass
            self.cursor= conn.cursor()
        return self.cursor

    def commit(self):
        self.conn.commit()

    def rollback(self):
        self.conn.rollback()
 
    def close(self):
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.commit()
            self.conn.close()

    def select(self, stmt, variables=()):
        c = self.cursor or self.get_cursor()
        logger.debug(stmt)
        logger.debug(repr(variables))
        c.execute(stmt, variables)
        result = c.fetchall()
        logger.debug(' >> ' + repr([row['id'] for row in result]))

        c.execute(stmt, variables)
        result = c.fetchall()
        return result

    def insert(self, *args, **kws):
        c = self.cursor or self.get_cursor()
        c.execute(SQL_INSERT, *args, **kws)
        return c.lastrowid

    def insert_root(self, (root_type, value)):
        c = self.cursor or self.get_cursor()
        conn = self.conn or self.get_connection()
        c.execute(SQL_INSERT_ROOT, (root_type, value))
        conn.commit()

    def prepare(self):
        self.insert_root((DICT, ''))
        rows = [
        (0,           -1, KEY          ,'store'),
        (1,            0, DICT, ''),
        (2,            1, KEY,          'book'),        
        (3,            2 ,LIST ,''),                    
        (4,            3 ,DICT,''),                     
        (5,            4 ,KEY          ,'category'),
        (6,            4 ,KEY          ,'price'),
        (7,            4 ,KEY          ,'title'),
        (8,            4 ,KEY          ,'author'),
        (9,            3 ,DICT, ''),                     
        (10,            9, KEY          ,'category'),
        (11,            9, KEY          ,'price'),
        (12,            9, KEY          ,'title'),
        (13,            9, KEY          ,'author'),
        (14,            3, DICT, ''),
        (15,           14, KEY          ,'category'),
        (16,           14, KEY          ,'price'),
        (17,           14, KEY          ,'title'),
        (18,           14, KEY          ,'isbn'),
        (19,           14, KEY          ,'author'),
        (20,            3, DICT, ''),
        (21,           20, KEY          ,'category'),
        (22,           20, KEY          ,'price'),
        (23,           20, KEY          ,'title'),
        (24,           20, KEY          ,'isbn'),
        (25,           20, KEY          ,'author'),
        (26,            1, KEY          ,'bicycle'),
        (27,           26, DICT, ''),                     
        (28,           27, KEY          ,'color'),       
        (29,           27, KEY          ,'price'),       
        (30,            5, STR          ,'reference'),   
        (31,            6, FLOAT                ,8.95),
        (32,            7, STR          ,'Sayings of the Century'),
        (33,            8, STR          ,'Nigel Rees'),
        (34,           10, STR          ,'fiction'),
        (35,           11, FLOAT               ,12.99),
        (36,           12, STR          ,'Sword of Honour'),
        (37,           13, STR          ,'Evelyn Waugh'),
        (38,           15, STR          ,'fiction'),     
        (39,           16, FLOAT                ,8.99),
        (40,           17, STR          ,'Moby Dick'),
        (41,           18, STR          ,'0-553-21311-3'),
        (42,           19, STR          ,'Herman Melville'),
        (43,           21, STR          ,'fiction'),     
        (44,           22, FLOAT               ,22.99),
        (45,           23, STR          ,'The Lord of the Rings'),
        (46,           24, STR          ,'0-395-19395-8'),
        (47,           25, STR          ,'J R R Tolkien'),
        (48,           28, STR          ,'red'),         
        (49,           29, FLOAT               ,19.95),
        ]

        for r in rows:
            self.insert(r)
        self.commit()

    def dumprows(self):
        c = self.cursor or self.get_cursor()
        c.execute('select * from jsondata order by id')
        fmt = '%s, [%s], [%s], [%s]'
        yield fmt % ('id', 'parent', 'type', 'value')
        for row in c.fetchall():
            yield fmt % (row['id'], row['parent'], row['type'], 'LINK: %s' % row['link'] if row['link'] else row['value'])

