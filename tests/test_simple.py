# -*- coding: utf-8 -*-

import os
from foo import *
from nose.tools import eq_

import logging
logging.basicConfig(level='DEBUG')

logger = logging.getLogger(__file__)


import sqlite3
logger.debug('sqlite3 version = %s' % sqlite3.sqlite_version)


class TestBookStore:
    all_titles = ['Sayings of the Century', 'Sword of Honour', 'Moby Dick', 'The Lord of the Rings']
    all_authors = ['Nigel Rees', 'Evelyn Waugh', 'Herman Melville', 'J. R. R. Tolkien']
    all_prices = [8.95, 12.99, 8.99, 22.99, 19.95]

    def setup(self):
        self.dbpath = os.path.join(os.path.dirname(__file__), 'bookstore.db')
        logger.debug('dbpath = %s' % self.dbpath)
        """
        if os.path.exists(self.dbpath):
            os.remove(self.dbpath)

        fpath = os.path.join(os.path.dirname(__file__), 'bookstore.json')
        db = JsonDB.from_file(self.dbpath, fpath)
        db.close()
        """

        self.db = Foo(self.dbpath)

    def teardown(self):
        self.db.close()

    def test_internal_eq(self):
        result = self.db.select("select t.id from jsondata t where t.id in (4,9,14,20) and exists (select * from (select id, type, value from jsondata where parent = (select id from jsondata where type = 8 and parent = t.id and value = 'author') union all select -9, -1, NULL) __t0__ where (__t0__.type > 0 and __t0__.value = 'Evelyn Waugh') and (__t0__.type > 0))")
        eq_([row['id'] for row in result], [9])

    def test_internal_eq_2(self):
        result = self.db.select("select t.id from jsondata t where t.id in (4,9,14,20) and exists (select * from (select id, type, value from jsondata where parent = (select id from jsondata where type = 8 and parent = t.id and value = 'author') union all select -9, -1, NULL) __t0__ where (__t0__.type > 0 and __t0__.value = ?) and (__t0__.type > 0))", ('Evelyn Waugh',))
        eq_([row['id'] for row in result], [9])
 
    def test_travis_eq(self):
        result = self.db.select("select t.id from jsondata t where t.id in (4,9,14,20) and exists (select * from (select id, type, value from jsondata t0 where t0.parent = (select id from jsondata where type = 8 and parent = t.id and value = 'author') union all select -9, -1, NULL) __t0__ where (__t0__.type > 0 and __t0__.value is not NULL and __t0__.value = 'Evelyn Waugh') and (__t0__.type > 0))")
        eq_([row['id'] for row in result], [9])

    def test_internal_eq_try(self):
        result = self.db.select("""
        select t.id from jsondata t
         where t.id in (4,9,14,20)
           and exists (
                select * from (
                    select id, type, value
                    from jsondata
                    where parent = (select id
                                    from jsondata
                                    where type = 8
                                    and parent = t.id
                                    and value = 'author')
                    union all
                    select -9 as id, -1 as type, 'x' as value
                ) t0
                where t0.type > 0
                  and t0.value = 'Evelyn Waugh'
                  and t0.type > 0
               )
        """)
        eq_([row['id'] for row in result], [9])


    def test_travis_eq_pass(self):
        result = self.db.select("""
        select id, type, value from (
            select id, type, value from jsondata t 
                where t.parent in (select id from jsondata where type = 8 and parent in (4,9,14,20) and value = 'author')
            union all select -9 as id, -1 as type, NULL as value
        ) __t0__
        where (__t0__.type > 0 and __t0__.value = 'Evelyn Waugh')
        and (__t0__.type > 0)""")
        eq_([row['id'] for row in result], [37])
        eq_([row['value'] for row in result], ['Evelyn Waugh'])

    def test_travis_eq_pass2(self):
        result = self.db.select("""
        select id, type, value from jsondata t where t.value = 'Evelyn Waugh'
        """)
        eq_([row['value'] for row in result], ['Evelyn Waugh'])

    def test_internal_like(self):
        result = self.db.select("select t.id from jsondata t where t.id in (4,9,14,20) and exists (select * from (select id, type, value from jsondata where parent = (select id from jsondata where type = 8 and parent = t.id and value = 'author') union all select -9, -1, NULL) __t0__ where (__t0__.type > 0 and __t0__.value like 'Evelyn Waugh') and (__t0__.type > 0))")
        eq_([row['id'] for row in result], [9])


if __name__ == '__main__':
    pass
