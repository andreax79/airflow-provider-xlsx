#!/usr/bin/env python

from unittest import TestCase, main
from datetime import datetime
from xlsx_provider import get_provider_info
from xlsx_provider.commons import (
    check_column_names,
    get_type,
    rmdiacritics,
    TYPE_INT,
    TYPE_NULLABLE_INT,
    TYPE_DATETIME,
    TYPE_DOUBLE,
    TYPE_STRING,
)


def expect_exception(exception):
    def test_decorator(fn):
        def test_decorated(self, *args, **kwargs):
            self.assertRaises(exception, fn, self, *args, **kwargs)

        return test_decorated

    return test_decorator


class TestMisc(TestCase):
    def test_get_provider_info(self):
        t = get_provider_info()
        for key in [
            "package-name",
            "name",
            "description",
            "hook-class-names",
            "extra-links",
            "versions",
        ]:
            self.assertTrue(key in t)

    def test_rmdiacritics(self):
        self.assertEqual(rmdiacritics('a'), 'a')
        self.assertEqual(rmdiacritics('à'), 'a')
        self.assertEqual(rmdiacritics('ü'), 'u')

    def test_get_type(self):
        self.assertEqual(get_type('t1', 1), TYPE_INT)
        self.assertEqual(get_type('t2', 1.0), TYPE_DOUBLE)
        self.assertEqual(get_type('t3', datetime.now()), TYPE_DATETIME)
        self.assertEqual(get_type('t4', 'ciao'), TYPE_STRING)
        self.assertEqual(get_type('t5', 'ciao'), TYPE_STRING)

    def test_get_type_nullable(self):
        self.assertEqual(get_type('t1', 1, True), TYPE_NULLABLE_INT)
        self.assertEqual(get_type('t2', 1.0, True), TYPE_DOUBLE)
        self.assertEqual(get_type('t3', datetime.now()), TYPE_DATETIME, True)
        self.assertEqual(get_type('t4', 'ciao', True), TYPE_STRING)
        self.assertEqual(get_type('t5', 'ciao', True), TYPE_STRING)

    @expect_exception(Exception)
    def test_get_type_exception(self):
        get_type('t1', None)

    def test_check_column_names(self):
        check_column_names([])
        check_column_names(['a', 'b', 'c'])
        self.assertTrue(True)

    @expect_exception(Exception)
    def test_check_column_names_exception(self):
        check_column_names(['a', 'b', 'a', 'c'])


if __name__ == '__main__':
    main()
