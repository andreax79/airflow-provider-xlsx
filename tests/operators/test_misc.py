#!/usr/bin/env python

from unittest import TestCase, main
from datetime import datetime
from xlsx_provider import get_provider_info
from xlsx_provider.commons import check_column_names, get_type, rmdiacritics


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

    def rmdiacritics(self):
        self.assertEquals(rmdiacritics('città'), 'citta')
        self.assertEquals(rmdiacritics('uüu'), 'uuu')

    def get_type(self):
        self.assertEquals(get_type(1), 'd')
        self.assertEquals(get_type(1.0), 'd')
        self.assertEquals(get_type(datetime.now()), 'datetime64[ns]')
        self.assertEquals(get_type('ciao'), 'str')
        self.assertEquals(get_type('ciao'), 'str')

    @expect_exception(Exception)
    def get_type_exception(self):
        get_type(None)

    def check_column_names(self):
        check_column_names([])
        check_column_names(['a', 'b', 'c'])
        self.assertTrue(True)

    @expect_exception(Exception)
    def check_column_names_exception(self):
        check_column_names(['a', 'b', 'a', 'c'])


if __name__ == '__main__':
    main()
