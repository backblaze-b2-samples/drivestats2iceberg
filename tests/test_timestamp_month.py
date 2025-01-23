import unittest

from timestamp_month import TimestampMonth

class TestTimestampMonth(unittest.TestCase):

    def test_creation(self):
        tm_zero = TimestampMonth(0)

        tm = TimestampMonth.from_year_month(1970, 0)
        self.assertEqual(tm, tm_zero)
        self.assertEqual(tm.month(), 0)
        self.assertEqual(tm.quarter(), 0)
        self.assertEqual(tm.year(), 1970)

        tm = TimestampMonth.from_year_quarter(1970, 0)
        self.assertEqual(tm, tm_zero)
        self.assertEqual(tm.month(), 0)
        self.assertEqual(tm.quarter(), 0)
        self.assertEqual(tm.year(), 1970)

        tm = TimestampMonth.from_year_month(2024, 11)
        self.assertNotEqual(tm, tm_zero)
        self.assertEqual(tm.month(), 11)
        self.assertEqual(tm.quarter(), 3)
        self.assertEqual(tm.year(), 2024)

        tm = TimestampMonth.from_year_quarter(2024, 3)
        self.assertNotEqual(tm, tm_zero)
        self.assertEqual(tm.month(), 9)
        self.assertEqual(tm.quarter(), 3)
        self.assertEqual(tm.year(), 2024)

        with self.assertRaises(TypeError):
            tm_bad = TimestampMonth('0')  # noqa

        with self.assertRaises(TypeError):
            tm_bad = TimestampMonth.from_year_month(1970, '0')  # noqa

        with self.assertRaises(TypeError):
            tm_bad = TimestampMonth.from_year_month('1970', 0)  # noqa

        with self.assertRaises(TypeError):
            tm_bad = TimestampMonth.from_year_month('1970', '0')  # noqa

        with self.assertRaises(TypeError):
            tm_bad = TimestampMonth.from_year_quarter(1970, '0')  # noqa

        with self.assertRaises(TypeError):
            tm_bad = TimestampMonth.from_year_quarter('1970', 0)  # noqa

        with self.assertRaises(TypeError):
            tm_bad = TimestampMonth.from_year_quarter('1970', '0')  # noqa

        with self.assertRaises(ValueError):
            tm_bad = TimestampMonth.from_year_month(1970, -1)  # noqa

        with self.assertRaises(ValueError):
            tm_bad = TimestampMonth.from_year_month(1970, 12)  # noqa

        with self.assertRaises(ValueError):
            tm_bad = TimestampMonth.from_year_quarter(1970, -1)  # noqa

        with self.assertRaises(ValueError):
            tm_bad = TimestampMonth.from_year_quarter(1970, 4)  # noqa

    def test_operators(self):
        tm1 = TimestampMonth.from_year_month(2025, 0)  # noqa
        self.assertTrue(tm1 == tm1)
        self.assertTrue(tm1 >= tm1)
        self.assertTrue(tm1 <= tm1)
        self.assertFalse(tm1 != tm1)
        self.assertFalse(tm1 > tm1)
        self.assertFalse(tm1 < tm1)

        tm2 = TimestampMonth.from_year_month(2025, 0)  # noqa
        self.assertTrue(tm1 == tm2)
        self.assertTrue(tm1 >= tm2)
        self.assertTrue(tm1 <= tm2)
        self.assertFalse(tm1 != tm2)
        self.assertFalse(tm1 > tm2)
        self.assertFalse(tm1 < tm2)

        tm3 = TimestampMonth.from_year_month(2025, 1)
        self.assertFalse(tm1 == tm3)
        self.assertFalse(tm1 >= tm3)
        self.assertTrue(tm1 <= tm3)
        self.assertTrue(tm1 != tm3)
        self.assertFalse(tm1 > tm3)
        self.assertTrue(tm1 < tm3)

        tm4 = TimestampMonth.from_year_month(2024, 11)
        self.assertFalse(tm1 == tm4)
        self.assertTrue(tm1 >= tm4)
        self.assertFalse(tm1 <= tm4)
        self.assertTrue(tm1 != tm4)
        self.assertTrue(tm1 > tm4)
        self.assertFalse(tm1 < tm4)

    def test_arithmetic(self):
        tm1 = TimestampMonth.from_year_month(2024, 0) + 1
        tm2 = TimestampMonth.from_year_month(2024, 1)
        self.assertEqual(tm1, tm2)

        tm1 = TimestampMonth.from_year_month(2024, 11) + 1
        tm2 = TimestampMonth.from_year_month(2025, 0)
        self.assertEqual(tm1, tm2)

        tm1 = TimestampMonth.from_year_month(2024, 0) - 1
        tm2 = TimestampMonth.from_year_month(2023, 11)
        self.assertEqual(tm1, tm2)

        tm1 = TimestampMonth.from_year_month(2024, 11) - 1
        tm2 = TimestampMonth.from_year_month(2024, 10)
        self.assertEqual(tm1, tm2)

