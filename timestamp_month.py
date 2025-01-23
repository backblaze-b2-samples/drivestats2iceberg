from typing import Self


class TimestampMonth:
    @classmethod
    def _check_year(cls, year: int):
        if not isinstance(year, int):
            raise TypeError('year must be an int')

    @classmethod
    def _check_month(cls, month):
        if not isinstance(month, int):
            raise TypeError('month must be an int')
        if month not in range(0, 12):
            raise ValueError('month is zero-based and must be between 0 and 11')

    @classmethod
    def _check_quarter(cls, quarter):
        if not isinstance(quarter, int):
            raise TypeError('quarter must be an int')
        if quarter not in range(0, 4):
            raise ValueError('quarter is zero-based and must be between 0 and 3')

    @classmethod
    def from_year_month(cls, year: int, month: int) -> Self:
        cls._check_year(year)
        cls._check_month(month)
        return TimestampMonth(((year - 1970) * 12) + month)

    @classmethod
    def from_year_quarter(cls, year: int, quarter: int) -> Self:
        cls._check_year(year)
        cls._check_quarter(quarter)
        return TimestampMonth(((year - 1970) * 12) + (quarter * 3))

    def __init__(self, month: int):
        if not isinstance(month, int):
            raise TypeError('month must be an int')
        self._month = month

    def as_year_month(self):
        return (self._month // 12) + 1970, self._month % 12

    def __eq__(self, other: Self) -> bool:
        if self._month == other._month:
            return True
        else:
            return False

    def __ne__(self, other: Self) -> bool:
        if self._month != other._month:
            return True
        else:
            return False

    def __gt__(self, other: Self) -> bool:
        if self._month > other._month:
            return True
        else:
            return False

    def __lt__(self, other: Self) -> bool:
        if self._month < other._month:
            return True
        else:
            return False

    def __ge__(self, other: Self) -> bool:
        if self._month >= other._month:
            return True
        else:
            return False

    def __le__(self, other: Self) -> bool:
        if self._month <= other._month:
            return True
        else:
            return False

    def __add__(self, other: int) -> Self:
        if not isinstance(other, int):
            raise TypeError('operand must be an int')
        return TimestampMonth(self._month + other)

    def __sub__(self, other: int) -> Self:
        if not isinstance(other, int):
            raise TypeError('operand must be an int')
        return TimestampMonth(self._month - other)

    def month(self) -> int:
        return self._month % 12

    def quarter(self) -> int:
        return (self._month // 3) % 4

    def year(self) -> int:
        return (self._month // 12) + 1970
