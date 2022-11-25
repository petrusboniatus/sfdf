from dataclasses import dataclass, replace
from typing import List, Union, Tuple, Set, Callable, Dict, Type, Any, TypeVar, Generic
import collections
import crosshair
import pandas as pd

from dataclasses import dataclass, replace
from typing import List, Union, Tuple, Set, Callable, Dict, Type, Any, TypeVar, Generic
import collections
import crosshair
import pandas as pd


INTEGER_COL_TYPE_ID = 0
STRING_COL_TYPE_ID = 1
FLOAT_COL_TYPE_ID = 2
BOOL_COL_TYPE_ID = 3
N_COL_TYPES = 4


ColunmReference = int
ValidColType = Union[int, str, float, bool]

@dataclass
class ColValueAccessor:
    fn: Callable[[ColunmReference], ValidColType]

MappingFunction = Union[
    Callable[[ColValueAccessor], int],
    Callable[[ColValueAccessor], float],
    Callable[[ColValueAccessor], str],
    Callable[[ColValueAccessor], bool]
]

def c(col_name: str) -> int:
    with crosshair.NoTracing():
        return hash(col_name)

T = TypeVar("T")
class LazyExpr(Generic[T]):
    sql: str
    fn: Callable[[ColValueAccessor], T]

    def __init__(self, sql: str, fn: Callable[[ColValueAccessor], T]) -> 'LazyExpr[T]]':
        self.sql = sql
        self.fn = fn

    def eq(self, o: 'LazyExpr[T]') -> 'LazyExpr[bool]':
        return LazyExpr(sql=f"({self.sql} = {o.sql})", fn=lambda acc: self.fn(acc) == o.fn(acc))

    def neq(self, o: 'LazyExpr[T]') -> 'LazyExpr[bool]':
        return LazyExpr(sql=f"({self.sql} <> {o.sql})", fn=lambda acc: self.fn(acc) != o.fn(acc))

    def and_(self, o: 'LazyExpr[bool]') -> 'LazyExpr[bool]':
        return LazyExpr(sql=f"({self.sql} AND {o.sql})", fn=lambda acc: self.fn(acc) and o.fn(acc))

    def __invert__(self) -> 'LazyExpr[bool]':
        return LazyExpr(sql=f"(NOT {self.sol})", fn=lambda acc: not self.fn(acc))

    def or_(self, o: 'LazyExpr[bool]') -> 'LazyExpr[bool]':
        return LazyExpr(sql=f"({self.sql} OR {o.sql})", fn=lambda acc: self.fn(acc) or o.fn(acc))

    def __lt__(self, o: 'LazyExpr[T]') -> 'LazyExpr[bool]':
        return LazyExpr(sql=f"({self.sql} < {o.sql})", fn=lambda acc: self.fn(acc) < o.fn(acc))

    def __gt__(self, o: 'LazyExpr[T]') -> 'LazyExpr[bool]':
        return LazyExpr(sql=f"({self.sql} > {o.sql})", fn=lambda acc: self.fn(acc) > o.fn(acc))

    def __le__(self, o: 'LazyExpr[T]') -> 'LazyExpr[bool]':
        return LazyExpr(sql=f"({self.sql} <= {o.sql})", fn=lambda acc: self.fn(acc) <= o.fn(acc))

    def __ge__(self, o: 'LazyExpr[T]') -> 'LazyExpr[bool]':
        return LazyExpr(sql=f"({self.sql} >= {o.sql})", fn=lambda acc: self.fn(acc) >= o.fn(acc))

    def __add__(self, o: 'LazyExpr[T]') -> 'LazyExpr[T]':
        return LazyExpr(sql=f"({self.sql} + {o.sql})", fn=lambda acc: self.fn(acc) + o.fn(acc))

    def __sub__(self, o: 'LazyExpr[T]') -> 'LazyExpr[T]':
        return LazyExpr(sql=f"({self.sql} - {o.sql})", fn=lambda acc: self.fn(acc) - o.fn(acc))

    def __mul__(self, o: 'LazyExpr[T]') -> 'LazyExpr[T]':
        return LazyExpr(sql=f"({self.sql} * {o.sql})", fn=lambda acc: self.fn(acc) * o.fn(acc))
    
    def __div__(self, o: 'LazyExpr[T]') -> 'LazyExpr[T]':
        return LazyExpr(sql=f"({self.sql} / {o.sql})", fn=lambda acc: self.fn(acc) / o.fn(acc))


def _lit_to_sql_str(e: object) -> str:
    if type(e) in [int, float, bool]:
        lit_str = str(e).upper()
    elif type(e) is None:
        lit_str = "NULL"
    elif type(e) == str:
        lit_str = f"'{e}'"
    elif isinstance(e, collections.Sequence):
        lit_str = "(" + ",".join(_lit_to_sql_str(it) for it in e) + ")"
    return lit_str


def lit(e: T) -> LazyExpr[T]:
    return LazyExpr(sql=_lit_to_sql_str(e), fn=lambda x, k=e: k)


def _untyped_col(c: str) -> LazyExpr[Any]:
    return LazyExpr(sql=c, fn=lambda acc: acc(c(ic)))


def i64_col(c: str) -> LazyExpr[int]:
    return untyped_col(c)


def f64_col(c: str) -> LazyExpr[float]:
    return _untyped_col(c)


def bool_col(c: str) -> LazyExpr[bool]:
    return _untyped_col(c)


def str_col(c: str) -> LazyExpr[str]:
    return _untyped_col(c)
