from dataclasses import dataclass, replace
from typing import List, Union, Tuple, Set, Callable, Dict, Type, Any, TypeVar, Generic
import collections
import crosshair
import pandas as pd
from expr import LazyExpr


INTEGER_COL_TYPE_ID = 0
STRING_COL_TYPE_ID = 1
FLOAT_COL_TYPE_ID = 2
BOOL_COL_TYPE_ID = 3
N_COL_TYPES = 4


ColunmReference = int
ValidColType = Union[int, str, float, bool]
ColValueAccessor = Callable[[ColunmReference], ValidColType]
MappingFunction = Union[
    Callable[[ColValueAccessor], int],
    Callable[[ColValueAccessor], float],
    Callable[[ColValueAccessor], str],
    Callable[[ColValueAccessor], bool]
]

@dataclass
class Expr:
    fn: MappingFunction

def _type_to_id(k: Type[ValidColType]) -> int:
    if k == int:
        return INTEGER_COL_TYPE_ID
    elif k == str:
        return STRING_COL_TYPE_ID
    elif k == float:
        return FLOAT_COL_TYPE_ID
    elif k == bool:
        return BOOL_COL_TYPE_ID
    else:
        raise TypeError("Type not supported")

@dataclass
class ColSpec:
    ctyp: int
    l_0: int
    l_1: int
    l_2: int
    l_3: int
    l_rest: List[int]


@dataclass
class Sfdf:
    size: int
    col_hashes: List[int]
    col_spces: List[ColSpec]
    possible_string_values: List[str]
    possible_float_values: List[float]


def required_cols(df: Sfdf, cols: List[int]) -> bool:
    return df.col_hashes[:len(cols)] == [c for c in cols]


def valid_df(df: Sfdf) -> bool:
    return (
        len(df.col_hashes) == len(df.col_spces)
        and len(df.col_hashes) > 0
        and df.size > 1
        and len(df.possible_string_values) > 10
        and len(df.possible_float_values) > 10
    )


def c(col_name: str) -> int:
    with crosshair.NoTracing():
        return hash(col_name)


def shape(df: Sfdf) -> Tuple[int, int]:
    return len(df.col_hashes), df.size


def get_element(df: Sfdf, col: int, row: int) -> Union[ValidColType]:
    spec = df.col_spces[col]
    if row >= df.size:
        raise IndexError(f"row index of {row} does not exists on dataframe of shape {shape(df)}")
    spec_list = [spec.l_0, spec.l_1, spec.l_2, spec.l_3] + spec.l_rest
    col_type = spec.ctyp % N_COL_TYPES
    if col_type == INTEGER_COL_TYPE_ID:
        return spec_list[row % len(spec_list)]
    elif col_type == STRING_COL_TYPE_ID:
        return df.possible_string_values[spec_list[row % len(spec_list)]
                                         % len(df.possible_string_values)]
    elif col_type == FLOAT_COL_TYPE_ID:
        return df.possible_float_values[spec_list[row % len(spec_list)]
                                        % len(df.possible_float_values)]
    else:
        col_type == BOOL_COL_TYPE_ID
        return spec_list[row % len(spec_list)] > 0

    raise IndexError("col type out of index, this should not happend")


def select(df: Sfdf, cols: Set[ColunmReference]) -> Sfdf:
    """
    pre: valid_df(df)
    pre: all(c in df.col_hashes for c in cols)
    pre: len(cols) > 1
    post: valid_df(__return__)
    post: c in __return__.col_hashes if c in cols else c not in __return__.col_hashes
    post: shape(__return__) == (len(cols), shape(df)[1])
    """
    col_list = list(cols)
    return replace(
        df,
        col_hashes=col_list,
        col_spces=[df.col_spces[df.col_hashes.index(c)] for c in col_list]
    )


def assing(df: Sfdf, maps: Dict[ColunmReference, Expr]) -> Sfdf:
    """
    pre: valid_df(df)
    pre: len(maps) > 0
    post: valid_df(__return__)
    post: shape(__return__) == (shape(df)[0] + len(maps), shape(df)[1])
    """
    new_cols_hashes = []
    new_cols_specs = []
    new_possible_floats = list(df.possible_float_values)
    new_possible_strings = list(df.possible_string_values)
    for k, expr in maps.items():
        new_cols_hashes.append(k)
        typ = _type_to_id(type(expr.fn(lambda x: get_element(df, x, 0))))
        result_data = [expr.fn(lambda x, c=idx: get_element(df, x, c)) for idx in range(df.size)]
        data = []
        if typ == INTEGER_COL_TYPE_ID or typ == BOOL_COL_TYPE_ID:
            data = result_data
        else:
            lookup_table = None
            if typ == FLOAT_COL_TYPE_ID:
                lookup_table = new_possible_floats
            elif typ == STRING_COL_TYPE_ID:
                lookup_table = new_possible_strings
            else:
                raise TypeError(f"invalid col type {typ}")

            for i, r in enumerate(result_data):
                if r in lookup_table:  # This can get out of hand very fast
                    data.append(lookup_table.index(r))
                else:
                    data.append(len(lookup_table))
                    lookup_table.append(r)

        get_or_default = lambda l, idx: l[idx] if df.size > idx else -1
        new_cols_specs.append(ColSpec(
            ctyp=typ,
            l_0=get_or_default(data, 0),
            l_1=get_or_default(data, 1),
            l_2=get_or_default(data, 2),
            l_3=get_or_default(data, 3),
            l_rest=data[4:] if df.size > 4 else []
        ))

    return replace(
        df,
        col_hashes=df.col_hashes + new_cols_hashes,
        col_spces=df.col_spces + new_cols_specs,
        possible_string_values=new_possible_strings,
        possible_float_values=new_possible_floats
    )

