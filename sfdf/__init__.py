from typing import Dict, List, Union, Tuple, Set, Callable, Any
from datetime import datetime

AlowedColumns = Union[List[float], List[int], List[str]]
DfSimbolicInternal = Dict[str, AlowedColumns]


def hash_element(e: Any) -> int:
    return hash(e) if type(e) != datetime else hash(str(e))


def shape(df: DfSimbolicInternal) -> Tuple[int, int]:
    """
    pre: len(df) != 0
    """
    return (len(df), len(next(iter(df.values()))))


def valid_dataframe(df: DfSimbolicInternal) -> bool:
    n_cols, n_rows = shape(df)
    return n_cols > 0 and n_rows > 0 and all(len(df[c]) == n_rows for c in df)


def select(df: DfSimbolicInternal, cols: Set[str]) -> DfSimbolicInternal:
    """
    pre: len(df) != 0
    pre: len(cols) > 0
    pre: all(len(v) == shape(df)[1] for v in df.values())
    pre: all(c in df for c in cols)
    post: all(c in __return__ for c in cols)
    post: __return__ != None
    post: shape(__return__) == (len(cols), shape(df)[1])
    """
    return {c: df[c] for c in cols}


def drop(df: DfSimbolicInternal, cols: Set[str]) -> DfSimbolicInternal:
    """
    pre: len(df) > len(cols)
    pre: len(cols) > 0
    pre: all(len(v) == shape(df)[1] for v in df.values())
    pre: all(c in df for c in cols)
    post: all(c not in __return__ for c in cols)
    post: all(c in __return__ for c in df if c not in cols)
    post: __return__ != None
    post: shape(__return__) == (shape(df)[0] - len(cols), shape(df)[1])
    """
    return {c: df[c] for c in df if c not in cols}


def union(df_1: DfSimbolicInternal, df_2: DfSimbolicInternal) -> DfSimbolicInternal:
    """
    pre: len(df_1) != 0
    pre: all(c in df_1 for c in df_2)
    pre: all(c in df_2 for c in df_1)
    post: __return__ != None
    post: shape(__return__) == (shape(df_1)[0], shape(df_1)[1] + shape(df_2)[1])
    """
    return {k: [*df_1[k], *df_2[k]] for k in df_1}


def join(df_1: DfSimbolicInternal, df_2: DfSimbolicInternal, on: Set[str]) -> DfSimbolicInternal:
    """
    left inner join

    pre: len(df_1) != 0
    pre: len(df_2) != 0
    pre: all(c in df_1 for c in on)
    pre: all(c in df_2 for c in on)
    post: all(c in __return__ for c in [*df_1, *df_2])
    """
    join_indexes = [
        (i, j)
        for i in range(shape(df_1)[1])
        for j in range(shape(df_2)[1])
        if all(df_1[c][i] == df_2[c][j] for c in on)
    ]

    return {
        c: (
            [df_1[c][i] for i, _ in join_indexes]  # type: ignore
            if c in df_1
            else [df_2[c][j] for _, j in join_indexes]
        )
        for c in {*df_1, *df_2}
    }


def drop_duplicates(df: DfSimbolicInternal) -> DfSimbolicInternal:
    """
    pre: valid_dataframe(df)
    pre: shape(df)[1] > 0
    post: shape(df)[1] >= shape(__return__)[1]
    post: shape(df)[1] > 0
    """
    new_df: DfSimbolicInternal = {c: [] for c in df}  # type: ignore
    row_set = set()
    for i in range(shape(df)[1]):
        row = 0
        for c in df:
            e = df[c][i]
            my_hash = hash_element(e)
            row += my_hash
        if row not in row_set:
            row_set.add(row)
            for c in df:
                e = df[c][i]
                new_df[c].append(e)  # type: ignore

    return new_df


def group_by(
    df: DfSimbolicInternal,
    key: List[str],
    fn: Callable[[DfSimbolicInternal], DfSimbolicInternal]
) -> DfSimbolicInternal:
    """
    pre: valid_dataframe(df)
    pre: all(k in df for k in key)
    pre: valid_dataframe(fn(df))
    post: valid_dataframe(__return__)

    """
    hash_col = [
        sum(hash_element(df[c][i]) for c in df)
        for i in range(shape(df)[1])
    ]

    groups: Dict[int, DfSimbolicInternal] = {}
    for idx, hash_val in enumerate(hash_col):
        if idx not in groups:
            groups[hash_val] = {c: [] for c in df}  # type: ignore
        for c in df:
            groups[hash_val][c].append(df[c][idx])  # type: ignore

    result = None
    for _, group in groups.items():
        next_gp = fn(group)
        if not result:
            result = next_gp
        else:
            assert set(next_gp.keys()) == set(result.keys())
            for c in result:
                result[c].append(next_gp[c])

    return result  # type: ignore


def assign(
    df: DfSimbolicInternal,
    **kwargs: Callable[[DfSimbolicInternal], Union[AlowedColumns]]
) -> DfSimbolicInternal:
    """
    pre: valid_dataframe(df)
    pre: all(len(fn(df)) == shape(df)[1] for fn in kwargs.values())
    pre: not any(k in df for k in kwargs.keys())
    post: valid_dataframe(df)
    post: all(k in __return__ for k in kwargs.keys())
    """
    for k, fn in kwargs.items():
        df[k] = fn(df)

    return df




