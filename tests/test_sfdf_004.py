# type: ignore
from sfdf import *


def left_inner_join_using_non_primary_keys_produce_duplicates_with_example() -> DfSymbolicInternal:
    """
    post: shape(__return__)[1] <= 2
    """
    a = {"a": [1, 1], "b": [1, 2]}
    b = {"a": [1, 1], "c": [1, 0]}
    k = join(a, b, on={"a"})

    return k



def left_inner_join_using_non_primary_keys_produce_duplicates(
    k: DfSymbolicInternal,
    p: DfSymbolicInternal
) -> DfSymbolicInternal:
    """
    pre: valid_dataframe(k)
    pre: valid_dataframe(p)
    pre: all(e in p for e in ["a", "b"])
    pre: all(e in k for e in ["a", "c"])
    post: valid_dataframe(__return__)
    post: shape(__return__)[1] <= shape(k)[1]
    """
    return join(k, p, {"a"})
