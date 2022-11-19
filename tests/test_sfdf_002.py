# type: ignore
from sfdf import *


def join_two_tables_containing_identical_names_should_fail_with_examples():
    """
    post: __return__ == True
    """
    a = {"a": [1, 2], "b": [1, 3]}
    b = {"a": [1, 2], "b": [1, 3]}
    r = join(a, b, on={"a"})
    return True


def join_two_tables_containing_identical_names_should_fail(
    k: DfSymbolicInternal,
    p: DfSymbolicInternal
):
    """
    pre: valid_dataframe(k)
    pre: valid_dataframe(p)
    pre: all(c in k for c in ["a", "b"])
    pre: all(c in p for c in ["a", "b"])
    post: __return__ == True
    """
    r = join(k, p, on={"a"})
    return True
