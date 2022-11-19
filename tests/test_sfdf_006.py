# type: ignore
from sfdf import *

def use_a_column_without_checking_the_types(
    k: DfSymbolicInternal,
) -> DfSymbolicInternal:
    """
    pre: "a" in k
    pre: type(k["a"][0]) == int
    pre: "new" not in k
    pre: valid_dataframe(k)
    post: valid_dataframe(__return__)
    """
    return assign(k, new=lambda x: [a + "42" for a in x["a"]])
