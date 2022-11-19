# type: ignore
from sfdf import *


def create_cols_based_on_parameters_bad_preconditions(
    k: DfSymbolicInternal,
    my_adds: List[int]
) -> DfSymbolicInternal:
    """
    should fail because my_adds can have the same thing multiple times

    pre: valid_dataframe(k)
    pre: "base_col" in k
    pre: type(k["base_col"][0]) == int
    post: valid_dataframe(__return__)
    """
    for e in my_adds:
        assign_function = lambda x: [i + e for i in x['base_col']]
        k = assign(k, **{f"col_{e}": assign_function})

    return k
