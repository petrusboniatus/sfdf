# type: ignore
from sfdf import *


def division_by_zero_waiting_to_happen(
    k: DfSymbolicInternal,
    base: int
) -> DfSymbolicInternal:
    """
    pre: valid_dataframe(k)
    pre: "x" in k and type(k["x"][0]) == float
    pre: "y" in k and type(k["y"][0]) == float
    pre: "achieved" not in k
    pre: "result" not in k
    post: "result" in k
    post: valid_dataframe(__return__)
    """
    k = assign(k, achieved=lambda df: [base / x for x in df["x"]])
    k = assign(p, result=lambda df: [
        df["achieved"][i] * df["y"][i]
        for i in range(shape(df)[0])
    ])

    return k
