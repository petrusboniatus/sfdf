from sfdf import *
import math

def group_by_sum_without_checking_for_nans_produces_nans(
    k: DfSymbolicInternal
) -> DfSymbolicInternal:
    """
    pre: valid_dataframe(k)
    pre: "potato" in k
    pre: "value" in k
    pre: type(k["value"][0]) == float
    post: valid_dataframe(__return__)
    post: not any(math.isnan(e) for e in __return__["value"])
    """
    k = select(k, {"potato", "value"})
    k = drop_duplicates(k)
    group_by(k, {"potato"}, lambda group: {
        "potato": group["potato"],
        "value": [sum(group["value"])]
    })

    return k
