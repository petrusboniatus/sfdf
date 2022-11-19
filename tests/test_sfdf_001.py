# type: ignore
from sfdf import *

def select_should_fail(k: DfSymbolicInternal, p: DfSymbolicInternal) -> bool:
    """
    pre: valid_dataframe(k)
    pre: "a" in k and "b" in k
    pre: valid_dataframe(p)
    pre: "a" in p
    post: __return__ == True
    """

    k = select(k, {"a", "b"})
    k = join(k, p, on={"a"})
    k = select(k, {"k"})

    return True
