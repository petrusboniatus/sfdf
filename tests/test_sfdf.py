# type: ignore
import sfdf
from datetime import datetime


def test_drop_duplicates():
    sfdf.drop_duplicates({'': [datetime(1, 1, 1, 0, 0)]})

