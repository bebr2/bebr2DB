
from .Rid import Rid

class RM_Record:
    def __init__(self, data_, rid_) -> None:
        self.rid = Rid(-1, -1)
        if data_ is not None:
            self.rid = rid_
        self.data = data_.copy() if data_ is not None else data_