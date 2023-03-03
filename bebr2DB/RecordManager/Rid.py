
class Rid:
    def __init__(self, page_num, slot_num) -> None:
        self.page_num = page_num
        self.slot_num = slot_num

    def __eq__(self, __o) -> bool:
        return __o.page_num == self.page_num and __o.slot_num == self.slot_num
