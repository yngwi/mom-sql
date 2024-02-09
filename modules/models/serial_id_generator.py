class _Borg:
    _shared_state = {}

    def __init__(self):
        self.__dict__ = self._shared_state


class SerialIDGenerator(_Borg):
    def __init__(self):
        super().__init__()
        if "counters" not in self.__dict__:
            self.counters = {}

    def get_serial_id(self, class_name: str) -> int:
        if class_name not in self.counters:
            self.counters[class_name] = 0
        self.counters[class_name] += 1
        return self.counters[class_name]
