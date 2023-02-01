class BenchmarkGetterError(Exception):
    def __init__(self, message):
        self.message = message

    def __str__(self):
        return "BenchmarkGetterError:" + self.message


class IRParserException(Exception):
    pass
