class PcException(Exception):

    def __init__(self, **kwargs):
        self.message = 'Generic exception'

        for kw, arg in kwargs.iteritems():
            setattr(self, kw, arg)

class PcRunTimeException(PcException):

    def __init__(self, **kwargs):
        self.message = 'Runtime exception'

        super(PcInsufficentDataException, self).__init__(kwargs)

class PcInvalidArgException(PcException):

    def __init__(self, **kwargs):
        self.message = 'Invalid argument'

        super(PcInvalidArgException, self).__init__(kwargs)
