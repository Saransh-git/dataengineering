class BaseIngestionException(Exception):
    note = None  # description of error in detail

    def __init__(self, msg: str='Ingestion Exception', **kwargs):
        self.msg: str = msg
        try:
            self.note = kwargs.pop('note')
        except KeyError:
            pass

    def __str__(self):
        error_msg = f"{self.msg} - {self.note}" if self.note else {self.msg}
        return f"{type(self)}: {error_msg}"


class ConnectionNotConfigured(BaseIngestionException):
    def __init__(self, msg='Connection not Configured', **kwargs):
        super().__init__(msg, **kwargs)


class SchemaMisMatch(BaseIngestionException):
    note = "Provided fields don't match with the table schema"

    def __init__(self, msg='Schema mismatch error', **kwargs):
        super().__init__(msg, **kwargs)
