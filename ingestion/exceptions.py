class BaseIngestionException(Exception):
    def __init__(self, msg='Ingestion Exception', **kwargs):
        self.msg = msg

    def __str__(self):
        return f"{type(self)}: {self.msg}"


class SchemaNotConfigured(BaseIngestionException):
    def __init__(self, msg='Schema Not Configured', **kwargs):
        super().__init__(msg, **kwargs)
