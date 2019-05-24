from ingestion.helpers import BlankParamValue


class BaseIngestionException(Exception):
    desc = "Ingestion Error"  # description of error in detail

    def __init__(self, msg: str='Ingestion Exception', **kwargs):
        self.msg: str = msg
        try:
            self.note = kwargs.pop('note')
        except KeyError:
            pass

    def __str__(self):
        return f"{self.msg} - {self.desc}"


class ConnectionNotConfigured(BaseIngestionException):
    def __init__(self, msg='Connection not Configured', **kwargs):
        super().__init__(msg, **kwargs)


class SchemaMisMatch(BaseIngestionException):

    def __init__(self, msg="Provided fields don't match with the table schema", **kwargs):
        super().__init__(msg, **kwargs)


class StrictIngestionError(BaseIngestionException):
    desc = "Ingesting strictly"

    def __init__(self, param, missing_col=None, **kwargs):
        is_data_mapped = kwargs.get('', False)
        msg = self._construct_msg(param, missing_col, is_data_mapped)
        super().__init__(msg, **kwargs)

    @staticmethod
    def _construct_msg(param, missing_col, is_data_mapped=False):
        if is_data_mapped:
            if not missing_col:
                raise ValueError("Strict Ingestion exception invoked for non missing values.")

            return f"Value missing for {missing_col} at {param}. " \
                   f"Consider removing strict constraint and/or passing a default"

        if not missing_col:
            missing_col = [key for key, val in param.items if isinstance(val, BlankParamValue)]

        if not missing_col:
            raise ValueError("Strict Ingestion exception invoked for non missing values.")

        item = {key: val for key, val in param.items() if not isinstance(val, BlankParamValue)}
        return f"Value missing for {missing_col} at {item}. " \
               f"Consider removing strict constraint and/or passing a default"


class EmptyDataIngestion(BaseIngestionException):
    desc = "Empty data ingestion is not allowed."
