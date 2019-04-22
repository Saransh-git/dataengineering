class BlankParamValue:
    """
    Denotes no value for the parameter provided.
    """
    def __init__(self, param):
        self.param = param

    def __str__(self):
        return f"Blank value for parameter {self.param}"
