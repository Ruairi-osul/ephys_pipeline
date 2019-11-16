class Error(Exception):
    pass


class DuplicateError(Error):
    pass


class NoNeuronsError(Error):
    pass


class CurruptDataError(Error):
    pass
