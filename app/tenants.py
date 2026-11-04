class ApiError(Exception):
    def __init__(self, http_status, code, message, **details):
        self.http_status = http_status
        self.code = code
        self.message = message
        self.details = details