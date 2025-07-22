class CustomHTTPException(Exception):
    def __init__(self, status_code: int, detail: dict):
        self.status_code = status_code
        self.detail = detail