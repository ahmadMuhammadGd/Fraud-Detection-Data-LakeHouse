import json

class Logger:
    def __init__(self, format_func=None):
        self.error_log = []
        self.format_func = format_func if format_func else self.default_format

    def default_format(self, index, column, error_type):
        return {'index': index, 'column': column, 'error_type': error_type}

    def log_error(self, index, column, error_type):
        self.error_log.append(self.format_func(index, column, error_type))

    def get_logs(self):
        return json.dumps({'errors': self.error_log}, indent=4)
