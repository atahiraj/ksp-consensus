class Logging:
    debug = {}
    existing_logger = {}

    def set_debug(process_number, namespace, value):
        Logging.debug[(process_number, namespace)] = value

    def __init__(self, process_number, namespace):
        self.process_number = process_number
        self.namespace = namespace
        self.id = self.get_next_logger_id()
        if self.is_debug() is None:
            Logging.set_debug(process_number, namespace, False)

    def get_next_logger_id(self):
        if self.get_uuid() in Logging.existing_logger:
            Logging.existing_logger[self.get_uuid()] += 1
        else:
            Logging.existing_logger[self.get_uuid()] = 0
        return Logging.existing_logger[self.get_uuid()]

    def get_uuid(self):
        return (self.process_number, self.namespace)

    def is_debug(self):
        if self.get_uuid() in Logging.debug:
            return Logging.debug[self.get_uuid()]
        return None

    def log_debug(self, message):
        if self.is_debug():
            print(f"Process {self.process_number} ({self.namespace}): {message}")
