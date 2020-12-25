import socket
import re
import pickle
import os
from threading import Thread

from basic_abstraction.base import Registrable
from utils import Logging


class PerfectLink(Registrable):
    """This class implements a perfect link.

    This class uses unix domain sockets for IPC operations. It is a Registrable
    such that it may serve multiple abstractions. An Abstraction object must go
    through the generate_abstraction_caller method instead of using the send
    method directly.

    """

    SEND = 0
    MAX_LEN = 1024

    def __init__(self, process_number):
        super().__init__()
        self.process_number = process_number
        self.create_socket()
        self.listener = Thread(target=self.receive)
        self.logger = Logging(self.process_number, "LINK")

    def start(self):
        super().start()
        self.listener.start()

    def create_socket(self):
        server_address = self.get_address(self.process_number)

        try:
            os.unlink(server_address)
        except OSError:
            if os.path.exists(server_address):
                raise

        self.socket = socket.socket(socket.AF_UNIX, socket.SOCK_DGRAM)
        self.socket.bind(server_address)
        self.socket.settimeout(self.TIMEOUT)

    def send(self, destination_process, callback_id, args=(), kwargs={}):
        message = (callback_id, args, kwargs)
        if self.alive:
            data = pickle.dumps(message)
            if len(data) > self.MAX_LEN:
                raise Exception(
                    f"Message exceding maximum length of {self.MAX_LEN} bytes, received {len(data)} bytes"
                )
            self.logger.log_debug(f"Sending {(args, kwargs)} to {destination_process}")
            try:
                self.socket.sendto(data, self.get_address(destination_process))
            except Exception as e:
                self.logger.log_debug(
                    f"Message {message} for {destination_process} dropped"
                )
        else:
            self.logger.log_debug(f"Not send {message} to {destination_process}")

    def receive(self):
        while self.alive:
            try:
                data, source = self.socket.recvfrom(self.MAX_LEN)
            except socket.timeout:
                continue
            else:
                callback_id, args, kwargs = pickle.loads(data)
                source_number = self.get_process(source)
                self.logger.log_debug(f"Received {(args, kwargs)} from {source_number}")
                self.callback(callback_id, args=args, kwargs=kwargs)
        self.socket.close()
        self.logger.log_debug(f"is done")

    def generate_abstraction_caller(self, callback_id):
        def sender(destination_process, event, args=(), kwargs={}):
            event_name = self.stringify_event(event)
            args = (event_name, self.process_number, *args)
            self.trigger_event(
                self.send, args=(destination_process, callback_id, args, kwargs)
            )

        return sender

    def get_address(self, process_number):
        return f"/tmp/fairlosslink{process_number}.socket"

    def get_process(self, address):
        return int(re.findall("[0-9]+", address)[0])


if __name__ == "__main__":
    import time
    from basic_abstraction.base import Abstraction

    class Test(Abstraction):
        def __init__(self, process_number):
            super().__init__()
            self.link = PerfectLink(process_number)
            self.send = self.link.register_abstraction(self)
            # Logging.set_debug(process_number, "LINK", True)

        def print_stuff(self, source_number, string):
            print(f"Got {string} from {source_number}")

        def print_split_stuff(self, source_number, string):
            print(f"Got {string.split()} from {source_number}")

        def start(self):
            super().start()
            self.link.start()

        def stop(self):
            super().stop()
            self.link.stop()

    test0 = Test(0)
    test1 = Test(1)
    test0.start()
    test1.start()
    test1.send(0, Test.print_stuff, args=("Do this and that",))
    test0.send(1, Test.print_split_stuff, args=("Do this and that",))

    time.sleep(1)
    test0.stop()
    test1.stop()
