import socket
import re
import pickle
import os
from threading import Thread

from base import Abstraction

class PerfectLink(Abstraction):
    SEND = 0
    DELIVER = 1
    MAX_LEN = 1024

    def __init__(self, process_number):
        super().__init__()
        self.process_number = process_number
        self.clients = []
        self.client_id = 0
        self.event_handler_map = {
            self.SEND: self.send,
            self.DELIVER: self.deliver
        }
        self.create_socket()
        self.listener = Thread(target=self.receive)

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

    def send(self, destination_process, client_id, message):
        if self.alive:
            message = (client_id, message)
            data = pickle.dumps(message)
            if len(data) > self.MAX_LEN:
                raise Exception(f"Message exceding maximum length of {self.MAX_LEN} bytes, received {len(data)} bytes")
            if self.debug:
                print(f"PL: {self.process_number}: Sending {message[1]} to {destination_process}")
            try:
                self.socket.sendto(data, self.get_address(destination_process))
            except Exception as e:
                if self.debug:
                    print(f"PL: {self.process_number}: Message {message[1]} for {destination_process} dropped")
        elif self.debug:
            print(f"PL: {self.process_number}: Not send {message[1]} to {destination_process}")

    def deliver(self, source_number, client_id, message):
        operation_id, data = message
        message = (source_number, data)
        self.clients[client_id].trigger_event(operation_id, message)

    def receive(self):
        while self.alive:
            try:
                data, source = self.socket.recvfrom(self.MAX_LEN)
            except socket.timeout:
                continue
            else:
                client_id, message = pickle.loads(data)
                source_number = self.get_process(source)
                if self.debug:
                    print(f"PL: {self.process_number}: Received {message} from {source_number}")
                self.trigger_event(self.DELIVER, args=(source_number, client_id, message))
        self.socket.close()
        if self.debug:
           print(f"Link {self.process_number} is done") 

    def register(self, client):
        self.clients.append(client)
        self.client_id += 1
        return self.generate_sender(self.client_id - 1)

    def generate_sender(self, client_id):
        def send(destination_process, message):
            self.trigger_event(self.SEND, args=(destination_process, client_id, message))
        return send

    def get_address(self, process_number):
        return f"/tmp/fairlosslink{process_number}.socket"
    
    def get_process(self, address):
        return int(re.findall("[0-9]+", address)[0])

if __name__ == "__main__":
    from queue import Queue
    import time
    class Test(Abstraction):
        PRINT = 0
        PRINT_SPLIT = 1
        def __init__(self, process_number):
            super().__init__()

            self.event_handler_map = {
                self.PRINT: self.print_stuff,
                self.PRINT_SPLIT: self.print_split_stuff
            }
            self.link = PerfectLink(process_number)
            self.queue = Queue()
            self.send = self.link.register(self)
            #self.link.debug = True

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
    test1.send(0, (Test.PRINT, "Do this and that"))
    test0.send(1, (Test.PRINT_SPLIT, "Do this and that"))

    time.sleep(1)
    test0.stop()
    test1.stop()
