import socket
import threading
import multiprocessing
import sys
import os
import re
import random
from queue import Queue
import time

class FairLossLink:
    max_message_length = 1024

    def __init__(self, process_number, receive_callback, loss=0.0):
        self.process_number = process_number
        self.receive_callback = receive_callback
        self.loss = loss
        self.listening_thread = None
        self.worker_thread = None
        self.create_socket()
        self.initialize_listener()

    def create_socket(self):
        server_address = self.get_address(self.process_number)

        try:
            os.unlink(server_address)
        except OSError:
            if os.path.exists(server_address):
                raise

        self.socket = socket.socket(socket.AF_UNIX, socket.SOCK_DGRAM)
        self.socket.bind(server_address)

    def initialize_listener(self):
        self.listening_thread = FairLossLink.ListeningThread(self)
        self.worker_thread = FairLossLink.WorkerThread()
        self.listening_thread.start()
        self.worker_thread.start()

    def send(self, destination_process, message_bytes):
        if (random.uniform(0, 1) >= self.loss):
            if(len(message_bytes) >= FairLossLink.max_message_length):
                raise Exception("Message exceding maximum length of {} bytes, received {} bytes"
                        .format(FairLossLink.max_message_length, len(message_bytes)))
            self.socket.sendto(message_bytes, self.get_address(destination_process))

    def receive(self):
        while True:
            data, source = self.socket.recvfrom(FairLossLink.max_message_length)
            self.worker_thread.put(self.receive_callback, (self.get_process(source), data))

    def get_address(self, process_number):
        return '/tmp/fairlosslink{}.socket'.format(process_number)

    def get_process(self, address):
        return re.findall("[0-9]+", address)[0]

    class ListeningThread(threading.Thread):
        def __init__(self, link):
            super().__init__()
            self.link = link

        def run(self):
            self.link.receive()

    class WorkerThread(threading.Thread):
        def __init__(self):
            super().__init__()
            self.queue = Queue()

        def run(self):
            while True:
                callback, args = self.queue.get()

                callback(*args)

        def put(self, callback, args):
            self.queue.put((callback, args))

if __name__ == "__main__":
    import time 

    class Test:
        def __init__(self, process_number):
            self.last = ""
            self.process_number = process_number

        def receive_callback(self, process_number, message):
            self.last = message
            time.sleep(1)
            print("{}: Received message: {} from {}".format(self.process_number, message.decode("utf-8"), process_number))

    test0 = Test(0)
    test1 = Test(1)

    link0 = FairLossLink(0, test0.receive_callback)
    link1 = FairLossLink(1, test1.receive_callback)

    link0.send(1, "Coucou".encode("utf-8"))
    print("Send Coucou")
    link1.send(0, "Hello".encode("utf-8"))
    print("Send Hello")
    link0.send(0, "Sup".encode("utf-8"))
    print("Send Sup")
