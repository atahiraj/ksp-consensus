import socket
import threading
import multiprocessing
import sys
import os
import re
import random

class FairLossLink:
    max_message_length = 1024

    def __init__(self, process_number, callback, loss=0.0):
        self.process_number = process_number
        self.callback = callback
        self.loss = loss
        self.thread_pool = None
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
        self.thread_pool = multiprocessing.Pool(processes=1)
        thread = FairLossLink.ListeningThread(self)
        thread.start()

    def send(self, destination_process, message):
        if (random.uniform(0, 1) >= self.loss):
            message_bytes = message.encode("utf-8")
            if(len(message_bytes) >= FairLossLink.max_message_length):
                raise Exception("Message exceding maximum length of {} bytes, received {} bytes"
                        .format(FairLossLink.max_message_length, len(message_bytes)))
            self.socket.sendto(message_bytes, self.get_address(destination_process))

    def receive(self):
        while True:
            data, source = self.socket.recvfrom(FairLossLink.max_message_length)
            self.thread_pool.apply_async(self.callback, (data.decode("utf-8"), self.get_process(source)))

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

if __name__ == "__main__":
    import time 

    class Test:
        def __init__(self, process_number):
            self.last = ""
            self.process_number = process_number

        def callback(self, process_number, message):
            self.last = message
            time.sleep(1)
            print("{}: Received message: {} from {}".format(self.process_number, message, process_number))

    test0 = Test(0)
    test1 = Test(1)

    link0 = FairLossLink(0, test0.callback)
    link1 = FairLossLink(1, test1.callback)

    link0.send(1, "Coucou")
    print("Send Coucou")
    link1.send(0, "Hello")
    print("Send Hello")
    link0.send(0, "Sup")
    print("Send Sup")
