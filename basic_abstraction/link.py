import socket
import threading
import os
import re
import random
import queue
import time
import pickle

class FairLossLink:
    max_message_length = 1024

    def __init__(self, process_number, loss=0.0):
        self.process_number = process_number
        self.receive_callbacks = []
        self.loss = loss
        self.listening_thread = None
        self.worker_thread = None
        self.alive = True
        self.debug = False
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
        self.socket.settimeout(1)


    def initialize_listener(self):
        self.listening_thread = FairLossLink.ListeningThread(self)
        self.worker_thread = FairLossLink.WorkerThread()
        self.listening_thread.start()
        self.worker_thread.start()
        

    def send(self, destination_process, message):
        if (self.alive and random.uniform(0, 1) >= self.loss):
            message_bytes = pickle.dumps(message)
            if(len(message_bytes) >= FairLossLink.max_message_length):
                raise Exception("Message exceding maximum length of {} bytes, received {} bytes"
                        .format(FairLossLink.max_message_length, len(message_bytes)))
            if self.debug:
                print("{}: Sending {} to {}".format(self.process_number, message, destination_process))
            try:
                self.socket.sendto(message_bytes, self.get_address(destination_process))
            except Exception as e:
                if self.debug:
                    print(f"{self.process_number}: Message {message} for {destination_process} dropped")

        elif self.debug:
            print("{}: Not send {} to {}".format(self.process_number, message, destination_process))

    def receive(self):
        while self.alive:
            try:
                data, source = self.socket.recvfrom(FairLossLink.max_message_length)
            except socket.timeout:
                pass
            else:
                message = pickle.loads(data)
                source_number = self.get_process(source)
                if self.debug:
                    print("{}: Received {} from {}".format(self.process_number, message, source_number))
                for callback in self.receive_callbacks:
                    self.worker_thread.put(callback, (source_number, message))
        self.socket.close()


    def get_address(self, process_number):
        return '/tmp/fairlosslink{}.socket'.format(process_number)

    def get_process(self, address):
        return int(re.findall("[0-9]+", address)[0])

    def add_callback(self, receive_callback):
        self.receive_callbacks.append(receive_callback)

    def kill(self):
        self.alive = False
        self.worker_thread.kill()

    def set_debug(self, debug):
        self.debug = debug

    class ListeningThread(threading.Thread):
        def __init__(self, link):
            super().__init__()
            self.link = link

        def run(self):
            self.link.receive()

    class WorkerThread(threading.Thread):
        def __init__(self):
            super().__init__()
            self.queue = queue.Queue()
            self.alive = True

        def run(self):
            while self.alive:
                try:
                    callback, args = self.queue.get(timeout=1)
                except queue.Empty:
                    pass
                else:
                    callback(*args)

        def kill(self):
            self.alive = False

        def put(self, callback, args):
            self.queue.put((callback, args))

if __name__ == "__main__":
    import time 

    class Test:
        def __init__(self, process_number):
            self.process_number = process_number
            self.link = FairLossLink(process_number)
            self.link.add_callback(self.link_receive)

        def link_receive(self, process_number, message):
            time.sleep(1)
            print("{}: Received message: {} from {}".format(self.process_number, message, process_number))

    test0 = Test(0)
    test1 = Test(1)

    def more_callback(process_number, message):
        print("You got mail: {} from {}".format(message, process_number))

    test0.link.add_callback(more_callback)

    test0.link.send(1, "Coucou")
    print("Send Coucou")
    test1.link.send(0, "Hello")
    print("Send Hello")
    test0.link.send(0, "Sup")
    print("Send Sup")

    time.sleep(0.1)

    test0.link.kill()
    test1.link.kill()
