from basic_abstraction.base import Abstraction
from basic_abstraction.link import PerfectLink

from utils import Logging

class Broadcast(Abstraction):
    BROADCAST = 0
    RECEIVE = 1
    def __init__(self, link):
        super().__init__()
        self.event_handler_map = {
            Broadcast.BROADCAST: self.broadcast,
            Broadcast.RECEIVE: self.receive
        }
        self.peers = set()
        self.link = link
        self.process_number = self.link.process_number
        self.send = link.register(self)
        self.clients = []
        self.client_id = 0

    def broadcast(self, client_id, operation_id, args=(), kwargs={}):
        pass

    def receive(self, source_number, client_id, operation_id, args=(), kwargs={}):
        pass

    def add_peers(self, *peers):
        self.peers.update(peers)

    def register(self, client):
        self.clients.append(client)
        self.client_id += 1
        return self.generate_broadcaster(self.client_id - 1)
    
    def generate_broadcaster(self, client_id):
        def broadcaster(operation_id, args=(), kwargs={}):
            self.trigger_event(self.BROADCAST, args=(client_id, operation_id, args, kwargs))
        return broadcaster

class BestEffortBroadcast(Broadcast):
    def __init__(self, link):
        super().__init__(link)
        self.logger = Logging(self.process_number, "BEB")

    def broadcast(self, client_id, operation_id, args=(), kwargs={}):
        self.logger.log_debug(f"Broadcasting {(operation_id, args, kwargs)}")
        for peer in self.peers:
            self.send(peer, self.RECEIVE, args=(client_id, operation_id, args, kwargs))

    def receive(self, source_number, client_id, operation_id, args=(), kwargs={}):
        self.logger.log_debug(f"Receiving {(operation_id, args, kwargs)} from {source_number}")
        self.clients[client_id].trigger_event(operation_id, args=(source_number, *args), kwargs=kwargs)

class EagerReliableBroadcast(Broadcast):
    def __init__(self, link, max_concurrent_messages=20):
        super().__init__(link)
        self.delivered = [None] * max_concurrent_messages
        self.delivered_cycle = 0
        self.timestamp = 0
        self.logger = Logging(self.process_number, "ERB")

    def register_delivered(self, message):
        self.delivered[self.delivered_cycle] = message
        self.delivered_cycle = (self.delivered_cycle + 1) % len(self.delivered)

    def broadcast(self, client_id, operation_id, args=(), kwargs={}):
        self.logger.log_debug(f"Broadcasting {(operation_id, args, kwargs)}")
        message = (self.timestamp, self.process_number, client_id, operation_id, args, kwargs)
        self.timestamp += 1
        self._broadcast(message)

    def receive(self, source_number, timestamp, original_source, client_id, operation_id, args=(), kwargs={}):
        message = (timestamp, original_source, client_id, operation_id, args, kwargs)
        if message not in self.delivered:
            self.logger.log_debug(f"Receiving {(operation_id, args, kwargs)} from {original_source}")
            self.register_delivered(message)
            self.clients[client_id].trigger_event(operation_id, args=(original_source, *args), kwargs=kwargs)
            self._broadcast(message)

    def _broadcast(self, message):
        for peer in self.peers:
            self.send(peer, self.RECEIVE, args=message)


if (__name__ == "__main__"):
    import time
    print("STARTING BEB")
    class TestBEB(Abstraction):
        DELIVER = 0
        def __init__(self, process_number):
            super().__init__()
            self.event_handler_map = {
                self.DELIVER: self.deliver
            }
            self.process_number = process_number
            self.link = PerfectLink(self.process_number)
            self.beb = BestEffortBroadcast(self.link)
            self.broadcast = self.beb.register(self)

        def deliver(self, source_number, message):
            print(f"{self.process_number}: {message} from {source_number}")

        def start(self):
            super().start()
            self.link.start()
            self.beb.start()

        def stop(self):
            super().stop()
            self.link.stop()
            self.beb.stop()
            

    test0 = TestBEB(0)
    test1 = TestBEB(1)
    test2 = TestBEB(2)
    test0.start()
    test1.start()
    test2.start()
    test0.beb.add_peers(0, 1, 2)
    test1.beb.add_peers(0, 1, 2)
    test2.beb.add_peers(0, 1, 2)
    
    test0.broadcast(test0.DELIVER, args=("Hello",))
    test1.broadcast(test0.DELIVER, args=("LEL",))
    test2.broadcast(test0.DELIVER, args=("lolilol",))

    time.sleep(1)
    test0.stop()
    test1.stop()
    test2.stop()

    print()
    print("STARTING ERB")

    class TestERB(Abstraction):
        DELIVER = 0
        def __init__(self, process_number):
            super().__init__()
            self.event_handler_map = {
                self.DELIVER: self.deliver
            }
            self.process_number = process_number
            self.link = PerfectLink(self.process_number)
            self.erb = EagerReliableBroadcast(self.link)
            self.broadcast = self.erb.register(self)
            #Logging.set_debug(self.process_number, "LINK", True)

        def deliver(self, source_number, message):
            print(f"{self.process_number}: {message} from {source_number}")

        def start(self):
            super().start()
            self.link.start()
            self.erb.start()

        def stop(self):
            super().stop()
            self.link.stop()
            self.erb.stop()

    
    test0 = TestERB(0)
    test1 = TestERB(1)
    test2 = TestERB(2)
    
    test0.start()
    test1.start()
    test2.start()
    test0.erb.add_peers(2)
    test1.erb.add_peers(2)
    test2.erb.add_peers(0, 1, 2)
    test0.broadcast(test0.DELIVER, args=("Hello",))
    test1.broadcast(test0.DELIVER, args=("LEL",))
    test2.broadcast(test0.DELIVER, args=("lolilol",))

    time.sleep(1)
    test0.stop()
    test1.stop()
    test2.stop()
