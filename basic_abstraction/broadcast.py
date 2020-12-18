from base import Abstraction
from link import PerfectLink

class Broadcast(Abstraction):
    BROADCAST = 0
    RECEIVE = 1
    def __init__(self, link, abstraction_callback, operation_id_callback):
        super().__init__()
        self.event_handler_map = {
            Broadcast.BROADCAST: self.broadcast,
            Broadcast.RECEIVE: self.receive
        }
        self.process_number = link.process_number
        self.send = link.register(self)
        self.abstraction_callback = abstraction_callback
        self.operation_id_callback = operation_id_callback

    def broadcast(self, message):
        pass

    def receive(self, source_number, message):
        pass

    def add_peers(self, *peers):
        pass

class BestEffortBroadcast(Broadcast):
    def __init__(self, link, abstraction_callback, operation_id_callback):
        super().__init__(link, abstraction_callback, operation_id_callback)
        self.peers = set()
        
    def add_peers(self, *peers):
        self.peers.update(peers)

    def broadcast(self, message):
        if self.debug:
            print(f"BEB: {self.process_number}: broadcasting {message}")
        for peer in self.peers:
            self.send(peer, (self.RECEIVE, message))

    def receive(self, source_number, message):
        if self.debug:
            print(f"BEB: {self.process_number}: receiving {message} from {source_number}")
        self.abstraction_callback.trigger_event(self.operation_id_callback, (source_number, message))


class EagerReliableBroadcast(Broadcast):
    def __init__(self, link, abstraction_callback, operation_id_callback, max_concurrent_messages=20):
        super().__init__(link, abstraction_callback, operation_id_callback)
        self.beb = BestEffortBroadcast(link, self, Broadcast.RECEIVE)
        self.delivered = [None] * max_concurrent_messages
        self.delivered_cycle = 0

    def start(self):
        super().start()
        self.beb.start()

    def stop(self):
        super().stop()
        self.beb.stop()

    def add_peers(self, *peers):
        self.beb.add_peers(*peers)

    def register_delivered(self, message):
        self.delivered[self.delivered_cycle] = message
        self.delivered_cycle = (self.delivered_cycle + 1) % len(self.delivered)

    def broadcast(self, message):
        if self.debug:
            print(f"ERB: {self.process_number}: broadcasting {message}")
        #self.register_delivered(message)
        self.beb.trigger_event(Broadcast.BROADCAST, args=(message,))

    def receive(self, source_number, message):
        if self.debug:
            print(f"ERB: {self.process_number}: receiving {message} from {source_number}")
        if message not in self.delivered:
            self.register_delivered(message)
            self.abstraction_callback.trigger_event(self.operation_id_callback, (source_number, message))
            self.beb.trigger_event(Broadcast.BROADCAST, args=(message,))

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
            self.beb = BestEffortBroadcast(self.link, self, self.DELIVER)

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
    
    test0.beb.broadcast("Hello")
    test1.beb.broadcast("LEL")
    test2.beb.broadcast("lolilol")

    time.sleep(1)
    test0.stop()
    test1.stop()
    test2.stop()
    time.sleep(1)

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
            self.erb = EagerReliableBroadcast(self.link, self, self.DELIVER)

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
    test0.erb.add_peers(0, 1, 2)
    test1.erb.add_peers(0, 1, 2)
    test2.erb.add_peers(0, 1, 2)

    test0.erb.broadcast("Hello")
    test1.erb.broadcast("LEL")
    test2.erb.broadcast("lolilol")

    time.sleep(1)
    test0.stop()
    test1.stop()
    test2.stop()
    time.sleep(1)
