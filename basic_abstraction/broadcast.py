from basic_abstraction.link import FairLossLink
import pickle

class Broadcast:
    def __init__(self, link, deliver_callback):
        self.link = link
        self.deliver_callback = deliver_callback

    def broadcast(self, message):
        pass

    def add_peers(self, peers_number_list):
        pass


class BestEffortBroadcast(Broadcast):
    def __init__(self, link, deliver_callback, loss=0.0):
        super().__init__(link, deliver_callback)
        self.link.add_callback(self.receive)
        self.peers = []
        
    def add_peers(self, peers_number_list):
        self.peers.extend(peers_number_list)

    def broadcast(self, message):
        for peer in self.peers:
            self.link.send(peer, message)

    def receive(self, source_number, message):
        self.deliver_callback(source_number, message)

class EagerReliableBroadcast(Broadcast):
    def __init__(self, link, deliver_callback, loss=0.0):
        super().__init__(link, deliver_callback)
        self.be_broadcast = BestEffortBroadcast(link, self.be_deliver, loss)
        self.delivered = [None] * 20
        self.delivered_cycle = 0

    def add_peers(self, peers_number_list):
        self.be_broadcast.add_peers(peers_number_list)

    def broadcast(self, message):
        raw_message = pickle.dumps((self.link.process_number, message.decode("utf-8")))
        self.be_broadcast.broadcast(raw_message)

    def be_deliver(self, source_number, raw_message):
        source, message = pickle.loads(raw_message)
        if message not in self.delivered:
            self.delivered[self.delivered_cycle] = message
            self.delivered_cycle = (self.delivered_cycle + 1) % 20
            self.be_broadcast.broadcast(raw_message)
            self.deliver_callback(source, message)

if (__name__ == "__main__"):
    class Test:
        def __init__(self, process_number):
            self.process_number = process_number
            self.link = FairLossLink(process_number)
            self.beb = EagerReliableBroadcast(self.link, self.deliver)

        def deliver(self, source_number, message):
            print("{}: Message received {} from {}".format(self.process_number, message.encode("utf-8"), source_number))

    test0 = Test(0)
    test1 = Test(1)
    test2 = Test(2)

    test0.beb.add_peers([0,1,2])
    test1.beb.add_peers([0,1,2])
    test2.beb.add_peers([0,1,2])

    test0.beb.broadcast("Hello world !".encode("utf-8"))
    test2.beb.broadcast("Hello back".encode("utf-8"))

