from basic_abstraction.link import FairLossLink

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
            self.link.send(peer, ("beb", message))

    def receive(self, source_number, raw_message):
        if raw_message[0] == "beb":
            message = raw_message[1]
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
        raw_message = ("erb", self.link.process_number, message)
        self.be_broadcast.broadcast(raw_message)

    def be_deliver(self, source_number, raw_message):
        mess_type, source, message = raw_message
        if mess_type == "erb":
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
            print("{}: Message received {} from {}".format(self.process_number, message, source_number))

    test0 = Test(0)
    test1 = Test(1)
    test2 = Test(2)

    test0.beb.add_peers([0,1,2])
    test1.beb.add_peers([0,1,2])
    test2.beb.add_peers([0,1,2])

    test0.beb.broadcast("Hello world !")
    test2.beb.broadcast("Hello back")

