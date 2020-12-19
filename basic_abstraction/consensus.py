import time

from broadcast import BestEffortBroadcast
from failure_detectors import PerfectFailureDetector
from link import PerfectLink
from base import Abstraction

class Consensus(Abstraction):
    def __init__(self, link, abstraction_callback, operation_id_callback):
        super().__init__()
        self.process_number = link.process_number
        self.abstraction_callback = abstraction_callback
        self.operation_id_callback = operation_id_callback
        self.send = link.register(self)

    def add_peers(peers_number_list):
        pass

    def propose(self, value):
        return value

class HierarchicalConsensus(Consensus):
    PROPOSE = 0
    RECEIVE = 1
    PEER_FAILURE = 2

    def __init__(self, link, abstraction_callback, operation_id_callback):
        super().__init__(link, abstraction_callback, operation_id_callback)
        self.event_handler_map = {
            self.PROPOSE: self.propose,
            self.RECEIVE: self.receive,
            self.PEER_FAILURE: self.peer_failure
        }

        self.beb = BestEffortBroadcast(link, self, self.RECEIVE)
        self.pfd = PerfectFailureDetector(link, self, self.PEER_FAILURE)
        self.peers = {self.process_number}
        self.beb.add_peers(self.process_number)
        self.detected = set()

        self.reset()

    def reset(self):
        self.round = 0
        self.proposal = None
        self.proposer = -1
        self.delivered = {peer: False for peer in self.peers}
        self.broadcast = False

    def start(self):
        super().start()
        self.beb.start()
        self.pfd.start()

    def stop(self):
        super().stop()
        self.beb.stop()
        self.pfd.stop()

    def add_peers(self, *peers):
        self.beb.add_peers(*peers)
        self.pfd.add_peers(*peers)
        self.peers.update(peers)
        self.delivered.update({peer: False for peer in peers})

    def peer_failure(self, process_number):
        if self.debug:
            print(f"HCO: {self.process_number}: peer {process_number} crashed")
        self.detected.add(process_number)
        self.round_update()

    def propose(self, value):
        if self.debug:
            print(f"HCO: {self.process_number}: new proposal {value}")
        if self.proposal is None:
            self.proposal = value
        self.round_update()

    def state_update(self):
        if self.round == self.process_number and self.proposal and not self.broadcast:
            self.broadcast = True
            args = (self.proposal,)
            self.beb.trigger_event(self.beb.BROADCAST, args=args)
            self.abstraction_callback.trigger_event(self.operation_id_callback, args=args)

    def round_update(self):
        while self.round < len(self.peers) and (self.round in self.detected or self.delivered[self.round]):
            self.round += 1
        if self.round == len(self.peers):
            self.reset()
        else:
            self.state_update()

    def receive(self, source_number, message):
        if self.debug:
            print(f"HCO: {self.process_number}: received {message} from {source_number}")
        if source_number < self.process_number and source_number > self.proposer:
            self.proposal = message
            self.proposer = source_number
        self.delivered[source_number] = True
        self.round_update()


if __name__ == "__main__":
    class Test(Abstraction):
        CONSENSUS = 0
        def __init__(self, process_number):
            super().__init__()
            self.event_handler_map = {
                self.CONSENSUS: self.consensus
            }
            self.process_number = process_number
            self.link = PerfectLink(process_number)
            self.hco = HierarchicalConsensus(self.link, self, self.CONSENSUS)
            #self.hco.debug = True
            #self.hco.pfd.debug = True

        def start(self):
            super().start()
            self.link.start()
            self.hco.start()

        def stop(self):
            super().stop()
            self.link.stop()
            self.hco.stop()

        def consensus(self, value):
            print(f"{self.process_number}: decided on {value}")


    test0 = Test(0)
    test1 = Test(1)
    test2 = Test(2)
    test0.hco.add_peers(1, 2)
    test1.hco.add_peers(0, 2)
    test2.hco.add_peers(0, 1)
    test0.start()
    test1.start()
    test2.start()
    test0.hco.propose("lol0")
    test1.hco.propose("lil0")
    test2.hco.propose("wesh0")

    time.sleep(0.5)

    test0.stop()
    test0.hco.propose("lol1")
    test1.hco.propose("lil1")
    test2.hco.propose("wesh1")

    time.sleep(0.5)

    test0.hco.propose("lol2")
    test1.hco.propose("lil2")
    test2.hco.propose("wesh2")

    time.sleep(0.5)
    test1.stop()
    test0.hco.propose("lol3")
    test1.hco.propose("lil3")
    test2.hco.propose("wesh3")

    time.sleep(0.5)
    test2.stop()
    