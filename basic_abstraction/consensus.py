import time

from basic_abstraction.broadcast import BestEffortBroadcast
from basic_abstraction.failure_detectors import PerfectFailureDetector
from basic_abstraction.link import PerfectLink
from basic_abstraction.base import Abstraction

from utils import Logging

class Consensus(Abstraction):
    def __init__(self, link, abstraction_callback, operation_id_callback):
        super().__init__()
        self.process_number = link.process_number
        self.abstraction_callback = abstraction_callback
        self.operation_id_callback = operation_id_callback
        self.send = link.register(self)
        self.logger = Logging(self.process_number, "HCO")

    def add_peers(peers_number_list):
        pass

    def propose(self, value):
        return value

class HierarchicalConsensus(Consensus):
    PROPOSE = 0
    RECEIVE = 1
    PEER_FAILURE = 2
    FINISHED = 3

    def __init__(self, link, pfd, abstraction_callback, operation_id_callback):
        super().__init__(link, abstraction_callback, operation_id_callback)
        self.event_handler_map = {
            self.PROPOSE: self.propose,
            self.RECEIVE: self.receive,
            self.PEER_FAILURE: self.peer_failure,
            self.FINISHED: self.finished
        }
        self.process_number = link.process_number

        self.link = link
        self.beb = BestEffortBroadcast(self.link, self, self.RECEIVE)
        self.beb_finished = BestEffortBroadcast(self.link, self, self.FINISHED)
        self.pfd = pfd
        self.pfd.register(self, self.PEER_FAILURE)
        
        self.peers = {self.process_number}
        self.beb.add_peers(self.process_number)
        self.beb_finished.add_peers(self.process_number)
        self.detected = set()

        self.reset()
        self.finished_peers = {peer: False for peer in self.peers}

    def reset(self):
        self.round = 0
        self.proposal = None
        self.proposer = -1
        self.delivered = {peer: False for peer in self.peers}
        self.broadcast = False

    def start(self):
        super().start()
        self.beb.start()
        self.beb_finished.start()

    def stop(self):
        super().stop()
        self.beb.stop()
        self.beb_finished.stop()

    def add_peers(self, *peers):
        self.beb.add_peers(*peers)
        self.beb_finished.add_peers(*peers)
        self.pfd.add_peers(*peers)
        self.peers.update(peers)
        self.delivered.update({peer: False for peer in peers})
        self.finished_peers.update({peer: False for peer in peers})

    def peer_failure(self, process_number):
        self.logger.log_debug(f"Peer {process_number} crashed")
        self.detected.add(process_number)
        self.round_update()
        self.finished(process_number, None)

    def propose(self, value):
        self.logger.log_debug(f"New proposal {value}")
        if self.proposal is None:
            self.proposal = value
        self.round_update()

    def round_update(self):
        while self.round < len(self.peers) and (self.round in self.detected or self.delivered[self.round]):
            self.round += 1
        if self.round == len(self.peers):
            self.reset()
            self.beb_finished.trigger_event(BestEffortBroadcast.BROADCAST, kwargs={"message": None})
        elif self.round == self.process_number and self.proposal and not self.broadcast:
            self.broadcast = True
            self.decided = self.proposal
            self.beb.trigger_event(self.beb.BROADCAST, kwargs={"message": self.decided})

    def receive(self, source_number, message):
        self.logger.log_debug(f"Process {source_number} has decided on {message}")
        if source_number < self.process_number and source_number > self.proposer:
            self.proposal = message
            self.proposer = source_number
        self.delivered[source_number] = True
        self.round_update()

    def finished(self, source_number, _):
        self.finished_peers[source_number] = True
        if all(self.finished_peers.values()):
            self.logger.log_debug(f"Consensus finished")
            self.abstraction_callback.trigger_event(self.operation_id_callback, kwargs={"value": self.decided})
            self.finished_peers = {peer: False for peer in self.peers - self.detected}

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
            self.pfd = PerfectFailureDetector(self.link)
            self.hco = HierarchicalConsensus(self.link, self.pfd, self, self.CONSENSUS)
            #Logging.set_debug(self.process_number, "HCO", True)

        def start(self):
            super().start()
            self.link.start()
            self.pfd.start()
            self.hco.start()

        def stop(self):
            super().stop()
            self.link.stop()
            self.pfd.stop()
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
    test0.hco.trigger_event(test0.hco.PROPOSE, kwargs={"value": "lol0"})
    test1.hco.trigger_event(test0.hco.PROPOSE, kwargs={"value":"lil0"})
    test2.hco.trigger_event(test0.hco.PROPOSE, kwargs={"value": "wsh0"})

    time.sleep(0.5)

    test0.stop()
    test0.hco.trigger_event(test0.hco.PROPOSE, kwargs={"value": "lol1"})
    test1.hco.trigger_event(test0.hco.PROPOSE, kwargs={"value":"lil1"})
    test2.hco.trigger_event(test0.hco.PROPOSE, kwargs={"value": "wsh1"})

    time.sleep(0.5)

    test0.hco.trigger_event(test0.hco.PROPOSE, kwargs={"value": "lol2"})
    test1.hco.trigger_event(test0.hco.PROPOSE, kwargs={"value":"lil2"})
    test2.hco.trigger_event(test0.hco.PROPOSE, kwargs={"value": "wsh2"})

    time.sleep(0.5)
    test1.stop()
    test0.hco.trigger_event(test0.hco.PROPOSE, kwargs={"value": "lol3"})
    test1.hco.trigger_event(test0.hco.PROPOSE, kwargs={"value":"lil3"})
    test2.hco.trigger_event(test0.hco.PROPOSE, kwargs={"value": "wsh3"})

    time.sleep(0.5)
    test2.stop()
