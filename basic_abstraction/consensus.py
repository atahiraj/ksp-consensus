import time

from basic_abstraction.broadcast import BestEffortBroadcast
from basic_abstraction.failure_detector import PerfectFailureDetector
from basic_abstraction.link import PerfectLink
from basic_abstraction.base import Subscriptable

from utils import Logging


class Consensus(Subscriptable):
    """This abstract  class implements a generic consensus algorithm."""

    def __init__(
        self,
        link,
    ):
        super().__init__()
        self.link = link
        self.process_number = link.process_number

    def add_peers(self, *peers):
        pass

    def propose(self, value):
        pass


class HierarchicalConsensus(Consensus):
    """This class implements the hierchical consensus algorithm.

    Uses:
        - PerfectFailureDetector
        - BestEffortBroadcast

    A peer's rank is greater than another's iff its process number is strictly
    lower than the other's. The propose method is the entry point to the
    consensus algorithm. When the consensus is finished, the subscribed
    callbacks are called.

    """

    def __init__(self, link, pfd, beb):
        super().__init__(link)
        self.beb = beb
        self.broadcast = self.beb.register_abstraction(self)

        self.pfd = pfd
        self.pfd.subscribe_abstraction(self, self.peer_failure)

        self.peers = {self.process_number}
        self.beb.add_peers(self.process_number)
        self.detected = set()

        self.reset()
        self.finished_peers = {peer: False for peer in self.peers}

        self.logger = Logging(self.process_number, "HCO")

    def reset(self):
        self.round = 0
        self.proposal = None
        self.proposer = -1
        self.delivered = {peer: False for peer in self.peers}
        self.broadcasting = False

    def add_peers(self, *peers):
        self.beb.add_peers(*peers)
        self.pfd.add_peers(*peers)
        self.peers.update(peers)
        self.delivered.update({peer: False for peer in peers})
        self.finished_peers.update({peer: False for peer in peers})

    def peer_failure(self, process_number):
        self.logger.log_debug(f"Peer {process_number} crashed")
        self.detected.add(process_number)
        self.round_update()
        self.finished(process_number)

    def propose(self, value):
        self.logger.log_debug(f"New proposal {value}")
        if self.proposal is None:
            self.proposal = value
        self.round_update()

    def round_update(self):
        while self.round < len(self.peers) and (
            self.round in self.detected or self.delivered[self.round]
        ):
            self.round += 1
        if self.round == len(self.peers):
            self.reset()
            self.broadcast(self.finished)
        elif (
            self.round == self.process_number
            and self.proposal is not None
            and not self.broadcasting
        ):
            self.broadcasting = True
            self.decided = self.proposal
            self.broadcast(self.receive, args=(self.decided,))

    def receive(self, source_number, value):
        if source_number in self.detected:
            return
        self.logger.log_debug(f"Process {source_number} has decided on {value}")
        if source_number < self.process_number and source_number > self.proposer:
            self.proposal = value
            self.proposer = source_number
        self.delivered[source_number] = True
        self.round_update()

    def finished(self, source_number):
        self.finished_peers[source_number] = True
        if all(self.finished_peers.values()):
            self.logger.log_debug(f"Consensus finished")
            self.call_callbacks(self.decided)
            self.finished_peers = {peer: False for peer in self.peers - self.detected}


if __name__ == "__main__":
    from basic_abstraction.base import Abstraction

    class Test(Abstraction):
        def __init__(self, process_number):
            super().__init__()
            self.process_number = process_number
            self.link = PerfectLink(process_number)
            self.pfd = PerfectFailureDetector(self.link)
            self.beb = BestEffortBroadcast(self.link)
            self.hco = HierarchicalConsensus(self.link, self.pfd, self.beb)
            self.hco.subscribe_abstraction(self, self.consensus)
            # Logging.set_debug(self.process_number, "HCO", True)

        def start(self):
            super().start()
            self.link.start()
            self.pfd.start()
            self.beb.start()
            self.hco.start()

        def stop(self):
            super().stop()
            self.link.stop()
            self.pfd.stop()
            self.beb.stop()
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

    test0.hco.trigger_event(test0.hco.propose, kwargs={"value": "lol0"})
    test1.hco.trigger_event(test0.hco.propose, kwargs={"value": "lil0"})
    test2.hco.trigger_event(test0.hco.propose, kwargs={"value": "wsh0"})

    time.sleep(0.5)

    test0.stop()
    test0.hco.trigger_event(test0.hco.propose, kwargs={"value": "lol1"})
    test1.hco.trigger_event(test0.hco.propose, kwargs={"value": "lil1"})
    test2.hco.trigger_event(test0.hco.propose, kwargs={"value": "wsh1"})

    time.sleep(0.5)

    test0.hco.trigger_event(test0.hco.propose, kwargs={"value": "lol2"})
    test1.hco.trigger_event(test0.hco.propose, kwargs={"value": "lil2"})
    test2.hco.trigger_event(test0.hco.propose, kwargs={"value": "wsh2"})

    time.sleep(0.5)
    test1.stop()
    test0.hco.trigger_event(test0.hco.propose, kwargs={"value": "lol3"})
    test1.hco.trigger_event(test0.hco.propose, kwargs={"value": "lil3"})
    test2.hco.trigger_event(test0.hco.propose, kwargs={"value": "wsh3"})

    time.sleep(0.5)
    test2.stop()
