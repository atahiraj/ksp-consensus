from basic_abstraction.base import Subscriptable

from utils import Logging


class LeaderElection(Subscriptable):
    """ This class implement a hierarchical leader election abstraction.

    Uses:
        - PerfectFailureDetector
        - HierarchicalConsensus

    A peer's rank is greater than another's iff its process number is strictly
    lower than the other's. All sbscribed methods are called whenever a new
    leader is elected.

    """
    def __init__(self, pfd, hco):
        super().__init__()
        self.process_number = hco.process_number
        
        self.pfd = pfd
        self.pfd.subscribe_abstraction(self, self.peer_failure)
        self.hco = hco
        self.hco.subscribe_abstraction(self, self.decided)

        self.peers = {self.process_number}
        self.detected = set()
        self.leader = None
        
        self.in_election = False

        self.logger = Logging(self.process_number, "LEL")

    def start(self):
        super().start()
        self.election()

    def add_peers(self, *peers):
        self.peers.update(peers)
        self.pfd.add_peers(*peers)
        self.hco.add_peers(*peers)

    def peer_failure(self, process_number):
        self.logger.log_debug(f"Peer {process_number} crashed")
        self.detected.add(process_number)
        self.election()

    def election(self):
        if not self.in_election:
            self.logger.log_debug(f"New election")
            self.in_election = True
            self.leader = None
            leader = min(self.peers - self.detected)
            self.hco.trigger_event(self.hco.propose, kwargs={"value": leader})
    
    def decided(self, value):
        self.in_election = False
        if value in (self.peers - self.detected):
            self.logger.log_debug(f"New leader {value}")
            self.leader = value
            self.call_callbacks(self.leader)
        else:
            self.election()


if __name__ == "__main__":
    import time

    from basic_abstraction.base import Abstraction
    from basic_abstraction.link import PerfectLink
    from basic_abstraction.failure_detector import PerfectFailureDetector
    from basic_abstraction.consensus import HierarchicalConsensus
    from basic_abstraction.broadcast import BestEffortBroadcast
    
    class Test(Abstraction):
        def __init__(self, process_number):
            super().__init__()
            self.process_number = process_number
            self.link = PerfectLink(process_number)
            self.pfd = PerfectFailureDetector(self.link)
            self.beb = BestEffortBroadcast(self.link)
            self.hco = HierarchicalConsensus(self.link, self.pfd, self.beb)
            self.lel = LeaderElection(self.pfd, self.hco)
            self.lel.subscribe_abstraction(self, self.new_leader)

        def start(self):
            super().start()
            self.link.start()
            self.pfd.start()
            self.beb.start()
            self.hco.start()
            self.lel.start()

        def stop(self):
            super().stop()
            self.link.stop()
            self.pfd.stop()
            self.beb.stop()
            self.hco.stop()
            self.lel.stop()

        def add_peers(self, *peers):
            self.pfd.add_peers(*peers)
            self.beb.add_peers(*peers)
            self.hco.add_peers(*peers)
            self.lel.add_peers(*peers)
        
        def new_leader(self, leader):
            print(f"{self.process_number}: new leader {leader}")
    
    test0 = Test(0)
    test1 = Test(1)
    test2 = Test(2)

    test0.add_peers(1, 2)
    test1.add_peers(0 ,2)
    test2.add_peers(0, 1)

    test0.start()
    test1.start()
    test2.start()

    time.sleep(0.05)

    test0.stop()

    time.sleep(0.25)

    test1.stop()

    time.sleep(0.25)

    test0.stop()
    test1.stop()
    test2.stop()
