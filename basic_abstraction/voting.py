from threading import Event

from basic_abstraction import Abstraction
from basic_abstraction import PerfectLink
from basic_abstraction import PerfectFailureDetector
from basic_abstraction import BestEffortBroadcast, EagerReliableBroadcast
from basic_abstraction import HierarchicalConsensus
from computer.base import FlightComputer

from utils import Logging

class MajorityVotingConsensus(Abstraction):
    def __init__(self, process_number, decide_callback, deliver_callback):
        super().__init__()
        self.process_number = process_number
        self.decide_callback = decide_callback
        self.deliver_callback = deliver_callback

        self.link = PerfectLink(self.process_number)
        #self.send = self.link.register(self)
        self.pfd = PerfectFailureDetector(self.link)
        self.pfd.register(self, self.peer_failure)
        self.erb = EagerReliableBroadcast(self.link)
        self.broadcast = self.erb.register(self)

        self.beb = BestEffortBroadcast(self.link)
        self.hco = HierarchicalConsensus(self.link, self.pfd, self.beb, self, self.consensus_decided)

        self.peers = {self.process_number}
        self.erb.add_peers(self.process_number)

        self.finished_consensus = Event()
        self.finished_consensus.set()
        self.consensus_result = None
        self.proposition = None

        self.logger = Logging(self.process_number, "VOT")

    def add_peers(self, *peers):
        self.peers.update(peers)
        self.pfd.add_peers(*peers)
        self.beb.add_peers(*peers)
        self.hco.add_peers(*peers)
        self.erb.add_peers(*peers)

    def start(self):
        super().start()
        self.link.start()
        self.pfd.start()
        self.erb.start()
        self.beb.start()
        self.hco.start()
        

    def stop(self):
        super().stop()
        self.link.stop()
        self.pfd.stop()
        self.erb.stop()
        self.beb.stop()
        self.hco.stop()

    def decide_on_value(self, value):
        self.logger.log_debug(f"New decide: {value}")
        self.finished_consensus.wait() # Last consensus
        self.finished_consensus.clear()
        
        self.broadcast(self.consensus_receive, kwargs={"value": value})

        self.finished_consensus.wait()
        
        return self.consensus_result

    def consensus_receive(self, source_number, value):
        self.logger.log_debug(f"Received {value} from {source_number}")
        self.finished_consensus.clear()
        self.proposition = value
        proposal = self.decide_callback(value)
        self.hco.trigger_event(self.hco.propose, kwargs={"value": proposal})

    def consensus_decided(self, value):
        self.logger.log_debug(f"Consensus decided on {value}")
        self.consensus_result = value
        if self.consensus_result:
            self.deliver_callback(self.proposition)
        self.finished_consensus.set()

    def peer_failure(self, process_number):
        pass


if __name__ == "__main__":
    class Test:
        def __init__(self, process_number):
            self.process_number = process_number
            self.majority_voting = MajorityVotingConsensus(process_number, self.decide, self.deliver)
            Logging.set_debug(self.process_number, "HCO", True)
            self.count = 0

        def add_peers(self, *peers):
            peers_number = [peer.process_number for peer in peers]
            self.majority_voting.add_peers(*peers_number)

        def deliver(self, value):
            if value == "increment":
                self.count += 1
        
        def decide(self, value):
            if value == "increment":
                return True
            else:
                return False
        
    test0 = Test(0)
    test1 = Test(1)
    test2 = Test(2)

    test0.add_peers(test1, test2)
    test1.add_peers(test0, test2)
    test2.add_peers(test0, test1)

    test0.majority_voting.start()
    test1.majority_voting.start()
    test2.majority_voting.start()

    decide = test0.majority_voting.decide_on_value("decrement")
    decide = test0.majority_voting.decide_on_value("increment")


    test0.majority_voting.stop()
    test1.majority_voting.stop()
    test2.majority_voting.stop()