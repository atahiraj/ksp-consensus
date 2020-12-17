from basic_abstraction.broadcast import BestEffortBroadcast
from basic_abstraction.failure_detectors import PerfectFailureDetector
from basic_abstraction.link import FairLossLink
import time

class Consensus:
    def __init__(self, link, decide_callback):
        self.link = link
        self.decide_callback = decide_callback

    def add_peers(peers_number_list):
        pass

    def propose(self, value):
        pass

class HierarchicalConsensus(Consensus):
    def __init__(self, link, decide_callback):
        super().__init__(link, decide_callback)
        self.broadcast = BestEffortBroadcast(link, self.broadcast_receive)
        self.failure_detector = PerfectFailureDetector(self.link, self.failure_detection)

        self.detected_nodes = []
        self.round = 0
        self.proposal = None
        self.proposer = -1
        self.delivered = [False]
        self.broadcasting = False

    def add_peers(self, peers_number_list):
        self.broadcast.add_peers(peers_number_list)
        self.failure_detector.add_peers(peers_number_list)
        self.delivered.extend([False] * len(peers_number_list))

    def failure_detection(self, process_number):
        self.detected_nodes.append(process_number)

    def propose(self, value):
        self.failure_detector.start_heartbeat()

        time.sleep(0.2)

        if self.proposal == None:
            self.proposal = value

        decided = False
        while not decided:
            if self.round in self.detected_nodes or self.delivered[self.round]:
                self.round += 1

            if self.round == self.link.process_number and self.proposal != None and not self.broadcasting:
                self.broadcasting = True
                self.broadcast.broadcast(self.proposal)
                self.decide_callback(self.proposal)
                decided = True

            time.sleep(0.01)

    def broadcast_receive(self, source_number, message):
        if source_number < self.link.process_number and source_number > self.proposer:
            self.proposal = message
            self.proposer = source_number
        self.delivered[source_number] = True

if __name__ == "__main__":
    class Test:
        def __init__(self, process_number):
            self.process_number = process_number
            self.link = FairLossLink(process_number)
            self.consensus = HierarchicalConsensus(self.link, self.decided)

        def decided(self, value):
            print("{}: Decided on {}".format(self.process_number, value))

    test0 = Test(0)
    test1 = Test(1)

    test0.consensus.add_peers([1])
    test1.consensus.add_peers([0])

    test0.link.alive = False
    test0.consensus.propose("lol".encode("utf-8"))
    test1.consensus.propose("lil".encode("utf-8"))
