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
        self.peers = []

        self.detected_nodes = []
        self.reset()

    def reset(self):
        self.round = 0
        self.proposal = None
        self.proposer = -1
        self.delivered = {self.link.process_number: False}
        for peer in self.peers:
            self.delivered[peer] = False
        self.broadcasting = False
        self.can_propose = True


    def add_peers(self, peers_number_list):
        self.broadcast.add_peers(peers_number_list)
        self.failure_detector.add_peers(peers_number_list)
        self.peers.extend(peers_number_list)
        for peer in peers_number_list:
            self.delivered[peer] = False

    def failure_detection(self, process_number):
        self.detected_nodes.append(process_number)

    def start(self):
        self.failure_detector.start_heartbeat()

    def propose(self, value):
        if self.can_propose:
            self.can_propose = False

            if self.proposal == None:
                self.proposal = value

            while self.round <= len(self.peers):
                if self.round in self.detected_nodes or self.delivered[self.round]:
                    self.round += 1

                if self.round == self.link.process_number and self.proposal != None and not self.broadcasting:
                    self.broadcasting = True
                    self.broadcast.broadcast(("c", self.proposal))
                    self.decide_callback(self.proposal)
                    self.round += 1

                time.sleep(0.05)
            self.reset()

    def broadcast_receive(self, source_number, raw_message):
        mess_type, message = raw_message
        if mess_type == "c":
            if source_number < self.link.process_number and source_number > self.proposer:
                self.proposal = message
                self.proposer = source_number
            self.delivered[source_number] = True

if __name__ == "__main__":
    import threading

    class Test:
        def __init__(self, process_number):
            self.process_number = process_number
            self.link = FairLossLink(process_number)
            self.consensus = HierarchicalConsensus(self.link, self.decided)
            self.proposal = None

        def decided(self, value):
            print("{}: Decided on {}".format(self.process_number, value))

        def propose(self, value):
            while not self.consensus.can_propose:
                time.sleep(0.1)
            self.proposal = value
            thread = Test.TestThread(self)
            thread.start()

        def run(self):
            self.consensus.propose(self.proposal)

        class TestThread(threading.Thread):
            def __init__(self, test):
                super().__init__()
                self.test = test

            def run(self):
                self.test.run()


    test0 = Test(0)
    test1 = Test(1)
    test2 = Test(2)

    test0.consensus.add_peers([1, 2])
    test1.consensus.add_peers([0, 2])
    test2.consensus.add_peers([1, 0])

    test0.consensus.start()
    test1.consensus.start()
    test2.consensus.start()

    test0.propose("lol0")
    test1.propose("lil0")
    test2.propose("wesh0")

    test0.propose("lol1")
    test1.propose("lil1")
    test2.propose("wesh1")
