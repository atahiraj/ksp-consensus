from threading import Event

from basic_abstraction.base import Abstraction
from basic_abstraction.link import PerfectLink
from basic_abstraction.failure_detector import PerfectFailureDetector
from basic_abstraction.broadcast import BestEffortBroadcast, EagerReliableBroadcast
from basic_abstraction.consensus import HierarchicalConsensus

from utils import Logging


class MajorityVoting(Abstraction):
    """This class implements a majority voting abstraction.

    Uses:
        - PerfectFailureDetector
        - EagerReliableBroadcast
        - HierchicalConsensus

    Since this is the top-level class, it instantiates all the classes it
    requires (and their requirements). It is also not meant to be used by an
    abstraction (but still can), but by, say, a flight computer object. This
    class is callable, and its entrypoint is its __call__ method.

    """

    def __init__(self, process_number, decide_callback, deliver_callback):
        super().__init__()
        self.process_number = process_number
        self.decide_callback = decide_callback
        self.deliver_callback = deliver_callback

        self.link = PerfectLink(self.process_number)

        self.pfd = PerfectFailureDetector(self.link)
        self.pfd.subscribe_abstraction(self, self.peer_failure)

        self.erb = EagerReliableBroadcast(self.link)
        self.broadcast = self.erb.register_abstraction(self)

        self.beb = BestEffortBroadcast(self.link)
        self.hco = HierarchicalConsensus(self.link, self.pfd, self.beb)
        self.hco.subscribe_abstraction(self, self.consensus_decided)

        self.peers = {self.process_number}
        self.detected = set()
        self.erb.add_peers(self.process_number)

        self.votes = {}
        self.voted = {peer: False for peer in self.peers}

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
        self.voted.update({peer: False for peer in self.peers})

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

    def __call__(self, value):
        self.logger.log_debug(f"New vote on: {value}")

        # Waiting last consensus
        if not self.finished_consensus.wait(self.TIMEOUT / 3):
            return False
        self.finished_consensus.clear()

        self.broadcast(self.new_vote, kwargs={"value": value})

        if not self.finished_consensus.wait(self.TIMEOUT / 3):
            return False
        return self.consensus_result

    def new_vote(self, source_number, value):
        self.logger.log_debug(f"Received new vote request {value} from {source_number}")
        self.finished_consensus.clear()
        self.proposition = value
        vote = self.decide_callback(value)
        self.broadcast(self.vote_receive, kwargs={"vote": vote})

    def vote_receive(self, source_number, vote):
        self.logger.log_debug(f"Received vote {vote} from {source_number}")
        if vote in self.votes:
            self.votes[vote] += 1
        else:
            self.votes[vote] = 1
        self.finished_vote(source_number)

    def finished_vote(self, process_number):
        self.voted[process_number] = True
        if all(self.voted.values()):
            self.logger.log_debug(f"Voting finished: {self.votes}")
            max_vote = max(self.votes, key=self.votes.get)
            self.votes.clear()
            self.voted = {peer: False for peer in self.peers - self.detected}
            self.hco.trigger_event(self.hco.propose, kwargs={"value": max_vote})

    def consensus_decided(self, value):
        self.logger.log_debug(f"Consensus decided on {value}")
        self.consensus_result = value
        if self.consensus_result:
            self.deliver_callback(self.proposition)
        self.finished_consensus.set()

    def peer_failure(self, process_number):
        self.logger.log_debug(f"Peer {process_number} crashed")
        self.detected.add(process_number)
        self.finished_vote(process_number)


if __name__ == "__main__":

    class Test:
        def __init__(self, process_number):
            self.process_number = process_number
            self.majority_voting = MajorityVotingConsensus(
                process_number, self.decide, self.deliver
            )
            Logging.set_debug(self.process_number, "VOT", True)
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

    class BuggedTest(Test):
        def __init__(self, process_number):
            super().__init__(process_number)

        def decide(self, value):
            return not super().decide(value)

    test0 = BuggedTest(0)
    test1 = Test(1)
    test2 = Test(2)

    test0.add_peers(test1, test2)
    test1.add_peers(test0, test2)
    test2.add_peers(test0, test1)

    test0.majority_voting.start()
    test1.majority_voting.start()
    test2.majority_voting.start()

    if test0.majority_voting("decrement"):
        raise Exception("A vote on 'decrement' should be False")

    test0.majority_voting.stop()

    if not test1.majority_voting("increment"):
        raise Exception("A vote on 'increment' should be True")

    """
    for i in range(20000):
        print(i)
        if test0.majority_voting("decrement"):
            raise Exception("A vote on 'decrement' should be False")
        if not test0.majority_voting("increment"):
            raise Exception("A vote on 'increment' should be True")
    """

    test0.majority_voting.stop()
    test1.majority_voting.stop()
    test2.majority_voting.stop()
