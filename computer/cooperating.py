from basic_abstraction.link import FairLossLink
from basic_abstraction.consensus import HierarchicalConsensus
from basic_abstraction.broadcast import EagerReliableBroadcast
from computer.base import FlightComputer
import time

class CooperatingComputer(FlightComputer):
    def __init__(self, state, process_number):
        super().__init__(state)
        self.process_number = process_number
        self.peers = set()

        self.link = FairLossLink(process_number)
        self.consensus = HierarchicalConsensus(self.link)
        self.broadcast = EagerReliableBroadcast(self.link, self.broadcast_receive)


    def add_peers(self, *peers):
        peers_number = [peer.process_number for peer in peers]
        self.peers.update(peers_number)
        self.consensus.add_peers(*peers_number)
        self.broadcast.add_peers(*peers_number)

    def start(self):
        self.consensus.start()

    def broadcast_receive(self, source, raw_message):
        message_type, message = raw_message
        if source != self.process_number:
            if message_type == "state":
                self.node_decide_on_state(message)
            elif message_type == "action":
                self.node_decide_on_action(message)

    def decide_on_state(self, state):
        self.broadcast.broadcast(("state", state))
        # TODO organise a vote
        return self.node_decide_on_state(state)


    def decide_on_action(self, action):
        self.broadcast.broadcast(("action", action))
        # TODO organise a vote
        return self.node_decide_on_action(action)

    def node_decide_on_state(self, state):
        decided = self.consensus.propose(self.acceptable_state(state))
        print(f"{self.process_number}: State {decided}")
        if decided:
            self.deliver_state(state)

        return decided

    def node_decide_on_action(self, action):
        decided = self.consensus.propose(self.acceptable_action(action))
        print(f"{self.process_number}: Action {decided}")
        if decided:
            self.deliver_action(action)

        return decided
