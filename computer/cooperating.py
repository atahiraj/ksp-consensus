import time
from threading import Event, Lock

from basic_abstraction import Abstraction
from basic_abstraction import PerfectLink
from basic_abstraction import PerfectFailureDetector
from basic_abstraction import EagerReliableBroadcast
from basic_abstraction import HierarchicalConsensus
from computer.base import FlightComputer

from utils import Logging

class CooperatingComputer(FlightComputer, Abstraction):
    RECEIVE = 0
    PEER_FAILURE = 1
    CONSENSUS = 2
    def __init__(self, state, process_number):
        FlightComputer.__init__(self, state)
        Abstraction.__init__(self)
        self.event_handler_map = {
            self.RECEIVE: self.broadcast_receive,
            self.PEER_FAILURE: self.peer_failure,
            self.CONSENSUS: self.decided
        }
        self.process_number = process_number

        self.link = PerfectLink(process_number)
        self.send = self.link.register(self)
        self.pfd = PerfectFailureDetector(self.link)
        self.pfd.register(self, self.PEER_FAILURE)
        self.hco = HierarchicalConsensus(self.link, self.pfd, self, self.CONSENSUS)
        self.erb = EagerReliableBroadcast(self.link, self, self.RECEIVE)

        self.peers = {self.process_number}
        self.erb.add_peers(self.process_number)

        self.finished_consensus = Event()
        self.finished_consensus.set()
        self.consensus_result = None
        self.proposition = None

        #Logging.set_debug(self.process_number, "HCO", True)
        #Logging.set_debug(self.process_number, "BEB", True)
        #Logging.set_debug(self.process_number, "LINK", True)

    def add_peers(self, *peers):
        peers_number = [peer.process_number for peer in peers]
        self.peers.update(peers_number)
        self.hco.add_peers(*peers_number)
        self.erb.add_peers(*peers_number)


    def start(self):
        Abstraction.start(self)
        self.link.start()
        self.pfd.start()
        self.hco.start()
        self.erb.start()


    def stop(self):
        Abstraction.stop(self)
        self.link.stop()
        self.pfd.stop()
        self.hco.stop()
        self.erb.stop()


    def broadcast_receive(self, source_number, message):
        if self.debug:
            print(f"{self.process_number}: received state {message} from {source_number}")
    
        message_type, value = message

        if message_type == "state":
            self.finished_consensus.clear()
            self.proposition = message
            self.node_decide_on_state(value)
        elif message_type == "action":
            self.finished_consensus.clear()
            self.proposition = message
            self.node_decide_on_action(value)


    def peer_failure(self, process_number):
        exit()


    def decide_on_state(self, state):
        self.finished_consensus.wait() # Last consensus
        self.finished_consensus.clear()
        
        # Voting
        # message = ("new_vote", state)
        # self.erb.trigger_event(self.erb.BROADCAST, kwargs={"message": message})

        message = ("state", state)
        self.erb.trigger_event(self.erb.BROADCAST, kwargs={"message": message})

        self.finished_consensus.wait()
        
        return self.consensus_result


    def decide_on_action(self, action):
        self.finished_consensus.wait() # Last consensus
        self.finished_consensus.clear()
        
        message = ("action", action)
        self.erb.trigger_event(self.erb.BROADCAST, kwargs={"message": message})

        self.finished_consensus.wait()
        
        return self.consensus_result


    def node_decide_on_state(self, state):
        value = self.acceptable_state(state)
        self.hco.trigger_event(self.hco.PROPOSE, kwargs={"value": value})


    def decided(self, value):
        self.consensus_result = value
        if self.consensus_result:
            proposition_type, value = self.proposition
            if proposition_type == "state":
                self.deliver_state(value)
            elif proposition_type == "action":
                self.deliver_action(value)
        self.finished_consensus.set()


    def node_decide_on_action(self, action):
        value = self.acceptable_action(action)
        self.hco.trigger_event(self.hco.PROPOSE, kwargs={"value": value})
