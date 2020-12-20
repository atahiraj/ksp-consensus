import time
from threading import Event, Lock

from basic_abstraction import Abstraction
from basic_abstraction import PerfectLink
from basic_abstraction import PerfectFailureDetector
from basic_abstraction import BestEffortBroadcast, EagerReliableBroadcast
from basic_abstraction import HierarchicalConsensus
from computer.base import FlightComputer

from utils import Logging

class CooperatingComputer(FlightComputer, Abstraction):
    def __init__(self, state, process_number):
        FlightComputer.__init__(self, state)
        Abstraction.__init__(self)
        self.process_number = process_number

        self.link = PerfectLink(self.process_number)
        #self.send = self.link.register(self)
        self.pfd = PerfectFailureDetector(self.link)
        self.pfd.register(self, self.peer_failure)
        self.erb = EagerReliableBroadcast(self.link)
        self.broadcast = self.erb.register(self)

        self.beb = BestEffortBroadcast(self.link)
        self.hco = HierarchicalConsensus(self.link, self.pfd, self.beb, self, self.decided)

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
        self.erb.start()
        self.beb.start()
        self.hco.start()
        

    def stop(self):
        Abstraction.stop(self)
        self.link.stop()
        self.pfd.stop()
        self.erb.stop()
        self.beb.stop()
        self.hco.stop()


    def broadcast_receive(self, source_number, message_type, value):
        if self.debug:
            print(f"{self.process_number}: received state {(message_type, value)} from {source_number}")
    
        if message_type == "state":
            self.finished_consensus.clear()
            self.proposition = (message_type, value)
            self.node_decide_on_state(value)
        elif message_type == "action":
            self.finished_consensus.clear()
            self.proposition = (message_type, value)
            self.node_decide_on_action(value)


    def peer_failure(self, process_number):
        exit()


    def decide_on_state(self, state):
        self.finished_consensus.wait() # Last consensus
        self.finished_consensus.clear()
        
        self.broadcast(self.broadcast_receive, args=("state", state))

        self.finished_consensus.wait()
        
        return self.consensus_result


    def decide_on_action(self, action):
        self.finished_consensus.wait() # Last consensus
        self.finished_consensus.clear()
        
        self.broadcast(self.broadcast_receive, args=("action", action))

        self.finished_consensus.wait()
        
        return self.consensus_result


    def node_decide_on_state(self, state):
        value = self.acceptable_state(state)
        self.hco.trigger_event(self.hco.propose, kwargs={"value": value})


    def node_decide_on_action(self, action):
        value = self.acceptable_action(action)
        self.hco.trigger_event(self.hco.propose, kwargs={"value": value})


    def decided(self, value):
        self.consensus_result = value
        if self.consensus_result:
            proposition_type, value = self.proposition
            if proposition_type == "state":
                self.deliver_state(value)
            elif proposition_type == "action":
                self.deliver_action(value)
        self.finished_consensus.set()
