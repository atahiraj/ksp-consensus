import time

from basic_abstraction import MajorityVoting
from .base import FlightComputer

from utils import Logging

class CooperatingComputer(FlightComputer):
    def __init__(self, state, process_number):
        super().__init__(state)
        self.process_number = process_number
        self.majority_voting = MajorityVoting(process_number, self.acceptable_value, self.deliver_value)
        #Logging.set_debug(self.process_number, "VOT", True)
        #Logging.set_debug(self.process_number, "HCO", True)


    def add_peers(self, *peers):
        peers_number = [peer.process_number for peer in peers]
        self.majority_voting.add_peers(*peers_number)

    def start(self):
        self.majority_voting.start()
    
    def stop(self):
        self.majority_voting.stop()

    def decide_on_value(self, value):
        vote_result = self.majority_voting(value)
        return vote_result

    def decide_on_state(self, state):
        value = ("state", state)
        return self.decide_on_value(value)

    def decide_on_action(self, action):
        value = ("action", action)
        return self.decide_on_value(value)

    def acceptable_value(self, value):
        proposition_type, actual_value = value
        if proposition_type == "state":
            return self.acceptable_state(actual_value)
        elif proposition_type == "action":
            return self.acceptable_action(actual_value)

    def deliver_value(self, value):
        proposition_type, actual_value = value
        if proposition_type == "state":
            self.deliver_state(actual_value)
        elif proposition_type == "action":
            self.deliver_action(actual_value)
