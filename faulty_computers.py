import numpy as np
import time

from computer import FlightComputer

class FullThrottleFlightComputer(FlightComputer):

    def __init__(self, state):
        super(FullThrottleFlightComputer, self).__init__(state)

    def sample_next_action(self):
        action = super(FullThrottleFlightComputer, self).sample_next_action()
        action["throttle"] = 1.0

        return action


class RandomThrottleFlightComputer(FlightComputer):

    def __init__(self, state):
        super(RandomThrottleFlightComputer, self).__init__(state)

    def sample_next_action(self):
        action = super(RandomThrottleFlightComputer, self).sample_next_action()
        action["throttle"] = np.random.uniform()

        return action


class SlowFlightComputer(FlightComputer):

    def __init__(self, state):
        super(SlowFlightComputer, self).__init__(state)

    def sample_next_action(self):
        action = super(SlowFlightComputer, self).sample_next_action()
        time.sleep(np.random.uniform()) # Seconds

        return action


class CrashingFlightComputer(FlightComputer):

    def __init__(self, state):
        super(CrashingFlightComputer, self).__init__(state)

    def sample_next_action(self):
        action = super(CrashingFlightComputer, self).sample_next_action()
        # 1% probability of a crash
        if np.random.uniform() <= 0.01:
            raise Exception("Flight computer crashed")

        return action



def allocate_faulty_flight_computer(state):
    computers = [
        FullThrottleFlightComputer,
        RandomThrottleFlightComputer,
        SlowFlightComputer,
        CrashingFlightComputer,
    ]

    return computers[np.random.randint(0, len(computers))](state)
