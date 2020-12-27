import argparse
import math
import pickle
import time
import random
import traceback

from computer import CooperatingComputer, allocate_faulty_flight_computer

# Load the pickle files
actions = pickle.load(open("data/actions.pickle", "rb"))
states = pickle.load(open("data/states.pickle", "rb"))
timestep = 0

# Argument parsing
parser = argparse.ArgumentParser()
parser.add_argument("--correct-fraction", type=float, default=1.0, help="Fraction of correct flight computers (default 1.0).")
parser.add_argument("--flight-computers", type=int, default=3, help="Number of flight computers (default: 3).")
arguments, _ = parser.parse_known_args()


def readout_state():
    return states[timestep]


def execute_action(action):
    print(action)
    print(actions[timestep])
    keys = set(action.keys())
    correct_keys = set(actions[timestep])
    assert(not(keys < correct_keys or keys > correct_keys)) 
    for k in action.keys():
        assert(action[k] == actions[timestep][k])


def allocate_flight_computers(arguments):
    n_fc = arguments.flight_computers
    n_correct_fc = math.ceil(arguments.correct_fraction * n_fc)
    n_incorrect_fc = n_fc - n_correct_fc
    state = readout_state()

    flight_computers = {}
    keys = list(range(n_fc))
    random.shuffle(keys)

    for i, key in enumerate(keys):
        if i < n_correct_fc:
            flight_computers[key] = CooperatingComputer(state, key)
        else:
            flight_computers[key] = allocate_faulty_flight_computer(state, key)

    # Add the peers for the consensus protocol
    for fc in flight_computers.values():
        for peer in flight_computers.values():
            if fc != peer:
                fc.add_peers(peer)
    for fc in flight_computers.values():
        fc.start()

    return flight_computers

# Connect with Kerbal Space Program
flight_computers = allocate_flight_computers(arguments)


def select_leader():
    counts = {fc.process_number: 0 for fc in flight_computers.values()}
    for fc in flight_computers.values():
        local_leader = fc.get_leader()
        if local_leader is not None:
            counts[local_leader] += 1
    actual_leader = max(counts, key=counts.get)
    return flight_computers[actual_leader]


def next_action(state):
    leader = select_leader()
    state_decided = leader.decide_on_state(state)
    if not state_decided:
        return None
    action = leader.sample_next_action()
    action_decided = leader.decide_on_action(action)
    if action_decided:
        return action

    return None

complete = False
leader = select_leader()
try:
    while not complete:
        print(timestep)
        timestep += 1
        state = readout_state()
        state_decided = leader.decide_on_state(state)
        if not state_decided:
            leader.stop()
            leader = select_leader()
            timestep -= 1
            continue

        action = leader.sample_next_action()
        if action is None:
            complete = True
            continue
        elif action is False:
            leader = select_leader()
            continue

        action_decided = leader.decide_on_action(action)
        if action_decided:
            execute_action(action)
        else:
            leader.stop()
            leader = select_leader()
            timestep -= 1

except Exception as e:
    print(e)
    traceback.print_exc()

if complete:
    print("Success!")
else:
    print("Fail!")

for fc in flight_computers.values():
    fc.stop()
