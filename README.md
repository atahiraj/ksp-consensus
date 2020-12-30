# Fault-Tolerant Flight Computers

## About

This project implements distributed consensus between redundant flight
computers responsible for controlling a rocket. The computers agree on sensor
state and control actions while accounting for faulty or slow participants,
using abstractions for links, broadcast, failure detection, leader election,
hierarchical consensus, and majority voting.

The target mission launches a Kerbal Space Program vehicle toward an
approximately 100 km circular orbit. The implementation can also replay the
included state and action traces without Kerbal Space Program. It was created
for the University of Liège INFO8002-1 course; the original pinned
[project description](https://github.com/glouppe/info8002-large-scale-data-systems/tree/2ea554fb203de4797629790698dec058b7d9c8db/project)
provides the problem statement and design context.

## Setup

Create and activate a Conda environment from the pinned dependencies:

```sh
conda env create -n ksp-consensus -f environment.yml
conda activate ksp-consensus
```

The environment uses Python 3.9, kRPC 0.4.8, and protobuf 3.14.0.

Running against the game additionally requires Kerbal Space Program and the
[kRPC plugin](https://krpc.github.io/krpc/getting-started.html). Load
`craft/INFO8002.craft`, place the vehicle on the launchpad, start the kRPC
server, and keep the game focused on the active vessel.

## Usage

Run the included trace simulation without Kerbal Space Program:

```sh
python without-ksp.py --flight-computers 10 --correct-fraction 1.0
```

Run the same consensus-driven controller against the active vessel in Kerbal
Space Program:

```sh
python with-ksp.py --flight-computers 10 --correct-fraction 1.0
```

Both entry points accept:

- `--flight-computers`: number of redundant flight computers; the default is
  `3`.
- `--correct-fraction`: fraction of computers that behave correctly; the
  default is `1.0`.

The offline mode validates decisions against `data/states.pickle` and
`data/actions.pickle`. The game mode reads live telemetry and sends agreed
control actions through kRPC.

## License

This project's original code is licensed under the MIT License. See
[LICENSE](LICENSE) for the complete terms. Portions derived from the linked
course project remain subject to its
[BSD 3-Clause License](https://github.com/glouppe/info8002-large-scale-data-systems/blob/2ea554fb203de4797629790698dec058b7d9c8db/project/LICENSE).
