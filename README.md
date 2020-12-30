# Flight Computers

> Project for the INFO8002-1 course of the ULiege University.

This project is described [here](https://github.com/glouppe/info8002-large-scale-data-systems/tree/master/project).

## Setup

The only non-standart package this implementation uses is `krpc 0.4.8`. Therefore, the `environment.yml` file contains that package only. Run 
```sh
conda env create -f environment.yml -n <env_name>
```
to create a conda envrionment with `krpc 0.4.8`. Activate it with
```sh
conda activate <env_name>
```

## Usage 

The entrypoints are `with-ksp.py` and `without-ksp.py`. Run
```sh
python3 without-ksp.py
```
to run the program without KSP, or
```sh
python3 with-ksp.py
```
to run it with KSP.

