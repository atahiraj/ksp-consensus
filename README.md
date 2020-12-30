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

The entrypoint for training and testing is `main.py`.

### Configuration

The `config.py` file contains the architecture configuration as well as other hyperparameters.
- `TEST`: set to `True` for testing a model and to `False` otherwise
- `SAVE`: specify under which name the weights should be saved (irrelevant if `TEST` is set to `True`)
- `LOAD`: specify a path for a checkpoint to continue training or to test (relevant only if `TEST` is set to `True`)
- `EMB`: specify the word embeddings to be used (`FastText`, `GloVe` or `Word2Vec`, case insensitive)
- `BATCH_SIZE`: specify the batch size
- `N_EPOCHS`: specify the number of epochs
- `N_LAYERS`: specify the number of layers of both LSTMs of the encoder and decoder
- `HID_DIM`: specify the hidden state dimension of both LSTMs of the encoder and decoder. Note that it correspondes to the size of `h` and `c`, unconcatenated
- `ENC_DROPOUT`: dropout probability of the encoder
- `DEC_DROPOUT`: dropout probability of the decoder
- `FREEZE`: set to `True` to freeze the embedding layers during training and to `False` otherwise

### Train and test
Once the file `config.py` has been modified, just run
```sh
python3 main.py
```

