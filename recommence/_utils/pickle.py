import pickle


def read_pickle(path: str):
    with open(path, 'rb') as f:
        return pickle.load(f)
