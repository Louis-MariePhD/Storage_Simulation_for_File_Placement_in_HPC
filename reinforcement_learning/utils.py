import string
import torch

DEVICE = "cuda" if torch.cuda.is_available() else "cpu"
VOCAB = list(string.printable[:-5])+['é', 'è', 'ê', 'ë', 'ù']
VOCAB_LEN = len(VOCAB)
STR2TENSOR_OUTPUT_SIZE = 128


def get_output_shape(model, image_dim):
    """
    To be used in a model constructor to reliably get the output shape of a layer. Not to be used in forward: it's slow.
    """
    return model(torch.rand(*(image_dim))).data.shape


def str2array(input_string, vocab=VOCAB):
    """
    :param input_string:
    :param target_output_size:
    :param vocab:
    :return: the str converted to an integer array
    """
    path_embedded = [vocab.index(char) if char in vocab else vocab.index(' ') for char in input_string]
    if len(path_embedded) > STR2TENSOR_OUTPUT_SIZE:
        return path_embedded[len(path_embedded) - STR2TENSOR_OUTPUT_SIZE:len(path_embedded):1]
    elif len(path_embedded) < STR2TENSOR_OUTPUT_SIZE:
        return [0 for _ in range(STR2TENSOR_OUTPUT_SIZE - len(path_embedded))] + path_embedded
