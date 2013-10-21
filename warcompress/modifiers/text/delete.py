import random


def modify(text, percentage=0.01):
    blocksize = int(len(text) * percentage)
    position = random.randint(0, max(0, len(text) - blocksize))
    return text[:position] + text[(position+blocksize):]
