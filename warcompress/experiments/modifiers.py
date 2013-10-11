import random


def identity(text):
    return text


def delete(text, blocksize=1):
    position = random.randint(0, max(0, len(text) - blocksize))
    return text[:position] + text[(position+blocksize):]


def insert(text, blocksize=1):
    new_chars = [random.choice(text) for _ in xrange(blocksize)]
    position = random.randint(0, len(text))
    return text[:position] + ''.join(new_chars) + text[position:]
