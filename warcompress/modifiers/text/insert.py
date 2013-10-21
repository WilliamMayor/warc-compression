import random


def modify(text, percentage=0.01):
    blocksize = int(len(text) * percentage)
    new_chars = [random.choice(text) for _ in xrange(blocksize)]
    position = random.randint(0, len(text))
    return text[:position] + ''.join(new_chars) + text[position:]
