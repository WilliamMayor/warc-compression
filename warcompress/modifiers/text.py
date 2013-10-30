import random


class TextDelete:

    def __init__(self, blocksize=1, count=20):
        self.blocksize = int(blocksize)
        self.count = int(count)
        self.name = ('TextDelete '
                     '%d character(s) '
                     '%d times') % (self.blocksize, self.count)

    def modify(self, text):
        yield text
        modified = text
        for _ in xrange(0, self.count):
            position = random.randint(
                0,
                max(0, len(modified) - self.blocksize)
            )
            modified = (modified[:position] +
                        modified[(position+self.blocksize):])
            yield modified


class TextInsert:

    def __init__(self, blocksize=1, count=20):
        self.blocksize = int(blocksize)
        self.count = int(count)
        self.name = ('TextInsert '
                     '%d character(s) '
                     '%d times') % (self.blocksize, self.count)

    def modify(self, text):
        yield text
        modified = text
        for _ in xrange(0, self.count):
            position = random.randint(
                0,
                max(0, len(modified) - self.blocksize)
            )
            new_chars = [random.choice(text) for _ in xrange(self.blocksize)]
            modified = modified[:position] + ''.join(new_chars) + modified[position:]
            yield modified


class TextSubstitute:

    def __init__(self, blocksize=1, count=20):
        self.blocksize = int(blocksize)
        self.count = int(count)
        self.name = ('TextSubstitute '
                     '%d character(s) '
                     '%d times') % (self.blocksize, self.count)

    def modify(self, text):
        yield text
        modified = text
        for _ in xrange(0, self.count):
            position = random.randint(
                0,
                max(0, len(modified) - self.blocksize)
            )
            new_chars = [random.choice(text) for _ in xrange(self.blocksize)]
            modified = (modified[:position] +
                        ''.join(new_chars) +
                        modified[(position+self.blocksize):])
            yield modified
