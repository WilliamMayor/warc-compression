

class Identity:

    def __init__(self, repeat=20):
        self.repeat = repeat
        self.name = 'Identity repeat %d times' % self.repeat

    def modify(self, data):
        for _ in xrange(0, self.repeat):
            yield data
