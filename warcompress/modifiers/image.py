from wand.image import Image


class ImageShrink:

    def __init__(self, percentage=0.1, count=20):
        self.percentage = float(percentage)
        self.count = int(count)
        self.name = ('ImageShrink '
                     '%f percent '
                     '%d times') % (self.percentage, self.count)

    def modify(self, data):
        with Image(blob=data) as img:
            modified = img.make_blob()
        yield modified
        for _ in xrange(0, self.count):
            with Image(blob=modified) as img:
                img.resize(
                    int(img.width * (1 - self.percentage)),
                    int(img.height * (1 - self.percentage)))
                modified = img.make_blob()
            yield modified
