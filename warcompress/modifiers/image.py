from wand.image import Image


class ImageScale:

    def __init__(self, percentage=0.9, count=20):
        self.percentage = float(percentage)
        self.count = int(count)
        self.name = ('ImageScale '
                     '%f percent '
                     '%d times') % (self.percentage, self.count)

    def modify(self, data):
        with Image(blob=data) as img:
            modified = img.make_blob()
        yield modified
        for _ in xrange(0, self.count):
            with Image(blob=modified) as img:
                img.resize(
                    int(img.width * self.percentage),
                    int(img.height * self.percentage))
                modified = img.make_blob()
            yield modified


class ImageRotate:

    def __init__(self, degrees=10, count=20):
        self.degrees = int(degrees)
        self.count = int(count)
        self.name = ('ImageRotate '
                     '%d degrees '
                     '%d times') % (self.degrees, self.count)

    def modify(self, data):
        with Image(blob=data) as img:
            modified = img.make_blob()
        yield modified
        for _ in xrange(0, self.count):
            with Image(blob=modified) as img:
                img.rotate(self.degrees)
                modified = img.make_blob()
            yield modified


class ImageCrop:

    def __init__(self, top=0.05, right=0.05, bottom=0.05, left=0.05, count=20):
        self.top = float(top)
        self.left = float(left)
        self.bottom = float(bottom)
        self.right = float(right)
        self.count = int(count)
        self.name = ('ImageCrop '
                     't:%f r:%f b:%f l:%f'
                     '%d times') % (self.top, self.right, self.bottom,
                                    self.left, self.count)

    def modify(self, data):
        with Image(blob=data) as img:
            modified = img.make_blob()
        yield modified
        for _ in xrange(0, self.count):
            with Image(blob=modified) as img:
                w = img.width
                h = img.height
                img.crop(
                    int(w * self.left),
                    int(h * self.top),
                    int(w * (1 - self.right)),
                    int(h * (1 - self.bottom))
                )
                modified = img.make_blob()
            yield modified


class ImageGrayScale:

    def __init__(self):
        self.name = 'ImageGrayScale'

    def modify(self, data):
        with Image(blob=data) as img:
            modified = img.make_blob()
        yield modified
        with Image(blob=modified) as img:
            img.type = 'grayscale'
            yield img.make_blob()


class ImageModulate:

    def __init__(self, brightness=105, hue=105, saturation=105, count=20):
        self.brightness = float(brightness)
        self.hue = float(hue)
        self.saturation = float(saturation)
        self.count = int(count)
        self.name = ('ImageModulate '
                     'brightness:%f hue:%f saturation:%f'
                     '%d times') % (self.brightness, self.hue, self.saturation,
                                    self.count)

    def modify(self, data):
        with Image(blob=data) as img:
            modified = img.make_blob()
        yield modified
        for _ in xrange(0, self.count):
            with Image(blob=modified) as img:
                img.modulate(
                    brightness=self.brightness,
                    hue=self.hue,
                    saturation=self.saturation
                )
                modified = img.make_blob()
            yield modified
