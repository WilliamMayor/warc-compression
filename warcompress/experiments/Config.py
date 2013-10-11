import ConfigParser


class Config:

    def __init__(self, path):
        self.path = path
        self.parser = ConfigParser.RawConfigParser()
        self.parser.read(path)

    def get(self, section, key, default=None):
        try:
            return self.parser.get(section, key)
        except (ConfigParser.NoOptionError, ConfigParser.NoSectionError):
            return default

    def getint(self, section, key, default=None):
        try:
            return self.parser.getint(section, key)
        except (ConfigParser.NoOptionError, ConfigParser.NoSectionError):
            return default

    def getfloat(self, section, key, default=None):
        try:
            return self.parser.getfloat(section, key)
        except (ConfigParser.NoOptionError, ConfigParser.NoSectionError):
            return default

    def set(self, section, key, value):
        try:
            self.parser.set(section, key, value)
        except ConfigParser.NoSectionError:
            self.parser.add_section(section)
            self.parser.set(section, key, value)

    def save(self):
        with open(self.path, 'wb') as fd:
            self.parser.write(fd)

    def sections(self):
        return self.parser.sections()
