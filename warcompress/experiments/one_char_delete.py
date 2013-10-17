"""
Removes a single character at random from the file 100 times
"""
import os
import sys
from warcompress.experiments.Experiment import Experiment
from warcompress.experiments import modifiers
import warcompress.experiments.encodings as encodings


def factory_factory(base_path, data_path):
    def g():
        with open(base_path, 'r') as fd:
            base_text = fd.read()
        text = base_text
        previous = base_text
        for i in xrange(0, 20):
            previous = modifiers.delete(previous, 1)
            text += encodings.RECORD_SEPARATOR + previous
        with open(data_path, 'w') as fd:
            fd.write(text)
        return data_path
    g.dir = os.path.dirname(data_path)
    return g

if __name__ == '__main__':
    if len(sys.argv) != 2:
        print 'Must provide base data to work from'
        exit(1)
    base_text_path = sys.argv[1]
    base_text_dir = os.path.dirname(base_text_path)
    data_dir = os.path.join(base_text_dir, 'one_char_delete')
    try:
        os.mkdir(data_dir)
    except OSError:
        # Directory already exists
        pass
    data_path = os.path.join(data_dir, 'data.txt')
    e = Experiment(
        'one_char_delete',
        factory_factory(base_text_path, data_path)
    )
    e.run(1)
