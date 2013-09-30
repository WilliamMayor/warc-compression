from Experiment import data_generator_factory, Experiment


def modify(text):
    return Experiment.delete(text, 1)

if __name__ == '__main__':
    with open('base.txt', 'r') as fd:
        base_text = fd.read()
    e = Experiment(
        'one_char_delete',
        data_generator_factory(
            base_text,
            modify,
            len(base_text)
        )
    )
    e.run()
