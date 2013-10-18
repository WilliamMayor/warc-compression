import os
import bz2


def encode(data_path):
    path = os.path.splitext(data_path)[0] + '.bz2'
    f_in = open(data_path, 'rb')
    f_out = bz2.BZ2File(path, 'w')
    f_out.writelines(f_in)
    f_out.close()
    f_in.close()
    return path
