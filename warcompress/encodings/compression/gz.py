import os
import gzip


def encode(data_path):
    path = os.path.splitext(data_path)[0] + '.gz'
    f_in = open(data_path, 'rb')
    f_out = gzip.open(path, 'wb')
    f_out.writelines(f_in)
    f_out.close()
    f_in.close()
    return path
