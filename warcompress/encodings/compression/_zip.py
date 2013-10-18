import os
import zipfile


def encode(data_path):
    path = os.path.splitext(data_path)[0] + '.zip'
    with zipfile.ZipFile(path, 'w', zipfile.ZIP_DEFLATED) as archive:
        archive.write(data_path)
    return path
