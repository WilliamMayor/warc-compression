import gzip
import os

bad = [
    ('2215-2215.github.io', 46787),
    ('columbia-3-columbia-3.github.io', 54960),
    ('morris-2-morris-2.github.io', 60494),
    ('rutgers-2-rutgers-2.github.io', 47582),
    ('WindowsAzure-windowsazure.github.io', 33847),
    ('columbia-4-columbia-4.github.io', 51750),
    ('morris-3-morris-3.github.io', 43922),
    ('rutgers-3-rutgers-3.github.io', 52991),
    ('alvinhui-alvinhui.github.io', 60227),
    ('darkjpeg-darkjpeg.github.io', 58741),
    ('morris-5-morris-5.github.io', 41834),
    ('thomasdavis-thomasdavis.github.io', 46616),
    ('chicago-1-chicago-1.github.io', 43133),
    ('ghc-1-ghc-1.github.io', 47406),
    ('morris-6-morris-6.github.io', 49492),
    ('tower-tower.github.io', 58992),
    ('chicago-2-chicago-2.github.io', 49997),
    ('hull-hull.github.io', 46746),
    ('noflo-noflo.github.io', 46970),
    ('typeplate-typeplate.github.io', 49324),
    ('mojombo-mojombo.github.io', 44960),
    ('project-open-data-project-open-data.github.io', 59985),
    ('columbia-2-columbia-2.github.io', 50263),
    ('morris-1-morris-1.github.io', 43286),
    ('rutgers-1-rutgers-1.github.io', 45396),
    ('railstutorial-china-railstutorial-china.github.io', 45890)]


def fix(job_dir):
    for b in bad:
        for root, dirs, files in os.walk(os.path.join(job_dir, b[0])):
            for f in files:
                if f.endswith('.warc.gz'):
                    print(f)
                    gz = gzip.open(os.path.join(root, f), 'rb')
                    content = gz.read()
                    gz.close()
                    content = content.replace(b[0], 'localhost:%d' % b[1])
                    gz = gzip.open(os.path.join(root, f), 'wb')
                    gz.write(content)
                    gz.close()

if __name__ == '__main__':
    import sys
    fix(sys.argv[1])
