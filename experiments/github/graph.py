import time
import sqlite3
import os
import math
import datetime


import matplotlib.pyplot as plt

compressions = ['gzip', 'bzip2', 'tar.gz', 'tar.bz2']
deltas = ['diffe', 'bsdiff', 'vcdiff']
delta_strategies = ['first', 'previous', '10']
db_root = '/Users/william/Desktop/results'
max_churn = [None, 0]
mean_churn = [0, 1]


def churn(query, db='index.db', attach=None, commit=False):
    print 'Running\n%s' % query
    global max_churn, mean_churn
    tick = time.time()
    db_path = os.path.join(db_root, db)
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    if attach is not None:
        cursor.execute('ATTACH "%s" AS a' % os.path.join(db_root, attach))
    cursor.execute(query)
    result = cursor.fetchall()
    if commit:
        conn.commit()
    conn.close()
    tt = time.time() - tick
    if tt > max_churn[1]:
        max_churn = [query, tt]
    if tt > 10:
        mean_churn = [mean_churn[0] + tt, mean_churn[1] + 1]
    return result

# Prepare by indexing
churn('CREATE INDEX IF NOT EXISTS idx_uri ON record(uri)', commit=True)
churn('CREATE INDEX IF NOT EXISTS idx_job ON record(job)', commit=True)
churn('CREATE INDEX IF NOT EXISTS idx_path ON record(path)', commit=True)
churn('CREATE INDEX IF NOT EXISTS idx_record_type ON record(record_type)', commit=True)
churn('CREATE INDEX IF NOT EXISTS idx_date ON record(date)', commit=True)
try:
    churn('ALTER TABLE record ADD COLUMN day INTEGER', commit=True)
    churn('UPDATE record SET day = round(julianday(date))', commit=True)
except:
    pass
churn('CREATE INDEX IF NOT EXISTS idx_day ON record(job)', commit=True)

for db in os.walk(db_root).next()[2]:
    if db.endswith('.db') and db not in ['index.db']:
        churn(
            """CREATE INDEX IF NOT EXISTS idx_ct
            ON archivesize(compression_type)""",
            db=db, commit=True)

# Calculate
uri_count = churn('SELECT COUNT(DISTINCT uri) FROM record')
domain_count = churn('SELECT COUNT(DISTINCT job) FROM record')
unique_response = churn("""
    SELECT COUNT(uri)
    FROM record
    WHERE record_type = "response" """)
request_count = churn("""
    SELECT COUNT(uri)
    FROM record
    WHERE record_type = "request" """)
min_date = churn("""
    SELECT MIN(date), MIN(day)
    FROM record
    WHERE date > "2008-01-01T00:00:00" """)
max_date = churn("""
    SELECT MAX(date), MAX(day)
    FROM record
    WHERE date < "2014-01-01T00:00:00" """)
_r = churn("""
    SELECT size
    FROM archivesize
    WHERE compression_type = "no_compression" """, db='no_delta.db')
raw_warc_sizes = [r[0] for r in _r]
large_warc_count = len(filter(lambda s: s > 1024*1024*1024, raw_warc_sizes))
warc_file_count = churn('SELECT COUNT(DISTINCT path) FROM record')[0][0]
production_file_count = churn("""
    SELECT SUM(size)
    FROM archivesize
    WHERE compression_type = "no_compression" """, db='no_delta.db')[0][0]
production_file_count = math.ceil(production_file_count / (1024*1024*1024))
file_changes_per_day = churn("""
    SELECT day, COUNT(*)
    FROM record WHERE record_type="response"
    GROUP BY day""")
split = int(churn(
    'SELECT round(julianday("2013-05-05")) - %d' % min_date[0][1])[0][0])
domain_changes_per_day = churn("""
    SELECT day, COUNT(DISTINCT job)
    FROM record
    WHERE record_type="response"
    GROUP BY day""")
change_per_day_by_content_type = churn("""
    SELECT content_type, AVG(cpd)
    FROM
        (SELECT content_type,
                COUNT(DISTINCT day) / (MAX(day) - MIN(day)) AS cpd
        FROM record
        WHERE record_type = "response"
        GROUP BY uri
        HAVING MAX(day) != MIN(day))
    GROUP BY content_type
""")
uri_count_by_domain = churn("""
    SELECT job, COUNT(DISTINCT uri)
    FROM record
    GROUP BY job""")
avg_uri_count_by_domain = churn("""
    SELECT job, AVG(c)
    FROM
        (SELECT job, COUNT(DISTINCT uri) AS c
        FROM record
        GROUP BY job, day)
    GROUP BY job""")
content_type_counts = churn("""
    SELECT content_type, COUNT(DISTINCT uri)
    FROM record
    GROUP BY content_type""")
sql = """
    SELECT compression_type, SUM(size)
    FROM archivesize
    GROUP BY compression_type"""
archive_sizes = {}
for db in os.walk(db_root).next()[2]:
    if db.endswith('.db') and db != 'index.db':
        name = db[:-3]
        archive_sizes[name] = {}
        for ct, s in churn(sql, db=db):
            archive_sizes[name][ct] = s


nc = map(lambda t: (t[0], t[1]['no_compression']), archive_sizes.iteritems())
at10 = sum(map(lambda t: t[1], filter(lambda t: '@10' in t[0], nc)))
previous = sum(map(lambda t: t[1], filter(lambda t: '@previous' in t[0], nc)))
at10_overheads_previous = float(at10) / previous - 1


first = archive_sizes['bsdiff@first']['no_compression']
previous = archive_sizes['bsdiff@previous']['no_compression']
bsdiff_first_overheads_previous = float(first) / previous - 1


mean_record_sizes = {}
_r = churn("""
    SELECT record_type, AVG(content_length)
    FROM record
    GROUP BY record_type""")
for r in _r:
    mean_record_sizes[r[0]] = r[1]

sql = 'SELECT AVG(content_length) FROM recordsize'
mean_record_sizes['delta'] = {}
for db in os.walk(db_root).next()[2]:
    if db.endswith('.db') and db not in ['index.db', 'no_delta.db']:
        name = db[:-3]
        s, c = churn(
            'SELECT SUM(content_length), COUNT(*) FROM recordsize', db=db)[0]
        ms, mc = churn("""
            SELECT SUM(content_length), COUNT(*)
            FROM record
            WHERE record.record_id NOT IN (
                SELECT record_id
                FROM a.recordsize)
                AND record_type="response" """, attach=db)[0]
        mean_record_sizes['delta'][name] = float(s + ms) / (c + mc)

print 'Longest query took %d seconds:\n %s' % (max_churn[1], max_churn[0])
print 'The average query took %d seconds' % (mean_churn[0] / mean_churn[1],)

try:
    os.makedirs('images')
except:
    pass

plt.clf()
labels, heights = zip(*sorted(
    [(k, v) for k, v in mean_record_sizes.iteritems() if k != 'delta'],
    key=lambda x: x[1]))
dlabels, dheights = zip(*sorted(
    [(k, v) for k, v in mean_record_sizes['delta'].iteritems()],
    key=lambda x: x[1], reverse=True))
labels += dlabels
heights += dheights
plt.bar(xrange(len(heights)), heights)
plt.axis([0, len(heights), 0, max(heights)])
plt.xlabel('Record Type')
plt.ylabel('Mean Record Size (bytes)')
plt.xticks(
    [i + 0.5 for i in xrange(len(heights))],
    labels, rotation='vertical')
plt.gcf().set_size_inches(6, 5)
plt.subplots_adjust(bottom=0.35, left=0.2)
plt.savefig('images/record_type_mean_size.png')


plt.clf()
x1 = [r[0] - min_date[0][1]
      for r in file_changes_per_day if r[0] - min_date[0][1] < split]
y1 = [r[1] for r in file_changes_per_day if r[0] - min_date[0][1] < split]
plt.plot(x1, y1, 'r.')
plt.axis([0, split, 0, max(y1)+10])
plt.xlabel('Day')
plt.ylabel('Number of changed files')
plt.savefig('images/file_changes_per_day_first.png')

plt.clf()
x2 = [r[0] - min_date[0][1]
      for r in file_changes_per_day if r[0] - min_date[0][1] >= split]
y2 = [r[1] for r in file_changes_per_day if r[0] - min_date[0][1] >= split]
plt.plot(x2, y2, 'r.')
plt.axis([split, max_date[0][1] - min_date[0][1], 0, max(y2)+50])
plt.xlabel('Day')
plt.ylabel('Number of changed files')
plt.savefig('images/file_changes_per_day_last.png')

mean_file_changes_first = float(sum(y1)) / split
_d = max_date[0][1] - min_date[0][1] - split
mean_file_changes_last = float(sum(y2)) / _d


plt.clf()
x1 = [r[0] - min_date[0][1]
      for r in domain_changes_per_day if r[0] - min_date[0][1] < split]
y1 = [r[1] for r in domain_changes_per_day if r[0] - min_date[0][1] < split]
plt.plot(x1, y1, 'r.')
plt.axis([0, split, 0, max(y1)])
plt.xlabel('Day')
plt.ylabel('Number of domain changes')
plt.savefig('images/domain_changes_per_day_first.png')

plt.clf()
x2 = [r[0] - min_date[0][1]
      for r in domain_changes_per_day if r[0] - min_date[0][1] >= split]
y2 = [r[1] for r in domain_changes_per_day if r[0] - min_date[0][1] >= split]
plt.plot(x2, y2, 'r.')
plt.axis([split, max_date[0][1] - min_date[0][1], 0, max(y2)])
plt.xlabel('Day')
plt.ylabel('Number of domain changes')
plt.savefig('images/domain_changes_per_day_last.png')

mean_domain_changes_first = float(sum(y1)) / split
_d = (max_date[0][1] - min_date[0][1] - split)
mean_domain_changes_last = float(sum(y2)) / _d


plt.clf()
_s = sorted(uri_count_by_domain, reverse=True, key=lambda x: x[1])
heights = [math.log(r[1]) for r in _s]
plt.plot(list(xrange(len(heights))), heights, 'r.')
plt.axis([0, len(heights), 0, max(heights)])
plt.xlabel('Domain')
plt.ylabel('Number of URIs (log)')
plt.suptitle('Number of URIs per domain')
plt.savefig('images/uris_per_domain.png')

largest_domain = [_s[0][0], _s[0][1], None]
smallest_domain = [_s[-1][0], _s[-1][1], None]


plt.clf()
_s = sorted(avg_uri_count_by_domain, reverse=True, key=lambda x: x[1])
heights = [math.log(r[1]) for r in _s]
plt.plot(list(xrange(len(heights))), heights, 'r.')
plt.axis([0, len(heights), 0, max(heights)])
plt.xlabel('Domain')
plt.ylabel('Average number of URIs (log)')
plt.suptitle('Average number of URIs per domain')
plt.savefig('images/uris_per_domain_avg.png')

largest_domain[2] = _s[0][1]
smallest_domain[2] = _s[-1][1]

if largest_domain[0] != _s[0][0]:
    print 'Largest domains not the same'
if smallest_domain[0] != _s[-1][0]:
    print 'Smallest domains not the same'


plt.clf()
counts = sorted(
    filter(lambda x: x[0] is not None, content_type_counts),
    reverse=True, key=lambda x: x[1])
total = sum(map(lambda x: x[1], counts))
bars = [float(r[1]) / total for r in counts]
labels = [r[0].strip() for r in counts]
plt.bar(xrange(len(bars)), bars)
plt.axis([0, len(bars), 0, 1])
plt.xlabel('Content Type')
plt.ylabel('Number of Distinct URIs')
plt.suptitle('The number of distinct URIs of each content type')
plt.xticks([i + 0.5 for i in xrange(len(bars))], labels, rotation='vertical')
plt.gcf().set_size_inches(5, 5)
plt.subplots_adjust(bottom=0.6)
plt.savefig('images/uris_by_content_type.png')


plt.clf()
bars = sorted(
    [(d, s['no_compression']) for d, s in archive_sizes.iteritems()],
    reverse=True, key=lambda x: x[1])
plt.bar(xrange(len(bars)), [x[1] for x in bars])
plt.axis([0, len(bars), 0, max([x[1] for x in bars])])
plt.ylabel('Total Archive Size (bytes)')
plt.xticks(
    [i + 0.5 for i in xrange(len(bars))], [x[0] for x in bars],
    rotation='vertical')
plt.gcf().set_size_inches(5, 5)
plt.subplots_adjust(bottom=0.3)
plt.savefig('images/tas_delta.png')

delta_max_size = max(bars, key=lambda x: x[1])
delta_min_size = min(bars, key=lambda x: x[1])


plt.clf()
bars = sorted(
    [(c, s) for c, s in archive_sizes['no_delta'].iteritems()],
    reverse=True, key=lambda x: x[1])
plt.bar(xrange(len(bars)), [x[1] for x in bars])
plt.axis([0, len(bars), 0, max([x[1] for x in bars])])
plt.ylabel('Total Archive Size (bytes)')
plt.xticks(
    [i + 0.5 for i in xrange(len(bars))], [x[0] for x in bars],
    rotation='vertical')
plt.gcf().set_size_inches(5, 5)
plt.subplots_adjust(bottom=0.3)
plt.savefig('images/tas_compression.png')

comp_max_size = max(bars, key=lambda x: x[1])
comp_min_size = min(bars, key=lambda x: x[1])


plt.clf()
nothing = archive_sizes['no_delta']['no_compression']
recommended = archive_sizes['no_delta']['gzip']
best = ('Error', float('inf'))
for d in archive_sizes:
    for c in archive_sizes[d]:
        if archive_sizes[d][c] < best[1]:
            best = ('%s:%s' % (d, c), archive_sizes[d][c])
plt.bar(
    [0, 1, 2, 3, 4],
    [nothing, delta_min_size[1], recommended,
     comp_min_size[1], best[1]])
plt.axis([0, 5, 0, nothing])
plt.ylabel('Total Archive Size (bytes)')
plt.xticks(
    [0.5, 1.5, 2.5, 3.5, 4.5],
    ['original', delta_min_size[0], 'GZip only',
     comp_min_size[0], best[0]], rotation='vertical')
plt.gcf().set_size_inches(5, 5)
plt.subplots_adjust(bottom=0.35)
plt.savefig('images/tas_best.png')


def commaify(x):
    if type(x) not in [type(0), type(0L)]:
        raise TypeError("Parameter must be an integer.")
    if x < 0:
        return '-' + commaify(-x)
    result = ''
    while x >= 1000:
        x, r = divmod(x, 1000)
        result = ",%03d%s" % (r, result)
    return "%d%s" % (x, result)


def nice_date(d):
    d = d.rstrip('Z')
    return datetime.datetime.strptime(
        d, '%Y-%m-%dT%H:%M:%S').strftime('%d %B %Y')


def strat_name(s):
    d, l = s.split('@', 1)
    try:
        return '%s with a reference record every %d records' % (d, int(l))
    except:
        return '%s comparing against the %s record' % (d, l)


print """
\def \gzipcompressionpct {{{gzip:.5f}}}
\def \\bzipcompressionpct {{{bzip2:.5f}}}
\def \\beststrategypct {{{best_strat:.5f}}}
\def \\bestdeltapct {{{best_delta_pct:.5f}}}
\def \\bestdeltaname {{{best_delta_name}}}
\def \\bestvsgzippct {{{best_vs_gzip:.5f}}}
\def \\bzipvsgzippct {{{bzip_vs_gzip:.5f}}}
\def \domainscrawled {{{domainscrawled}}}
\def \uniqueuris {{{uniqueuris}}}
\def \\requestssent {{{requestssent}}}
\def \uniqueresponses {{{uniqueresponses}}}
\def \oldestpage {{{oldestpage}}}
\def \youngestpage {{{youngestpage}}}
\def \\totaldays {{{totaldays}}}
\def \iostarted {{{io_started}}}
\def \\afterio {{{after_io}}}
\def \largewarccount {{{large_warc_count}}}
\def \meanwarcsize {{{mean_warc_size}}}
\def \maxwarcsize {{{max_warc_size}}}
\def \minwarcsize {{{min_warc_size}}}
\def \\newiasize {{{new_ia_size:.5f}}}
\def \\newglaciercost {{{new_glacier_cost}}}
\def \\newglaciersavings {{{glacier_savings}}}
\def \largestdomainname {{{largest_domain_name}}}
\def \largestdomaintotal {{{largest_domain_total}}}
\def \largestdomainmean {{{largest_domain_mean}}}
\def \smallestdomainname {{{smallest_domain_name}}}
\def \smallestdomaintotal {{{smallest_domain_total}}}
\def \smallestdomainmean {{{smallest_domain_mean}}}
\def \meanfilechangesfirst {{{mean_file_changes_first:.5f}}}
\def \meanfilechangeslast {{{mean_file_changes_last:.5f}}}
\def \meandomainchangesfirst {{{mean_domain_changes_first:.5f}}}
\def \meandomainchangeslast {{{mean_domain_changes_last:.5f}}}
\def \\referenceframeoverhead {{{at10_overheads_previous:.5f}}}
\def \\bsdifffirstoverhead {{{bsdiff_first_overheads_previous:.5f}}}
\def \warcfilecount {{{warc_file_count}}}
\def \productionfilecount {{{production_file_count}}}
\def \meanresponsesize {{{mean_response_size:.5f}}}
\def \meanresponsebsdiffimprovement {{{mean_response_bsdiff_improvement:.5f}}}
\def \meanresponsebsdiffsize {{{mean_response_bsdiff_size:.5f}}}
\def \meanresponsebsdiffmultiplier {{{mean_response_bsdiff_multiplier}}}
""".format(
    gzip=(100.0 * archive_sizes['no_delta']['gzip'] / comp_max_size[1]),
    bzip2=(100.0 * archive_sizes['no_delta']['bz2'] / comp_max_size[1]),
    bzip_vs_gzip=(100.0 * archive_sizes['no_delta']['bz2'] / recommended),
    best_delta_pct=(100.0 * delta_min_size[1] / delta_max_size[1]),
    best_delta_name=strat_name(delta_min_size[0]),
    best_strat=(100.0 * best[1] / nothing),
    best_vs_gzip=(100.0 * best[1] / recommended),
    domainscrawled=commaify(domain_count[0][0]),
    uniqueuris=commaify(uri_count[0][0]),
    requestssent=commaify(request_count[0][0]),
    uniqueresponses=commaify(unique_response[0][0]),
    oldestpage=nice_date(min_date[0][0]),
    youngestpage=nice_date(max_date[0][0]),
    totaldays=commaify(int(max_date[0][1] - min_date[0][1])),
    large_warc_count=commaify(large_warc_count),
    mean_warc_size=commaify(sum(raw_warc_sizes) / len(raw_warc_sizes)),
    min_warc_size=commaify(min(raw_warc_sizes)),
    max_warc_size=commaify(max(raw_warc_sizes)),
    new_ia_size=(2.0 * best[1] / recommended),
    new_glacier_cost=commaify(int(20972.0 * best[1] / recommended)),
    glacier_savings=commaify(20972 - int(20972.0 * best[1] / recommended)),
    io_started=commaify(split),
    after_io=commaify(int(max_date[0][1] - min_date[0][1]) - split),
    mean_file_changes_first=mean_file_changes_first,
    mean_file_changes_last=mean_file_changes_last,
    mean_domain_changes_first=mean_domain_changes_first,
    mean_domain_changes_last=mean_domain_changes_last,
    largest_domain_name=largest_domain[0],
    largest_domain_total=commaify(largest_domain[1]),
    largest_domain_mean=commaify(int(largest_domain[2] + 0.5)),
    smallest_domain_name=smallest_domain[0],
    smallest_domain_total=commaify(smallest_domain[1]),
    smallest_domain_mean=commaify(int(smallest_domain[2] + 0.5)),
    at10_overheads_previous=100 * at10_overheads_previous,
    bsdiff_first_overheads_previous=100 * bsdiff_first_overheads_previous,
    warc_file_count=commaify(warc_file_count),
    production_file_count=commaify(int(production_file_count)),
    mean_response_size=mean_record_sizes['response'],
    mean_response_multiplier=int(mean_record_sizes['response'] / mean_record_sizes['metadata']),
    mean_response_bsdiff_improvement=100.0 * mean_record_sizes['response'] / mean_record_sizes['delta']['bsdiff@previous'],
    mean_response_bsdiff_size=mean_record_sizes['delta']['bsdiff@previous'],
    mean_response_bsdiff_multiplier=int(mean_record_sizes['delta']['bsdiff@previous'] / mean_record_sizes['metadata']))
