"""
python statistics.py main.db compare.db compare2.db ...
"""
import sqlite3
import sys

uri_count = ('SELECT COUNT(DISTINCT uri) AS "Distinct URIs"'
             ' FROM record')

domain_uri_count = ('SELECT COUNT(DISTINCT uri) AS "Distinct In-Domain URIs"'
                    ' FROM record'
                    ' WHERE uri LIKE "%localhost%"')

content_type_size_count = ('SELECT content_type AS "Content Type",'
                           '       AVG(content_length) AS "Average Size (bytes)",'
                           '       COUNT(*) AS "Crawl Count"'
                           ' FROM record'
                           ' GROUP BY content_type'
                           ' ORDER BY 2 DESC')

average_changes_per_day = ('SELECT content_type AS "Content Type",'
                           '       AVG(count / (julianday(max_date) - julianday(min_date))) AS "Average Average Changes per day"'
                           ' FROM (SELECT content_type,'
                           '              COUNT(DISTINCT digest) AS count,'
                           '              MIN(date) AS min_date,'
                           '              MAX(date) AS max_date'
                           '        FROM record'
                           '        GROUP BY uri)'
                           ' GROUP BY content_type'
                           ' ORDER BY 2 DESC')

record_type_size_count = ('SELECT record_type AS "Record Type",'
                          '       COUNT(record_type) AS "Count",'
                          '       AVG(content_length) AS "Average Size (bytes)"'
                          ' FROM record'
                          ' GROUP BY record_type'
                          ' ORDER BY 3 DESC')

all_record_comparison = ('SELECT AVG(raw.content_length - d.content_length) AS "Average Size Difference (bytes, all records)",'
                         '       AVG(raw.content_length / d.content_length) AS "Average Compression Ratio (all records)",'
                         '       AVG(1 - d.content_length / raw.content_length) AS "Average Space Savings (all records)"'
                         ' FROM record AS raw,'
                         '      compare.record as d '
                         ' WHERE raw.record_id == d.record_id')

response_record_comparison = ('SELECT AVG(raw.content_length - d.content_length) AS "Average Size Difference (bytes, response records)",'
                              '       AVG(raw.content_length / d.content_length) AS "Average Compression Ratio (response records)",'
                              '       AVG(1 - d.content_length / raw.content_length) AS "Average Space Savings (response records)"'
                              ' FROM record AS raw,'
                              '      compare.record as d '
                              ' WHERE raw.record_id == d.record_id'
                              '       AND raw.record_type = "response"')

response_record_comparison_by_content_type = ('SELECT raw.content_type,'
                                              '       AVG(raw.content_length - d.content_length) AS "Average Size Difference (bytes, response records)",'
                                              '       AVG(raw.content_length / d.content_length) AS "Average Compression Ratio (response records)",'
                                              '       AVG(1 - d.content_length / raw.content_length) AS "Average Space Savings (response records)"'
                                              ' FROM record AS raw,'
                                              '      compare.record as d '
                                              ' WHERE raw.record_id == d.record_id'
                                              '       AND raw.record_type = "response"'
                                              ' GROUP BY raw.content_type')


def pp(cursor, data=None, rowlens=0):
    d = cursor.description
    if not d:
        return "#### NO RESULTS ###"
    names = []
    lengths = []
    rules = []
    if not data:
        data = cursor.fetchall()
    for dd in d:    # iterate over description
        l = dd[1]
        if not l:
            l = 12             # or default arg ...
        l = max(l, len(dd[0])) # handle long names
        names.append(dd[0])
        lengths.append(l)
    for col in range(len(lengths)):
        if rowlens:
            rls = [len(str(row[col])) for row in data if row[col]]
            lengths[col] = max([lengths[col]]+rls)
        rules.append("-"*lengths[col])
    format = " ".join(["%%-%ss" % l for l in lengths])
    result = [format % tuple(names)]
    result.append(format % tuple(rules))
    for row in data:
        result.append(format % tuple([str(r).strip() for r in row]))
    return "\n".join(result)

conn = sqlite3.connect(sys.argv[1])
cursor = conn.cursor()

for sql in [uri_count, domain_uri_count, content_type_size_count, average_changes_per_day, record_type_size_count]:
    cursor.execute(sql)
    print pp(cursor, rowlens=True), '\n'

for compare in sys.argv[2:]:
    cursor.execute('ATTACH "%s" AS compare' % compare)
    print 'Comparing', compare
    for sql in [all_record_comparison, response_record_comparison, response_record_comparison_by_content_type]:
        cursor.execute(sql)
        print pp(cursor, rowlens=True), '\n'
    cursor.execute('DETACH compare')
