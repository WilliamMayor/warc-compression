import pprint
import os

import archive

from WARC import WARC

by_date_path = os.path.join('test', 'by_date.warc')
by_digest_path = os.path.join('test', 'by_digest.warc')

by_date = WARC(by_date_path, order_by='WARC-Date')
by_digest = WARC(by_digest_path, order_by='WARC-Payload-Digest')
for r in archive.filter_records('test'):
    by_date.add(r)
    by_digest.add(r)
by_date.save()
by_digest.save()

by_date = list(WARC(by_date_path))
by_digest = list(WARC(by_digest_path))

try:
    ids = [r.headers['WARC-Record-ID'] for r in by_date]
    assert len(ids) == 31
    expected_ids = set(['<urn:uuid:2d3ce775-3a91-4227-b256-7d1cdb174de1>', '<urn:uuid:c6ed326f-8037-45a5-b7f5-02df483a8fab>', '<urn:uuid:d9e8885a-8a36-4096-b218-6d542d6ac329>', '<urn:uuid:ea040b0c-6616-42a3-b493-4cbab6e9d274>', '<urn:uuid:b6ae5081-c16a-4840-b467-7106afdc0f93>', '<urn:uuid:c9756ab8-1eff-4ed1-a2a2-808a1b66fad9>', '<urn:uuid:1663a697-74dc-435c-8419-3fc8a9503e4b>', '<urn:uuid:447365d2-7436-452a-bade-4ab1fa842e85>', '<urn:uuid:54ddf8ed-0606-4918-99c2-c0a06e9575d4>', '<urn:uuid:bf8c9d05-696a-4dde-9e40-ef0ec84ba50f>', '<urn:uuid:0b015eff-585b-4a56-80ab-be769e72f009>', '<urn:uuid:46d97916-f9c6-43cc-a040-7cca366604a2>', '<urn:uuid:a783821e-1a6b-4b9b-910a-733733256b52>', '<urn:uuid:27491225-9b42-4751-b8be-52a6d505811c>', '<urn:uuid:f8886060-657c-4beb-a4aa-fbe974501e0a>', '<urn:uuid:b7fa14b3-c6d0-4139-bb7a-03dc069518b5>', '<urn:uuid:8454eabb-adbf-4a00-ba8e-79907d2e6189>', '<urn:uuid:e7946479-aa7a-42c7-8eca-ae2678d83e27>', '<urn:uuid:0b46cd06-dee4-4891-99e9-8523548ef325>', '<urn:uuid:7379008d-9495-407b-96e7-6c95a1be43f8>', '<urn:uuid:d015cdd7-39b1-4a48-a537-cd220b7ae14e>', '<urn:uuid:3f348f44-49e5-4398-b9eb-70cef9bdb284>', '<urn:uuid:5190ef46-8fa0-482a-ba5e-77364428051b>', '<urn:uuid:cefe8d16-3a06-4353-b21d-d546c887505a>', '<urn:uuid:1409bffd-dd90-434c-a45d-3513b10859e7>', '<urn:uuid:afb8898e-b07f-422b-962a-bd55f13a1c57>', '<urn:uuid:9b51cf15-263e-47cc-874c-257e474ad6c8>', '<urn:uuid:57c3c88b-56c1-4372-969d-f08ae8e57bdb>', '<urn:uuid:8f8b7366-a18a-452c-b428-43aa38567300>', '<urn:uuid:2ccbd7f1-458e-430e-b1c2-d88947ab5443>', '<urn:uuid:0c1f28e7-0b54-425f-a374-b10df4ade457>'])
    assert set(ids) == expected_ids

    assert by_date[0].headers['WARC-Type'].lower() == 'warcinfo'
    assert by_digest[0].headers['WARC-Type'].lower() == 'warcinfo'

    dates = [r.headers['WARC-Date'] for r in by_date]
    digests = [r.headers.get('WARC-Payload-Digest', None) for r in by_date]
    assert sorted(dates) == dates
    assert sorted(digests) != digests
    digests = filter(lambda d: d is not None, digests)

    digests = [r.headers.get('WARC-Payload-Digest', None) for r in by_digest]
    digests = filter(lambda d: d is not None, digests)
    assert sorted(digests) == digests

finally:
    os.remove(by_date_path)
    os.remove(by_digest_path)
