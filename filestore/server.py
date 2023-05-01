import os
import sys
import ssl
import time
import uuid
import json
import sanic
import signal
import pickle
import random
import hashlib
import asyncio
import aiohttp
import logging


APP = sanic.Sanic('logdb')
APP.config.KEEP_ALIVE_TIMEOUT = 300

QUORUM = None
SERVERS = set()
DEFAULT_SEQ = '00000000-000000'
LEARNED_SEQ = '99999999-999999'


def blob_dump(path, blob):
    tmpfile = '{}-{}.tmp'.format(path, uuid.uuid4())
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(tmpfile, 'wb') as fd:
        fd.write(blob)
    os.replace(tmpfile, path)


def paxos_dump(path, promised_seq, accepted_seq, accepted_value):
    blob_dump(path, json.dumps(
        [promised_seq, accepted_seq, accepted_value],
        indent=4, sort_keys=True).encode())


# PAXOS Acceptor and Learner
@APP.post('/paxos/<phase:str>/<proposal_seq:str>/<path:path>')
async def paxos_server(request, phase, proposal_seq, path):
    if 'http' != request.scheme and get_peer(request) not in SERVERS:
        raise sanic.exceptions.Unauthorized()

    accepted_value = dict()
    promised_seq = accepted_seq = DEFAULT_SEQ

    if os.path.isfile(path):
        with open(path) as fd:
            promised_seq, accepted_seq, accepted_value = json.load(fd)

    if LEARNED_SEQ == promised_seq == accepted_seq:
        if 'promise' == phase:
            return response([accepted_seq, accepted_value])

        elif phase in ('accept', 'learn'):
            return response('OK')

    # Paxos standard - phase 1
    if 'promise' == phase and proposal_seq > promised_seq:
        paxos_dump(path, proposal_seq, accepted_seq, accepted_value)
        return response([accepted_seq, accepted_value])

    # Paxos standard - phase 2
    if 'accept' == phase and proposal_seq == promised_seq:
        paxos_dump(path, proposal_seq, proposal_seq,
                   pickle.loads(request.body))
        return response('OK')

    # Paxos protocol is already complete. This is a custom learn step.
    if 'learn' == phase and proposal_seq == promised_seq == accepted_seq:
        # This paxos round completed. Mark this value as final.
        # Set promise_seq = accepted_seq = '99999999-999999'
        # This is the largest possible value for seq and would ensure
        # tha any subsequent paxos rounds get rejected.
        paxos_dump(path, LEARNED_SEQ, LEARNED_SEQ, accepted_value)
        return response('OK')

    raise sanic.exceptions.BadRequest()


# PAXOS Proposer
async def paxos_client(path, value):
    url = 'paxos/{{}}/{}/{}'.format(time.strftime('%Y%m%d-%H%M%S'), path)

    res = await rpc(url.format('promise'))
    if QUORUM > len(res):
        return 'NO_PROMISE_QUORUM'

    proposal = (DEFAULT_SEQ, value)
    for accepted_seq, accepted_val in res.values():
        if accepted_seq > proposal[0]:
            proposal = (accepted_seq, accepted_val)

    res = await rpc(url.format('accept'), proposal[1])
    if QUORUM > len(res):
        return 'NO_ACCEPT_QUORUM'

    # No need to check the result of this.
    # PAXOS round has already completed.  If a node doesn't learn
    # the value, another paxos round would be run at the read time.
    await rpc(url.format('learn'))

    return 'CONFLICT' if value is not proposal[1] else 'OK'


def response(obj):
    return sanic.response.raw(pickle.dumps(obj))


def get_peer(request):
    cert = request.transport.get_extra_info('peercert')
    return dict(cert['subject'][0])['commonName']


async def rpc(url, obj=None, servers=None):
    servers = servers if servers else SERVERS

    if not hasattr(rpc, 'session'):
        ssl_ctx = None
        rpc.scheme = 'http'
        if os.path.isfile('ca.pem'):
            ssl_ctx = ssl.create_default_context(
                cafile='ca.pem',
                purpose=ssl.Purpose.SERVER_AUTH)
            ssl_ctx.load_cert_chain('client.pem', 'client.key')
            ssl_ctx.verify_mode = ssl.CERT_REQUIRED
            rpc.scheme = 'https'

        rpc.session = aiohttp.ClientSession(connector=aiohttp.TCPConnector(
            ssl=ssl_ctx, limit=1000, limit_per_host=1000))

    responses = await asyncio.gather(
        *[asyncio.ensure_future(
          rpc.session.post('{}://{}/{}'.format(rpc.scheme, s, url),
                           data=pickle.dumps(obj)))
          for s in servers],
        return_exceptions=True)

    result = dict()
    for s, r in zip(servers, responses):
        if type(r) is aiohttp.client_reqrep.ClientResponse:
            if 200 == r.status:
                result[s] = pickle.loads(await r.read())

    return result


@APP.post('/uuid/<action:str>/<path:path>')
async def put_uuid(request, action, path):
    if 'http' != request.scheme and get_peer(request) not in SERVERS:
        raise sanic.exceptions.Unauthorized()

    if 'put' == action:
        blob_dump(path, pickle.loads(request.body))
        return response('OK')

    if 'get' == action:
        with open(path, 'rb') as fd:
            return response(fd.read())


@APP.put('/<path:path>/<version:int>')
async def put(request, path, version):
    writer = get_peer(request) if 'http' != request.scheme else ''
    writer = hashlib.sha256(writer.encode()).hexdigest()

    path = os.path.join('kv', writer, path.strip('/'))

    blobs = list()
    if request.body:
        # Write the content blobs
        blobs = [str(uuid.uuid4())]
        res = await rpc('uuid/put/{}/{}'.format(path, blobs[0]), request.body)
        if QUORUM > len(res):
            return 'NO_UUID_WRITE_QUORUM'

    tags = dict(host='{}:{}'.format(HOST, PORT),
                datetime=time.strftime('%Y-%m-%d %H:%M:%S'),
                path=path, version=int(version), blobs=blobs)

    path = os.path.join(path, str(int(version)))
    tags['status'] = await paxos_client(path, tags)
    return sanic.response.json(tags)


@APP.post('/version/<path:path>')
async def version(request, path):
    if 'http' != request.scheme and get_peer(request) not in SERVERS:
        raise sanic.exceptions.Unauthorized()

    path = path.strip('/')
    files = [int(f) for f in os.listdir(path) if f.isdigit()]

    for version in sorted(files, reverse=True):
        path = os.path.join(path, str(version))
        with open(path) as fd:
            promised_seq, accepted_seq, accepted_value = json.load(fd)

        if accepted_seq != DEFAULT_SEQ:
            return response(version)


@APP.get('/<path:path>')
async def get(request, path):
    reader = get_peer(request) if 'http' != request.scheme else ''
    reader = hashlib.sha256(reader.encode()).hexdigest()
    localpath = os.path.join('kv', reader, path.strip('/'))

    res = await rpc('version//{}'.format(localpath))
    ver = max(res.values())

    localpath = os.path.join(localpath, str(ver))

    promised_seq = DEFAULT_SEQ
    if os.path.isfile(localpath):
        with open(localpath) as fd:
            promised_seq, accepted_seq, accepted_value = json.load(fd)

    if LEARNED_SEQ != promised_seq:
        await put(request, path, ver)

    with open(localpath) as fd:
        promised_seq, accepted_seq, accepted_value = json.load(fd)

    for guid in accepted_value['blobs']:
        localpath = os.path.join(os.path.dirname(localpath), guid)

        if os.path.isfile(localpath):
            continue

        for i in range(len(SERVERS)):
            srv = random.choice(list(SERVERS))
            res = await rpc('uuid/get/{}'.format(localpath), servers=[srv])
            if res:
                blob_dump(localpath, res[srv])
                break

    blobs = list()
    for guid in accepted_value['blobs']:
        localpath = os.path.join(os.path.dirname(localpath), guid)
        with open(localpath, 'rb') as fd:
            blobs.append(fd.read())

    return sanic.response.raw(b''.join(blobs), headers={'x-seq': ver})


if '__main__' == __name__:
    for i in range(2, len(sys.argv)):
        SERVERS.add(sys.argv[i])

    QUORUM = int(len(SERVERS)/2) + 1

    HOST, PORT = sys.argv[1].split(':')

    ssl_ctx = None
    if os.path.isfile('ca.pem'):
        ssl_ctx = ssl.create_default_context(
            cafile='ca.pem',
            purpose=ssl.Purpose.CLIENT_AUTH)
        ssl_ctx.load_cert_chain('server.pem', 'server.key')
        ssl_ctx.verify_mode = ssl.CERT_REQUIRED

    signal.alarm(random.randint(1, 5))

    for i, srv in enumerate(sorted(SERVERS)):
        logging.critical('cluster node({}) : {}'.format(i+1, srv))
    logging.critical('server({}:{})'.format(HOST, PORT))
    APP.run(host=HOST, port=int(PORT), single_process=True, access_log=True,
            ssl=ssl_ctx)
