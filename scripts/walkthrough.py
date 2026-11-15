#!/usr/bin/env python3
"""Run bounded production flows against a temporary Atlas Render server."""
import argparse
import base64
import hashlib
import json
import os
from pathlib import Path
import socket
import struct
import subprocess
import tempfile
import time
import urllib.error
import urllib.request
import zlib

ROOT = Path(__file__).resolve().parents[1]


def sample():
    return {"width": 320, "height": 240, "background": "#F5F1E8", "layers": [
        {"id": "plate", "kind": "rectangle", "width": 220, "height": 150,
         "transform": {"x": 45, "y": 35, "scaleX": 1, "scaleY": 1, "degrees": 5},
         "opacity": 1, "fill": {"kind": "linear", "color": "#145A70", "endColor": "#32A5A0", "from": {"x": 0, "y": 0}, "to": {"x": 220, "y": 150}}},
        {"id": "accent", "kind": "ellipse", "width": 130, "height": 130,
         "transform": {"x": 150, "y": 95, "scaleX": 1, "scaleY": 1, "degrees": 0},
         "opacity": .85, "fill": {"kind": "solid", "color": "#E87E4F"}},
        {"id": "inset", "kind": "group", "transform": {"x": 150, "y": 100, "scaleX": 1, "scaleY": 1, "degrees": -8}, "opacity": .9,
         "clip": {"x": 0, "y": 0, "width": 90, "height": 75}, "children": [
             {"id": "facet", "kind": "polygon", "width": 100, "height": 100,
              "transform": {"scaleX": 1, "scaleY": 1}, "opacity": .8,
              "fill": {"kind": "solid", "color": "#FFF4D5"}, "points": [{"x": 10, "y": 90}, {"x": 50, "y": 0}, {"x": 100, "y": 90}]}]}]}


def tiny_png():
    def chunk(tag, data):
        return struct.pack('>I', len(data)) + tag + data + struct.pack('>I', zlib.crc32(tag + data))
    return b'\x89PNG\r\n\x1a\n' + chunk(b'IHDR', struct.pack('>IIBBBBB', 2, 2, 8, 6, 0, 0, 0)) + chunk(b'IDAT', zlib.compress(b'\0' + bytes([240, 40, 80, 255] * 2) + b'\0' + bytes([40, 80, 240, 128] * 2))) + chunk(b'IEND', b'')


class Demo:
    def __init__(self, binary, directory):
        self.binary = binary
        self.directory = directory
        self.proc = None
        self.log = open(directory / 'server.log', 'ab')
        self.start()

    def start(self):
        with socket.socket() as sock:
            sock.bind(('127.0.0.1', 0))
            port = sock.getsockname()[1]
        self.url = f'http://127.0.0.1:{port}'
        env = dict(os.environ, ATLAS_ADDR=f'127.0.0.1:{port}', ATLAS_STATE=str(self.directory / 'state.json'))
        self.proc = subprocess.Popen([str(self.binary)], env=env, stdout=self.log, stderr=self.log)
        for _ in range(100):
            try:
                self.call('GET', '/healthz')
                return
            except (urllib.error.URLError, ConnectionError):
                if self.proc.poll() is not None:
                    raise RuntimeError((self.directory / 'server.log').read_text())
                time.sleep(.03)
        raise RuntimeError('server startup deadline exceeded')

    def stop(self, abrupt=False):
        if self.proc and self.proc.poll() is None:
            (self.proc.kill if abrupt else self.proc.terminate)()
            self.proc.wait(timeout=15)

    def call(self, method, path, body=None, status=200, headers=None):
        hdr = dict(headers or {})
        if isinstance(body, (dict, list)):
            body = json.dumps(body).encode()
            hdr['Content-Type'] = 'application/json'
        request = urllib.request.Request(self.url + path, data=body, method=method, headers=hdr)
        try:
            response = urllib.request.urlopen(request, timeout=15)
        except urllib.error.HTTPError as error:
            response = error
        with response:
            data = response.read()
            if response.status != status:
                raise RuntimeError(f'{method} {path}: expected {status}, received {response.status}: {data[:500]!r}')
            if response.headers.get('Content-Type', '').startswith('application/json'):
                return json.loads(data)
            return data

    def wait(self, job_id, wanted=('succeeded',)):
        deadline = time.monotonic() + 20
        while time.monotonic() < deadline:
            job = self.call('GET', '/v1/jobs/' + job_id)
            if job['state'] in wanted:
                return job
            if job['state'] in ('failed', 'cancelled'):
                raise RuntimeError(f'unexpected job outcome: {job}')
            time.sleep(.025)
        raise RuntimeError('render deadline exceeded')

    def submit(self, snapshot, key='render-one', scale=1):
        return self.call('POST', '/v1/jobs', {'snapshotId': snapshot['id'], 'scale': scale}, 202, {'Idempotency-Key': key})

    def status(self):
        return self.call('GET', '/v1/status')

    def waiting_running(self, job_id):
        """Wait until the executor has claimed the job and registered its attempt."""
        deadline = time.monotonic() + 20
        while time.monotonic() < deadline:
            job = self.call('GET', '/v1/jobs/' + job_id)
            if job['state'] == 'running' and self.status()['activeExecutions'] > 0:
                return job
            if job['state'] in ('succeeded', 'failed', 'cancelled'):
                raise RuntimeError(f'unexpected job outcome: {job}')
            time.sleep(.005)
        raise RuntimeError('render did not start')

    def quiet(self):
        """Wait until this process owns no execution handle."""
        deadline = time.monotonic() + 20
        while time.monotonic() < deadline:
            if self.status()['activeExecutions'] == 0:
                return
            time.sleep(.005)
        raise RuntimeError('execution handles were not released')

    def durable(self):
        """Read committed state from disk; only call once the server is stopped."""
        return json.loads((self.directory / 'state.json').read_text())


def run(demo, workflow, out):
    d = sample()
    put = lambda doc, rev=0: demo.call('PUT', '/v1/scenes/emblem', {'name': '双色印记', 'expectedRevision': rev, 'document': doc})
    scene = put(d)
    if workflow == 'create-scene':
        assert demo.call('GET', '/v1/scenes/emblem')['document'] == scene['document']
        bad = sample(); bad['layers'][0]['transform']['scaleX'] = 0
        demo.call('PUT', '/v1/scenes/invalid', {'name': '无效几何', 'expectedRevision': 0, 'document': bad}, 400)
    elif workflow == 'revise-scene':
        d['background'] = '#F0F0F0'; assert put(d, 1)['revision'] == 2
        demo.call('PUT', '/v1/scenes/emblem', {'name': '旧版本', 'expectedRevision': 1, 'document': d}, 409)
    elif workflow == 'archive-scene':
        archived = demo.call('POST', '/v1/scenes/emblem/state', {'expectedRevision': 1, 'state': 'archived'})
        demo.call('POST', '/v1/scenes/emblem/snapshots', {'expectedRevision': archived['revision']}, 409)
        assert demo.call('POST', '/v1/scenes/emblem/state', {'expectedRevision': archived['revision'], 'state': 'active'})['revision'] == 3
    if workflow in ('import-asset', 'retire-asset'):
        asset = demo.call('PUT', '/v1/assets/ribbon', tiny_png(), 201)
        assert asset['width'] == 2 and asset['digest'] == hashlib.sha256(tiny_png()).hexdigest()
        demo.call('PUT', '/v1/assets/broken', b'not png', 400)
        layer = {'id': 'ribbon', 'kind': 'image', 'width': 60, 'height': 60, 'assetId': 'ribbon', 'opacity': 1, 'transform': {'x': 20, 'y': 30, 'scaleX': 1, 'scaleY': 1}}
        d['layers'].append(layer); put(d, 1)
        pinned = demo.call('POST', '/v1/scenes/emblem/snapshots', {'expectedRevision': 2}, 201)
        demo.call('POST', '/v1/assets/ribbon/retire')
        demo.call('POST', '/v1/scenes/emblem/snapshots', {'expectedRevision': 2}, 409)
        result = demo.wait(demo.submit(pinned)['id']); assert result['state'] == 'succeeded'
        return {'workflow': workflow, 'assetDigest': asset['digest'], 'pinnedRender': result['state']}
    current = demo.call('GET', '/v1/scenes/emblem')
    snapshot = demo.call('POST', '/v1/scenes/emblem/snapshots', {'expectedRevision': current['revision']}, 201)
    if workflow == 'freeze-scene':
        original_digest = snapshot['digest']; d['background'] = '#000000'; put(d, current['revision'])
        assert demo.call('GET', '/v1/snapshots/' + snapshot['id'])['digest'] == original_digest
    if workflow in ('export-package', 'import-package'):
        package = demo.call('GET', '/v1/snapshots/' + snapshot['id'] + '/package')
        restored = demo.call('POST', '/v1/imports/restored', {'name': '恢复的印记', 'package': package}, 201)
        assert restored['document'] == snapshot['document']
        corrupt = json.loads(json.dumps(package)); corrupt['snapshot']['document']['width'] += 1
        demo.call('POST', '/v1/imports/corrupt', {'name': '损坏包', 'package': corrupt}, 400)
        demo.call('GET', '/v1/scenes/corrupt', status=404)
    if workflow in ('cancel-render', 'retry-render', 'recover-render', 'attempt-ownership', 'state-compat'):
        heavy = sample(); heavy['width'] = 1500; heavy['height'] = 1500
        heavy['layers'] = [dict(heavy['layers'][0], id=f'sheet-{i}', width=1500, height=1500, transform={'scaleX': 1, 'scaleY': 1}) for i in range(16)]
        put(heavy, current['revision'])
        snapshot = demo.call('POST', '/v1/scenes/emblem/snapshots', {'expectedRevision': current['revision'] + 1}, 201)
    job = demo.submit(snapshot)
    if workflow == 'attempt-ownership':
        # Attempt 1 owns a live execution handle. Cancel commits, then a retry
        # immediately reopens the job as attempt 2 while attempt 1 is still
        # unwinding. Only attempt 2 may publish; only its handle may survive.
        demo.waiting_running(job['id'])
        assert demo.status()['activeExecutions'] == 1
        stopped = demo.call('POST', '/v1/jobs/' + job['id'] + '/cancel')
        assert stopped['state'] == 'cancelled' and stopped['attempt'] == 1
        retried = demo.call('POST', '/v1/jobs/' + job['id'] + '/retry', status=202)
        assert retried['attempt'] == 2 and retried['state'] == 'queued'
        assert demo.status()['activeExecutions'] <= 1
        first = demo.wait(job['id'])
        assert first['attempt'] == 2
        png = demo.call('GET', '/v1/artifacts/' + first['artifactId'])
        assert struct.unpack('>II', png[16:24]) == (snapshot['document']['width'], snapshot['document']['height'])
        demo.quiet(); assert demo.status()['activeExecutions'] == 0
        # Second job exercises orderly shutdown: the process releases its
        # handle without committing, and the restart resumes the durable fact.
        second = demo.submit(snapshot, 'render-two')
        demo.waiting_running(second['id'])
        demo.stop()
        state = demo.durable()
        interrupted = state['jobs'][second['id']]
        assert interrupted['state'] == 'running' and interrupted['attempt'] == 1
        assert 'payload' not in interrupted
        assert not any(a['jobId'] == second['id'] for a in state['artifacts'].values())
        assert 'payload' not in state['jobs'][job['id']]
        demo.start()
        recovered = demo.wait(second['id'])
        assert recovered['attempt'] == 1
        assert any(e['kind'] == 'job.recovered' and e['subject'] == second['id'] for e in demo.call('GET', '/v1/events'))
        assert demo.call('POST', '/v1/compare', {'left': first['artifactId'], 'right': recovered['artifactId']})['changedPixels'] == 0
        demo.stop()
        state = demo.durable()
        counts = {job['id']: 0, second['id']: 0}
        for a in state['artifacts'].values():
            if a['jobId'] in counts:
                counts[a['jobId']] += 1
        assert counts == {job['id']: 1, second['id']: 1}, counts
        assert state['jobs'][job['id']]['attempt'] == 2 and state['jobs'][second['id']]['attempt'] == 1
        return {'workflow': workflow, 'firstAttempt': 2, 'recoveredState': recovered['state'],
                'artifactsPerJob': counts, 'executionsAfterShutdown': 0}
    if workflow == 'submit-render':
        assert demo.submit(snapshot)['id'] == job['id']
        demo.call('POST', '/v1/jobs', {'snapshotId': snapshot['id'], 'scale': 2}, 409, {'Idempotency-Key': 'render-one'})
    if workflow == 'revoke-snapshot':
        demo.call('POST', '/v1/snapshots/' + snapshot['id'] + '/revoke')
        demo.call('POST', '/v1/jobs', {'snapshotId': snapshot['id'], 'scale': 1}, 409, {'Idempotency-Key': 'blocked-after-revoke'})
    if workflow in ('cancel-render', 'retry-render'):
        # Cancel only after the executor registered the attempt handle, so the
        # abort really has to cross from durable commit to executor resource.
        demo.waiting_running(job['id'])
        stopped = demo.call('POST', '/v1/jobs/' + job['id'] + '/cancel')
        assert stopped['state'] == 'cancelled'
        demo.quiet()
        if workflow == 'cancel-render':
            time.sleep(.1)
            assert demo.call('GET', '/v1/jobs/' + job['id'])['state'] == 'cancelled'
            assert demo.status()['activeExecutions'] == 0
            return {'workflow': workflow, 'state': 'cancelled'}
        job = demo.call('POST', '/v1/jobs/' + job['id'] + '/retry', status=202)
        assert job['attempt'] == 2
    if workflow == 'recover-render':
        demo.waiting_running(job['id']); demo.stop(abrupt=True); demo.start()
    if workflow == 'state-compat':
        # Durable files written by the earlier layout carry a full input copy
        # per job. Such a file must still open and serve the documented job
        # detail shape, and both finished and retryable facts must survive.
        done = demo.wait(job['id'])
        assert done['state'] == 'succeeded'
        pending = demo.submit(snapshot, 'render-two')
        demo.waiting_running(pending['id'])
        assert demo.call('POST', '/v1/jobs/' + pending['id'] + '/cancel')['state'] == 'cancelled'
        demo.quiet(); demo.stop()
        state = demo.durable()
        for stored in state['jobs'].values():
            stored['payload'] = snapshot
        (demo.directory / 'state.json').write_text(json.dumps(state))
        demo.start()
        for job_id, want in ((job['id'], 'succeeded'), (pending['id'], 'cancelled')):
            detail = demo.call('GET', '/v1/jobs/' + job_id)
            assert detail['state'] == want and detail['snapshotId'] == snapshot['id']
            assert set(detail) <= {'id', 'snapshotId', 'state', 'scale', 'attempt', 'createdAt', 'updatedAt', 'payload', 'error', 'artifactId'}
            assert {'id', 'snapshotId', 'state', 'scale', 'attempt', 'createdAt', 'updatedAt', 'payload'} <= set(detail)
            assert detail['payload']['id'] == ''
        png = demo.call('GET', '/v1/artifacts/' + done['artifactId'])
        assert hashlib.sha256(png).hexdigest() == state['artifacts'][done['artifactId']]['digest']
        retried = demo.call('POST', '/v1/jobs/' + pending['id'] + '/retry', status=202)
        assert retried['attempt'] == 2 and retried['snapshotId'] == snapshot['id']
        result = demo.wait(pending['id']); assert result['state'] == 'succeeded'
        demo.stop()
        rewritten = demo.durable()
        assert all('payload' not in stored for stored in rewritten['jobs'].values())
        return {'workflow': workflow, 'state': result['state'], 'legacyFileAccepted': True}
    result = demo.wait(job['id'])
    png = demo.call('GET', '/v1/artifacts/' + result['artifactId'])
    assert png[:8] == b'\x89PNG\r\n\x1a\n'
    assert struct.unpack('>II', png[16:24]) == (snapshot['document']['width'], snapshot['document']['height'])
    if out:
        Path(out).mkdir(parents=True, exist_ok=True); (Path(out) / 'atlas-sample.png').write_bytes(png)
    if workflow == 'compare-renders':
        other = demo.wait(demo.submit(snapshot, 'render-two')['id'])
        comparison = demo.call('POST', '/v1/compare', {'left': result['artifactId'], 'right': other['artifactId']})
        assert comparison['changedPixels'] == 0
        d['background'] = '#000000'; put(d, current['revision'])
        altered = demo.call('POST', '/v1/scenes/emblem/snapshots', {'expectedRevision': current['revision'] + 1}, 201)
        changed = demo.wait(demo.submit(altered, 'render-three')['id'])
        assert demo.call('POST', '/v1/compare', {'left': result['artifactId'], 'right': changed['artifactId']})['changedPixels'] > 0
    if workflow == 'purge-artifact':
        demo.call('DELETE', '/v1/artifacts/' + result['artifactId'])
        demo.call('GET', '/v1/artifacts/' + result['artifactId'], status=410)
        assert demo.call('GET', '/v1/jobs/' + job['id'])['state'] == 'succeeded'
    if workflow == 'observe-service':
        summary = demo.call('GET', '/v1/status'); assert summary['jobs']['succeeded'] >= 1
        events = demo.call('GET', '/v1/events'); assert any(e['kind'] == 'job.succeeded' for e in events)
    return {'workflow': workflow, 'state': result['state'], 'pngBytes': len(png), 'pngDigest': hashlib.sha256(png).hexdigest()}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--workflow', default='submit-render')
    parser.add_argument('--out')
    args = parser.parse_args()
    with tempfile.TemporaryDirectory(prefix='atlas-walkthrough-') as temp:
        directory = Path(temp); binary = directory / 'atlas'
        subprocess.run(['go', 'build', '-o', str(binary), './cmd/atlas'], cwd=ROOT, check=True, timeout=60)
        demo = Demo(binary, directory)
        try:
            print(json.dumps(run(demo, args.workflow, args.out), ensure_ascii=False))
        finally:
            demo.stop(); demo.log.close()


if __name__ == '__main__':
    main()
