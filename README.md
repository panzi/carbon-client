Carbon Client
=============

[![Test Status](https://img.shields.io/github/actions/workflow/status/panzi/carbon-client/test.yml?branch=main)](https://github.com/panzi/carbon-client/actions/workflows/test.yml)
[![License: MIT](https://img.shields.io/github/license/panzi/carbon-client)](https://github.com/panzi/carbon-client/blob/main/LICENSE)
[Documentation](https://panzi.github.io/carbon-client)
[GitHub](https://github.com/panzi/carbon-client/)

[Graphite](https://graphiteapp.org/) [Carbon](https://github.com/graphite-project/carbon)
client for ingesting metrics in TypeScript for NodeJS.

This library supports both TCP and UDP. In addition to these standard ways to
talk to Carbon this library supports Unix domain sockets and TLS connections.

The Carbon protocol is an unencrypted line based plain text protocol, so you
shouldn't let Carbon listen on anything else than localhost. You can secure
your Carbon server by putting it behind a TLS proxy e.g. like this:

```bash
socat \
    openssl-listen:5000,reuseaddr,cert=server-crt.pem,cafile=ca-crt.pem,key=server-key.pem \
    TCP:localhost:2001
```

Then you can connect using a client certificate from another machine like this:

```TypeScript
const client = new CarbonClient({
    address: 'carbon.example.com',
    port: 5000,
    transport: 'TLS',
    tlsCert: fs.readFileSync('client-crt.pem'),
    tlsKey:  fs.readFileSync('client-key.pem'),
    tlsCA:   fs.readFileSync('ca-crt.pem'),
});
```

See e.g. [this](https://gist.github.com/pcan/e384fcad2a83e3ce20f9a4c33f4a13ae#file-readme-md)
for how to generate the appropriate certificate files with OpenSSL.

Features
--------

* TCP, UDP, Unix domain sockets, TLS
* IPv4 and IPv6
* Carbon plain-text protocol
* Validates keys, tag-names, and tag-values.
* Batched writes

### Optional Features

These features are controlled via options.

* Prefix all keys.
* Buffering to a fixed size buffer and sending that buffer at a defined interval.
* Auto-connect on writing metrics, so you don't need to call `connect()` and it
  automatically re-connects if the connection is lost.
* Retry sending on failure with a given retry count and delay.

Examples
--------

```TypeScript
const client = new CarbonClient('localhost', 2003, 'TCP');

client.on('connect', () => {
    console.log('carbon client connected');
});

client.on('error', error => {
    console.error('carbon client error:', error);
});

client.on('close', (hadError: boolean) => {
    console.log('carbon client closed connection, hadError:', hadError);
});

// Not needed if autoConnect is true.
await client.connect();

await client.write('foo.bar.key1', 123.456);
await client.write('foo.bar.key2', -78.90, { tag1: 'value1', tag2: 'value2' });
await client.write('foo.bar.key3', 0, new Date('2022-04-01T15:00:00+0200'));
await client.write('foo.bar.key4', 1, new Date('2022-04-01T16:00:00+0200'), { tag2: 'value2' });

await client.batchWrite([
    ['foo.bar.key1', 123.456],
    ['foo.bar.key2', -78.90, { tag1: 'value1', tag2: 'value2' }],
    ['foo.bar.key3', 0, new Date('2022-04-01T15:00:00+0200')],
    ['foo.bar.key4', 1, new Date('2022-04-01T16:00:00+0200'), { tag2: 'value2' }],
]);

await client.batchWrite({
    'foo.bar.key1': 123.456,
    'foo.bar.key2': { value: -78.90, tags: { tag1: 'value1', tag2: 'value2' } },
    'foo.bar.key3': { value: 0, timestamp: new Date('2022-04-01T15:00:00+0200') }],
    'foo.bar.key4': { value: 1, timestamp: new Date('2022-04-01T16:00:00+0200'), tags: { tag2: 'value2' } }],
});

// If you use a sendBufferSize > 0 (default: 0), flush before
// you disconnect to make sure anything buffered is sent.
await client.flush();

await client.disconnect();
```

If you use UDP keep batch sizes small enough to fit into one UDP packet!

Instead of `write()`/`batchWrite()` you can also use `vwrite()`/`vbatchWrite()`
which won't return promises (v for `void`), but instead if an error is
encountered during sending the error will be dispatched to any registered error
event handlers.
