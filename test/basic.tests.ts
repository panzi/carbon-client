import { describe, expect, test, beforeAll, afterAll } from '@jest/globals';
import { Socket as NetSocket, Server } from 'net';
import { Socket as DgramSocket, createSocket as createDgramSocket } from 'dgram';
import MockDate from 'mockdate';
import { CarbonClient, EventMap, DEFAULT_PORT, DEFAULT_TRANSPORT, IPTransport, MetricMap, MetricParams, MetricTuple } from '../src';
import { resolve } from 'path';
import { tmpdir } from 'os';
import * as fs from 'fs/promises';

const DATE_TIME_0_STR = '2022-04-01T00:00:00+0200';
const DATE_TIME_1_STR = '2022-04-01T01:00:00+0200';
const DATE_TIME_2_STR = '2022-04-01T02:00:00+0200';
const MOCK_DATE_TIME = new Date(DATE_TIME_0_STR);

function sleep(milliseconds: number): Promise<void> {
    return new Promise((resolve, reject) => {
        setTimeout(resolve, milliseconds);
    });
}

function receive(server: Server): Promise<Buffer> {
    return new Promise<Buffer>((resolve, reject) => {
        server.once('error', reject);

        server.once('connection', socket => {
            const buf: Buffer[] = [];
            let finished = false;

            socket.on('data', data => {
                buf.push(data);
            });

            const onerror = (error: Error) => {
                reject(error);
                finished = true;
                if (!socket.destroyed) {
                    socket.destroy(error);
                }
            };

            socket.once('end', () => {
                resolve(Buffer.concat(buf));
                finished = true;
                server.off('error', reject);
                socket.off('error', onerror);
                if (!socket.destroyed) {
                    socket.destroy();
                }
            });

            socket.once('close', () => {
                if (!finished) {
                    server.off('error', reject);
                    socket.off('error', onerror);
                    reject(new Error('connection closed before end event'));
                    finished = true;
                }
            });

            socket.once('error', onerror);
        });
    });
}

function connect(server: Server): Promise<NetSocket> {
    return new Promise<NetSocket>((resolve, reject) => {
        server.once('error', reject);

        server.once('connection', socket => {
            resolve(socket);
            server.off('error', reject);
        });
    });
}

function close(server: Server|DgramSocket): Promise<void> {
    if (server instanceof Server) {
        server.on('connection', socket => {
            socket.destroy();
        });
    }

    return new Promise<void>((resolve, reject) => {
        server.close(error => error ? reject(error) : resolve());
    });
}

function expectEvent(client: CarbonClient, event: 'connect'|'close'|'error'): () => boolean {
    let fired = false;
    const handler = () => {
        fired = true;
        client.off(event, handler);
    };
    client.on(event, handler);
    return () => {
        return fired;
    };
}

type Arg0<F extends Function> =
    F extends (arg0: infer T) => unknown ? T :
    F extends () => unknown ? void : never;

function waitEvent<Event extends keyof EventMap>(client: CarbonClient, event: Event): Promise<Arg0<EventMap[Event]>> {
    return new Promise<Arg0<EventMap[Event]>>((resolve, reject) => {
        client.once(event, resolve as any); // XXX: don't know why type check fails here
    });
}

function listenIPC(server: Server, path: string): Promise<void> {
    return new Promise<void>((resolve, reject) => {
        server.once('error', reject);

        server.listen(path, () => {
            server.off('error', reject);
            resolve();
        });
    });
}

function listenTCP(server: Server, port: number, host?: string): Promise<void> {
    return new Promise<void>((resolve, reject) => {
        server.once('error', reject);

        server.listen(port, host, 1024, () => {
            server.off('error', reject);
            resolve();
        });
    });
}

async function unlinkIfExists(path: string): Promise<void> {
    try {
        await fs.unlink(path);
    } catch (error) {
        // XXX: error is not an instance of Error here!?! How?
        if ((error as any)?.code !== 'ENOENT') {
            throw error;
        }
    }
}

function unixTime(isoDatetime: string): number {
    const ms = new Date(isoDatetime).getTime();
    if (isNaN(ms)) {
        throw new TypeError(`illegal ISO data-time string: ${JSON.stringify(isoDatetime)}`);
    }
    return ms / 1000;
}

function bind(socket: DgramSocket, port: number, host?: string): Promise<void> {
    return new Promise<void>((resolve, reject) => {
        socket.once('error', reject);

        socket.bind(port, host, () => {
            socket.off('error', reject);
            resolve();
        });
    });
}

function receiveMessage(socket: DgramSocket): Promise<Buffer> {
    return new Promise<Buffer>((resolve, reject) => {
        const onmessage = (data: Buffer) => {
            resolve(data);
            socket.off('error', reject);
            socket.off('message', onmessage);
        };

        socket.on('error', reject);
        socket.on('message', onmessage);
    });
}

beforeAll(() => {
    MockDate.set(MOCK_DATE_TIME);
});

afterAll(() => {
    MockDate.reset();
});

describe('Initialize Client', () => {
    const host = 'localhost';
    const port = 3004;

    test('Defaults', () => {
        for (const autoConnect of [true, false]) {
            let client = autoConnect ?
                new CarbonClient(host, port, undefined, autoConnect) :
                new CarbonClient(host);

            expect(client.address).toBe(host);
            expect(client.port).toBe(autoConnect ? port : DEFAULT_PORT);
            expect(client.transport).toBe(DEFAULT_TRANSPORT);
            expect(client.autoConnect).toBe(autoConnect);

            client = autoConnect ?
                new CarbonClient({
                    address: host,
                    // default port
                    autoConnect,
                }) :
                new CarbonClient({
                    address: host,
                    port,
                    // default autoConnect
                });

            expect(client.address).toBe(host);
            expect(client.port).toBe(autoConnect ? DEFAULT_PORT : port);
            expect(client.transport).toBe(DEFAULT_TRANSPORT);
            expect(client.autoConnect).toBe(autoConnect);
        }
    });

    for (const transport of ['TCP', 'UDP'] as IPTransport[]) {
        test(transport, () => {
            for (const autoConnect of [true, false]) {
                let client = new CarbonClient(host, port, transport, autoConnect);

                expect(client.address).toBe(host);
                expect(client.port).toBe(port);
                expect(client.transport).toBe(transport);
                expect(client.autoConnect).toBe(autoConnect);

                client = new CarbonClient({
                    address: host,
                    port,
                    autoConnect,
                    transport,
                });

                expect(client.address).toBe(host);
                expect(client.port).toBe(port);
                expect(client.transport).toBe(transport);
                expect(client.autoConnect).toBe(autoConnect);
            }
        });
    }

    test('IPC', () => {
        const path = '/tmp/namedpipe';
        for (const autoConnect of [true, false]) {
            let client = new CarbonClient(path, 'IPC', autoConnect);

            expect(client.address).toBe(path);
            expect(client.port).toBe(-1);
            expect(client.transport).toBe('IPC');
            expect(client.autoConnect).toBe(autoConnect);

            client = new CarbonClient({
                address: path,
                autoConnect,
                transport: 'IPC',
            });

            expect(client.address).toBe(path);
            expect(client.port).toBe(-1);
            expect(client.transport).toBe('IPC');
            expect(client.autoConnect).toBe(autoConnect);
        }
    });
});

describe('Illegal Values', () => {
    const port = 2222;

    const controlChars = [
        '\0', '\x01', '\x02', '\x03', '\x04', '\x05', '\x06', '\x07', '\x0C',
        '\x0E', '\x0F', '\x10', '\x11', '\x12', '\x13', '\x14', '\x15', '\x16',
        '\x17', '\x18', '\x19', '\x1A', '\x1B', '\x1C', '\x1D', '\x1E', '\x1F',
        '\x7f',
    ]

    const illegalPathChars = [
        ' ', '\n', '/', '\\', '\t', '\r', '\v', '\t', '!', '?', '*', '%', '%a', '%zz',
        '(', ')', '[', ']', '{', '}', '<', '>', '|', '"', "'", '^', '~', ',', ':', ';',
        '=', '`', '#', 'Ä',
        ...controlChars,
    ];

    const illegalPaths = [
        '',
        '.foo',
        'foo.',
        'foo..bar',
        ...illegalPathChars,
    ];

    const illegalPrefixes = [
        '.foo',
        'foo..bar',
        ...illegalPathChars,
    ];

    const illegalTagValues = [
        '',
        '%', '%a', '%zz', ';', '"', "'", '~',
        ...controlChars,
    ];

    const illegalPorts = [
        0,
        -0.1,
        -2003,
        1.01,
        NaN,
        Infinity,
        -Infinity,
        65_536,
    ];

    const illegalSendBufferSizes = [
        -10,
        1.1,
        NaN,
        Infinity,
        -Infinity,
    ];

    const illegalUdpSendBufferSizes = [
        -10,
        0,
        1.1,
        NaN,
        Infinity,
        -Infinity,
    ];

    const illegalSendIntervals = [
        -10,
        NaN,
        Infinity,
        -Infinity,
    ];

    const illegalRetryOnErrorValues = [
        -10,
        NaN,
        Infinity,
        -Infinity,
    ];

    const illegalRetryTimeouts = [
        -10,
        NaN,
        Infinity,
        -Infinity,
    ];

    test('Illegal Ports', () => {
        for (const illegalPort of illegalPorts) {
            expect(() => new CarbonClient('localhost', illegalPort)).toThrow(`illegal port number: ${illegalPort}`);
        }
    });

    test('Illegal Send Buffer Sizes', () => {
        for (const sendBufferSize of illegalSendBufferSizes) {
            expect(() => new CarbonClient({ address: 'localhost', sendBufferSize })).toThrow(`illegal sendBufferSize: ${sendBufferSize}`);
        }
    });

    test('Illegal Send Intervals', () => {
        for (const sendInterval of illegalSendIntervals) {
            expect(() => new CarbonClient({ address: 'localhost', sendInterval })).toThrow(`illegal sendInterval: ${sendInterval}`);
        }
    });

    test('Illegal UDP Send Buffer Sizes', () => {
        for (const udpSendBufferSize of illegalUdpSendBufferSizes) {
            expect(() => new CarbonClient({ address: 'localhost', transport: 'UDP', udpSendBufferSize })).toThrow(`illegal udpSendBufferSize: ${udpSendBufferSize}`);
        }
    });

    test('Illegal Paths', async () => {
        const client = new CarbonClient('localhost', port, 'UDP', true);

        try {
            for (const illegalPath of illegalPaths) {
                await expect(client.write(illegalPath, -1)).rejects.toMatchObject({
                    message: `illegal path: ${JSON.stringify(illegalPath)}`
                });

                await expect(client.batchWrite([[illegalPath, -1]])).rejects.toMatchObject({
                    message: `illegal path: ${JSON.stringify(illegalPath)}`
                });

                await expect(client.batchWrite({ [illegalPath]: -1 })).rejects.toMatchObject({
                    message: `illegal path: ${JSON.stringify(illegalPath)}`
                });
            }
        } finally {
            await client.disconnect();
        }
    });

    test('Illegal Prefixes', async () => {
        for (const illegalPrefix of illegalPrefixes) {
            expect(() => new CarbonClient({
                address: 'localhost',
                transport: 'UDP',
                prefix: illegalPrefix,
            })).toThrow(`illegal prefix: ${JSON.stringify(illegalPrefix)}`);
        }
    });

    test('Illegal Dates', async () => {
        const client = new CarbonClient('localhost', port, 'UDP', true);
        const invalidDate = new Date('Invalid Date');

        try {
            await expect(client.write('a', -1, invalidDate)).rejects.toMatchObject({
                message: `illegal date: Invalid Date`
            });

            await expect(client.batchWrite([], invalidDate)).rejects.toMatchObject({
                message: `illegal date: Invalid Date`
            });

            await expect(client.batchWrite([['a', -1, invalidDate]])).rejects.toMatchObject({
                message: `illegal date: Invalid Date`
            });

            await expect(client.batchWrite({ a: { value: -1, timestamp: invalidDate } })).rejects.toMatchObject({
                message: `illegal date: Invalid Date`
            });
        } finally {
            await client.disconnect();
        }
    });

    test('Illegal Tag Names', async () => {
        const client = new CarbonClient('localhost', port, 'UDP', true);

        try {
            for (const illegalPath of illegalPaths) {
                const tags = {
                    [illegalPath]: 'a'
                };
                await expect(client.write('a', -1, tags)).rejects.toMatchObject({
                    message: `illegal tag name: ${JSON.stringify(illegalPath)}`
                });

                await expect(client.batchWrite([['a', -1, tags]])).rejects.toMatchObject({
                    message: `illegal tag name: ${JSON.stringify(illegalPath)}`
                });

                await expect(client.batchWrite({ a: { value: -1, tags } })).rejects.toMatchObject({
                    message: `illegal tag name: ${JSON.stringify(illegalPath)}`
                });
            }
        } finally {
            await client.disconnect();
        }
    });

    test('Illegal Tag Values', async () => {
        const client = new CarbonClient('localhost', port, 'UDP', true);

        try {
            for (const illegalTagValue of illegalTagValues) {
                const tags = {
                    a: illegalTagValue
                };
                await expect(client.write('a', -1, tags)).rejects.toMatchObject({
                    message: `illegal tag value: a=${JSON.stringify(illegalTagValue)}`
                });

                await expect(client.batchWrite([['a', -1, tags]])).rejects.toMatchObject({
                    message: `illegal tag value: a=${JSON.stringify(illegalTagValue)}`
                });

                await expect(client.batchWrite({ a: { value: -1, tags } })).rejects.toMatchObject({
                    message: `illegal tag value: a=${JSON.stringify(illegalTagValue)}`
                });
            }
        } finally {
            await client.disconnect();
        }
    });

    test('Illegal Retry On Error Values', () => {
        for (const illegalRetryOnError of illegalRetryOnErrorValues) {
            expect(() => new CarbonClient({
                address: 'localhost',
                retryOnError: illegalRetryOnError,
            })).toThrow(`illegal retryOnError: ${illegalRetryOnError}`);
        }
    });

    test('Illegal Retry Timeouts', () => {
        for (const illegalRetryTimeout of illegalRetryTimeouts) {
            expect(() => new CarbonClient({
                address: 'localhost',
                retryTimeout: illegalRetryTimeout,
            })).toThrow(`illegal retryTimeout: ${illegalRetryTimeout}`);
        }
    });
});

describe('Only Connect', () => {
    for (const buffered of [true, false]) {
        describe(buffered ? 'Buffered' : 'Unbuffered', () => {
            const port = 2222;
            const pipe = resolve(tmpdir(), buffered ?
                `carbon-client.test.${process.pid}.connect.pipe` :
                `carbon-client.test.${process.pid}.connect-unbuffered.pipe`);
            const sendBufferSize = buffered ? undefined : 0;

            test('TCP', async () => {
                const server = new Server();
                try {
                    await listenTCP(server, port, 'localhost');
                    const promise = connect(server);

                    const client = new CarbonClient({
                        address: 'localhost',
                        port,
                        transport: 'TCP',
                        sendBufferSize,
                    });
                    const isConnected = expectEvent(client, 'connect');
                    const whenClosed = waitEvent(client, 'close');
                    const connectPromise = client.connect();
                    expect(client.isConnecting).toBe(true);
                    expect(client.isConnected).toBe(false);
                    expect(client.isDisconnected).toBe(false);
                    await connectPromise;
                    expect(isConnected()).toBe(true);
                    expect(client.isConnecting).toBe(false);
                    expect(client.isConnected).toBe(true);
                    expect(client.isDisconnected).toBe(false);
                    (await promise).destroy();
                    await client.disconnect();
                    expect(client.isConnecting).toBe(false);
                    expect(client.isConnected).toBe(false);
                    expect(client.isDisconnected).toBe(true);
                    await whenClosed;
                } finally {
                    if (server.listening) {
                        await close(server);
                    }
                }
            });

            test('UDP', async () => {
                const client = new CarbonClient({
                    address: 'localhost',
                    port,
                    transport: 'UDP',
                    sendBufferSize,
                });
                const isConnected = expectEvent(client, 'connect');
                const whenClosed = waitEvent(client, 'close');
                const connectPromise = client.connect();
                expect(client.isConnecting).toBe(true);
                expect(client.isConnected).toBe(false);
                expect(client.isDisconnected).toBe(false);
                await connectPromise;
                expect(isConnected()).toBe(true);
                expect(client.isConnecting).toBe(false);
                expect(client.isConnected).toBe(true);
                expect(client.isDisconnected).toBe(false);
                await client.disconnect();
                expect(client.isConnecting).toBe(false);
                expect(client.isConnected).toBe(false);
                expect(client.isDisconnected).toBe(true);
                await whenClosed;
            });

            test('IPC', async () => {
                const server = new Server();
                try {
                    await listenIPC(server, pipe);
                    const promise = connect(server);

                    const client = new CarbonClient({
                        address: pipe,
                        transport: 'IPC',
                        sendBufferSize,
                    });
                    const isConnected = expectEvent(client, 'connect');
                    const whenClosed = waitEvent(client, 'close');
                    const connectPromise = client.connect();
                    expect(client.isConnecting).toBe(true);
                    expect(client.isConnected).toBe(false);
                    expect(client.isDisconnected).toBe(false);
                    await connectPromise;
                    expect(isConnected()).toBe(true);
                    expect(client.isConnecting).toBe(false);
                    expect(client.isConnected).toBe(true);
                    expect(client.isDisconnected).toBe(false);
                    (await promise).destroy();
                    await client.disconnect();
                    expect(client.isConnecting).toBe(false);
                    expect(client.isConnected).toBe(false);
                    expect(client.isDisconnected).toBe(true);
                    await whenClosed;
                } finally {
                    if (server.listening) {
                        await close(server);
                    }
                    await unlinkIfExists(pipe);
                }
            });
        });
    }
});

describe('Write Metrics', () => {
    const tagValue = 'tag.value-+@$_.*:,&#|<>[](){}=!?^/\\%12%0A%0f';

    const metrics: MetricTuple[] = [
        ['host.server.key1', +123.456, new Date(DATE_TIME_1_STR), { 'tag.name': tagValue }],
        ['host.server.key-', -123.456, new Date(DATE_TIME_2_STR)],
        ['host.server.key+', Infinity],
        ['host.server.key$', -Infinity],
        ['host.server.key@', NaN],
        ['host.server.%20_', 1.5e100],
    ];

    const metricsMap: MetricMap = metrics.reduce((map: MetricMap, metric) => {
        if (metric.length === 2) {
            map[metric[0]] = metric[1];
        } else {
            const params: MetricParams = {
                value: metric[1],
            };

            const m2 = metric[2];
            if (m2 instanceof Date) {
                params.timestamp = m2;
            } else if (m2) {
                params.tags = m2;
            }

            const m3 = metric[3];
            if (m3) {
                params.tags = m3;
            }

            map[metric[0]] = params;
        }

        return map;
    }, {});

    const expectedLines = [
        `host.server.key1;tag.name=${tagValue} 123.456 ${unixTime(DATE_TIME_1_STR)}\n`,
        `host.server.key- -123.456 ${unixTime(DATE_TIME_2_STR)}\n`,
        `host.server.key+ Infinity ${unixTime(DATE_TIME_0_STR)}\n`,
        `host.server.key$ -Infinity ${unixTime(DATE_TIME_0_STR)}\n`,
        `host.server.key@ NaN ${unixTime(DATE_TIME_0_STR)}\n`,
        `host.server.%20_ 1.5e+100 ${unixTime(DATE_TIME_0_STR)}\n`,
    ];

    const expectedBatch = expectedLines.join('');

    async function testWrite(client: CarbonClient, receive: () => Promise<Buffer>): Promise<void> {
        const errors: Error[] = [];
        client.on('error', error => {
            errors.push(error);
        });
        let promise = receive();
        let whenClosed = waitEvent(client, 'close');

        await client.connect();
        await client.write(metrics[0][0], metrics[0][1], metrics[0][2] as Date, metrics[0][3]);
        if (client.isBuffered) {
            await client.write(metrics[1][0], metrics[1][1], metrics[1][2] as Date);
            await client.flush();
        }
        await client.disconnect();
        if (client.isBuffered) {
            expect((await promise).toString()).toBe(expectedLines[0] + expectedLines[1]);
        } else {
            expect((await promise).toString()).toBe(expectedLines[0]);
        }
        await whenClosed;
        expect(errors.length).toStrictEqual(0);

        whenClosed = waitEvent(client, 'close');
        promise = receive();
        await client.connect();
        await client.batchWrite(metrics);
        await client.flush();
        await client.disconnect();
        expect((await promise).toString()).toBe(expectedBatch);
        await whenClosed;
        expect(errors.length).toStrictEqual(0);

        whenClosed = waitEvent(client, 'close');
        promise = receive();
        await client.connect();
        await client.batchWrite(metricsMap);
        await client.flush();
        await client.disconnect();
        expect((await promise).toString()).toBe(expectedBatch);
        await whenClosed;
        expect(errors.length).toStrictEqual(0);
    }

    const extraWait = 100;

    async function testBufferedWrite(client: CarbonClient, receive: () => Promise<Buffer>): Promise<void> {
        const errors: Error[] = [];
        client.on('error', error => {
            errors.push(error);
        });
        let promise = receive();
        let whenClosed = waitEvent(client, 'close');

        await client.connect();
        await client.write(metrics[0][0], metrics[0][1], metrics[0][2] as Date, metrics[0][3]);
        await client.write(metrics[1][0], metrics[1][1], metrics[1][2] as Date);

        expect(client.bufferedBytes).toBeGreaterThan(0);
        await sleep(client.sendInterval + extraWait);
        expect(client.bufferedBytes).toBe(0);

        await client.flush();
        await client.disconnect();
        expect((await promise).toString()).toBe(expectedLines[0] + expectedLines[1]);
        await whenClosed;
        expect(errors.length).toStrictEqual(0);

        whenClosed = waitEvent(client, 'close');
        promise = receive();
        await client.connect();
        await client.batchWrite(metrics);

        expect(client.bufferedBytes).toBeGreaterThan(0);
        await sleep(client.sendInterval + extraWait);
        expect(client.bufferedBytes).toBe(0);

        await client.flush();
        await client.disconnect();
        expect((await promise).toString()).toBe(expectedBatch);
        await whenClosed;
        expect(errors.length).toStrictEqual(0);

        whenClosed = waitEvent(client, 'close');
        promise = receive();
        await client.connect();
        await client.batchWrite(metricsMap);

        expect(client.bufferedBytes).toBeGreaterThan(0);
        await sleep(client.sendInterval + extraWait);
        expect(client.bufferedBytes).toBe(0);

        await client.flush();
        await client.disconnect();
        expect((await promise).toString()).toBe(expectedBatch);
        await whenClosed;
        expect(errors.length).toStrictEqual(0);
    }

    interface TestGroup {
        name: string;
        suffix: string;
        buffered: boolean;
        sendInterval?: number;
        testFunc(client: CarbonClient, receive: () => Promise<Buffer>): Promise<void>;
    }

    const testGroups: TestGroup[] = [
        {
            name: 'Buffered',
            suffix: 'buffered',
            buffered: true,
            testFunc: testWrite,
        },
        {
            name: 'Unbuffered',
            suffix: 'unbuffered',
            buffered: false,
            testFunc: testWrite,
        },
        {
            name: 'Buffered (Wait Before Disconnect)',
            suffix: 'buffered-wait',
            buffered: true,
            sendInterval: 200,
            testFunc: testBufferedWrite,
        },
    ];

    for (const testGroup of testGroups) {
        describe(testGroup.name, () => {
            const port = 2222;
            const pipe = resolve(tmpdir(), `carbon-client.test.${process.pid}.write-${testGroup.suffix}.pipe`);
            const sendBufferSize = testGroup.buffered ? undefined : 0;

            test('TCP', async () => {
                const server = new Server();
                try {
                    await listenTCP(server, port, 'localhost');
                    const client = new CarbonClient({
                        address: 'localhost',
                        port,
                        transport: 'TCP',
                        sendBufferSize,
                        sendInterval: testGroup.sendInterval,
                    });
                    expect(client.isBuffered).toBe(testGroup.buffered);

                    await testGroup.testFunc(client, () => receive(server));
                } finally {
                    if (server.listening) {
                        await close(server);
                    }
                }
            });

            test('UDP', async () => {
                const server = createDgramSocket('udp4');
                try {
                    await bind(server, port, 'localhost')
                    const client = new CarbonClient({
                        address: 'localhost',
                        port,
                        transport: 'UDP',
                        family: 4,
                        sendBufferSize,
                        sendInterval: testGroup.sendInterval,
                    });
                    expect(client.isBuffered).toBe(testGroup.buffered);

                    await testGroup.testFunc(client, () => receiveMessage(server));
                } finally {
                    await close(server);
                }
            });

            test('IPC', async () => {
                const server = new Server();
                try {
                    await listenIPC(server, pipe);
                    const client = new CarbonClient({
                        address: pipe,
                        transport: 'IPC',
                        sendBufferSize,
                        sendInterval: testGroup.sendInterval,
                    });
                    expect(client.isBuffered).toBe(testGroup.buffered);

                    await testGroup.testFunc(client, () => receive(server));
                } finally {
                    if (server.listening) {
                        await close(server);
                    }
                    await unlinkIfExists(pipe);
                }
            });
        })
    }

    for (const prefix of ['foo', 'foo.bar.']) {
        test(`With Prefix ${prefix}`, async () => {
            const server = new Server();
            const port = 2222;
            try {
                await listenTCP(server, port, 'localhost');
                const client = new CarbonClient({
                    address: 'localhost',
                    port,
                    transport: 'TCP',
                    prefix,
                });

                let promise = receive(server);
                let whenClosed = waitEvent(client, 'close');

                await client.connect();
                await client.write('baz', 1, new Date(DATE_TIME_1_STR));
                await client.flush();
                await client.disconnect();
                expect((await promise).toString()).toBe(`${prefix}baz 1 ${unixTime(DATE_TIME_1_STR)}\n`);
                await whenClosed;

                promise = receive(server);
                whenClosed = waitEvent(client, 'close');
                await client.connect();
                await client.batchWrite([
                    ['baz', 1],
                    ['egg.bacon', 2],
                ], new Date(DATE_TIME_1_STR));
                await client.flush();
                await client.disconnect();
                expect((await promise).toString()).toBe(
                    `${prefix}baz 1 ${unixTime(DATE_TIME_1_STR)}\n`+
                    `${prefix}egg.bacon 2 ${unixTime(DATE_TIME_1_STR)}\n`
                );
                await whenClosed;

                promise = receive(server);
                whenClosed = waitEvent(client, 'close');
                await client.connect();
                await client.batchWrite({
                    'baz': 1,
                    'egg.bacon': 2,
                }, new Date(DATE_TIME_1_STR));
                await client.flush();
                await client.disconnect();
                expect((await promise).toString()).toBe(
                    `${prefix}baz 1 ${unixTime(DATE_TIME_1_STR)}\n`+
                    `${prefix}egg.bacon 2 ${unixTime(DATE_TIME_1_STR)}\n`
                );
                await whenClosed;
            } finally {
                if (server.listening) {
                    await close(server);
                }
            }
        });
    }
});

describe('Write/Disconnect While Not Connected', () => {
    async function testDisconnectWhileNotConnected(client: CarbonClient): Promise<void> {
        await expect(client.write('a.b.c', 1)).rejects.toMatchObject({ message: 'not connected' });
        await expect(client.batchWrite([])).rejects.toMatchObject({ message: 'not connected' });
        await expect(client.disconnect()).rejects.toMatchObject({ message: 'not connected' });
        await expect(new Promise<void>((resolve, reject) => {
            const res = client.disconnect(error => error ? reject(error) : resolve());
            if (res !== undefined) {
                reject(new Error(`disconnect didn't return undefined but: ${res}`));
            }
        })).rejects.toMatchObject({ message: 'not connected' });
    }

    describe('Buffered', () => {
        const port = 2222;
        const pipe = resolve(tmpdir(), `carbon-client.test.${process.pid}.not-connected.pipe`);

        test('TCP', async () => {
            const client = new CarbonClient('localhost', port, 'TCP');
            await testDisconnectWhileNotConnected(client);
        });

        test('UDP', async () => {
            const client = new CarbonClient({
                address: 'localhost',
                port,
                transport: 'UDP',
                family: 4,
            });
            await testDisconnectWhileNotConnected(client);
        });

        test('IPC', async () => {
            const client = new CarbonClient(pipe, 'IPC');
            await testDisconnectWhileNotConnected(client);
        });
    });

    describe('Unbuffered', () => {
        const port = 2222;
        const pipe = resolve(tmpdir(), `carbon-client.test.${process.pid}.not-connected-unbuffered.pipe`);
    
        test('TCP', async () => {
            const client = new CarbonClient({
                address: 'localhost',
                port,
                transport: 'TCP',
                sendBufferSize: 0,
            });
            await testDisconnectWhileNotConnected(client);
        });
    
        test('UDP', async () => {
            const client = new CarbonClient({
                address: 'localhost',
                port,
                transport: 'UDP',
                family: 4,
                sendBufferSize: 0,
            });
            await testDisconnectWhileNotConnected(client);
        });
    
        test('IPC', async () => {
            const client = new CarbonClient({
                address: pipe,
                transport: 'IPC',
                sendBufferSize: 0,
            });
            await testDisconnectWhileNotConnected(client);
        });

    })
});

describe('Server Offline', () => {
    for (const buffered of [true, false]) {
        describe(buffered ? 'Buffered' : 'Unbuffered', () => {
            const port = 2222;
            const pipe = resolve(tmpdir(), buffered ?
                `carbon-client.test.${process.pid}.no-server.pipe` :
                `carbon-client.test.${process.pid}.no-server-unbuffered.pipe`);
            const sendBufferSize = buffered ? undefined : 0;

            test('TCP', async () => {
                const client = new CarbonClient({
                    address: 'localhost',
                    port,
                    transport: 'TCP',
                    sendBufferSize,
                });
                await expect(client.connect()).rejects.toMatchObject({
                    code: 'ECONNREFUSED'
                });
            });

            // UDP can of course not detect an offline server

            test('IPC', async () => {
                const client = new CarbonClient({
                    address: pipe,
                    transport: 'IPC',
                    sendBufferSize,
                });
                await expect(client.connect()).rejects.toMatchObject({
                    code: 'ENOENT'
                });

                await fs.writeFile(pipe, '');
                try {
                    await expect(client.connect()).rejects.toMatchObject({
                        code: 'ECONNREFUSED'
                    });
                } finally {
                    await unlinkIfExists(pipe);
                }
            });
        });
    }
});

describe('Retry On Error', () => {
    const key = 'host.server.SUCCESS';
    const value = 123;
    const timestamp = new Date(DATE_TIME_1_STR);
    const expectedLine = `${key} ${value} ${unixTime(DATE_TIME_1_STR)}\n`;

    for (const buffered of [true, false]) {
        describe(buffered ? 'Buffered' : 'Unbuffered', () => {
            const port = 2222;
            const pipe = resolve(tmpdir(), buffered ?
                `carbon-client.test.${process.pid}.no-server.pipe` :
                `carbon-client.test.${process.pid}.no-server-unbuffered.pipe`);
            const sendBufferSize = buffered ? undefined : 0;

            async function doTest(client: CarbonClient, listen: (server: Server) => Promise<void>, errorCode: string): Promise<void> {
                let errors: Error[] = [];
                client.on('error', error => {
                    errors.push(error);
                });
                client.vwrite(key, value, timestamp);

                await sleep(25);

                const server = new Server();
                try {
                    await listen(server);
                    const promise = receive(server);
                    await sleep(25);
                    await client.flush();

                    // Without this sleep the `await promise` below hangs sometimes.
                    await sleep(25);

                    await client.disconnect();
                    expect((await promise).toString()).toStrictEqual(expectedLine);
                } finally {
                    if (server.listening) {
                        await close(server);
                    }
                }

                expect(errors.length).toBeGreaterThan(0);

                if (buffered) {
                    errors = [];
                    client.vwrite('host.server.ERROR', 123);
                    await sleep(25);
                    expect(errors.length).toBeGreaterThan(0);
                    expect(errors[0]).toMatchObject({
                        code: errorCode
                    });
                } else {
                    await expect(client.write('host.server.ERROR', 123)).rejects.toMatchObject({
                        code: errorCode
                    });
                }
                await client.disconnect();
            }

            test('TCP', async () => {
                const client = new CarbonClient({
                    address: 'localhost',
                    port,
                    transport: 'TCP',
                    sendBufferSize,
                    sendInterval: 2,
                    retryOnError: 15,
                    retryTimeout: 15,
                    autoConnect: true,
                });
                await doTest(client, server => listenTCP(server, port, 'localhost'), 'ECONNREFUSED');
            });

            test('IPC', async () => {
                const client = new CarbonClient({
                    address: pipe,
                    transport: 'IPC',
                    sendBufferSize,
                    sendInterval: 2,
                    retryOnError: 15,
                    retryTimeout: 15,
                    autoConnect: true,
                });
                await doTest(client, server => listenIPC(server, pipe), 'ENOENT');
            });
        });
    }
});

describe('Server Disconnect', () => {
    for (const buffered of [true, false]) {
        describe(buffered ? 'Buffered' : 'Unbuffered', () => {
            const port = 2222;
            const pipe = resolve(tmpdir(), buffered ?
                `carbon-client.test.${process.pid}.server-disconnect.pipe` :
                `carbon-client.test.${process.pid}.server-disconnect-unbuffered.pipe`);
            const sendBufferSize = buffered ? undefined : 0;

            async function doTests(client: CarbonClient, createServer: () => Promise<Server>): Promise<void> {
                {
                    const server = await createServer();
                    try {
                        const connections: NetSocket[] = [];
                        let running = true;
                        server.on('connection', connection => {
                            if (running) {
                                connections.push(connection);
                            } else {
                                connection.destroy();
                            }
                        });

                        const willClose = waitEvent(client, 'close');
                        await client.connect();
                        running = false;
                        for (const connection of connections) {
                            connection.destroy();
                        }
                        await close(server);
                        await expect(willClose).resolves.toStrictEqual(false);
                    } finally {
                        if (server.listening) {
                            await close(server);
                        }
                    }
                }
                {
                    const server = await createServer();
                    try {
                        const connections: NetSocket[] = [];
                        let running = true;
                        server.on('connection', connection => {
                            if (running) {
                                connections.push(connection);
                            } else {
                                connection.destroy();
                            }
                        });

                        const willClose = waitEvent(client, 'close');
                        await client.connect();
                        await client.write('a.b.c', 1);
                        await client.flush();
                        running = false;
                        for (const connection of connections) {
                            connection.destroy();
                        }
                        await close(server);
                        await expect(willClose).resolves.toStrictEqual(true);
                    } finally {
                        if (server.listening) {
                            await close(server);
                        }
                    }
                }
            }

            test('TCP', async () => {
                const client = new CarbonClient({
                    address: 'localhost',
                    port,
                    transport: 'TCP',
                    sendBufferSize,
                });

                await doTests(client, async () => {
                    const server = new Server();
                    await listenTCP(server, port, 'localhost');
                    return server;
                });
            });

            test('IPC', async () => {
                const client = new CarbonClient({
                    address: pipe,
                    transport: 'IPC',
                    sendBufferSize,
                });

                await doTests(client, async () => {
                    const server = new Server();
                    await listenIPC(server, pipe);
                    return server;
                });
            });
        });
    }
});

describe('Auto-Connect', () => {
    for (const buffered of [true, false]) {
        describe(buffered ? 'Buffered' : 'Unbuffered', () => {
            const port = 2222;
            const pipe = resolve(tmpdir(), buffered ?
                `carbon-client.test.${process.pid}.auto-connect.pipe` :
                `carbon-client.test.${process.pid}.auto-connect-unbuffered.pipe`);
            const sendBufferSize = buffered ? undefined : 0;

            const metric: MetricTuple = ['host.server.key1', +123.456, new Date(DATE_TIME_1_STR), { 'tag.name': 'tag.value' }];
            const expectedLine = `host.server.key1;tag.name=tag.value 123.456 ${unixTime(DATE_TIME_1_STR)}\n`;

            async function doTests(client: CarbonClient, receive: () => Promise<Buffer>): Promise<void> {
                const promise = receive();
                const whenClosed = waitEvent(client, 'close');

                await client.write(metric[0], metric[1], metric[2] as Date, metric[3]);
                await client.flush();
                await client.disconnect();
                expect((await promise).toString()).toStrictEqual(expectedLine);
                await whenClosed;
            }
            
            async function doReconnectTests(client: CarbonClient, createServer: () => Promise<Server>): Promise<void> {
                let server = await createServer();
                try {
                    let whenClosed = waitEvent(client, 'close');

                    const connections: NetSocket[] = [];
                    server.on('connection', connection => {
                        connections.push(connection);
                    });

                    await client.connect();
                    for (const connection of connections) {
                        connection.destroy();
                    }
                    await whenClosed;
                    const promise = receive(server);
                    whenClosed = waitEvent(client, 'close');

                    await client.write(metric[0], metric[1], metric[2] as Date, metric[3]);
                    await client.flush();
                    await client.disconnect();
                    expect((await promise).toString()).toStrictEqual(expectedLine);
                    await whenClosed;
                } finally {
                    if (server.listening) {
                        await close(server);
                    }
                }
            }

            test('TCP', async () => {
                {
                    const client = new CarbonClient({
                        address: 'localhost',
                        port,
                        transport: 'TCP',
                        autoConnect: true,
                        sendBufferSize,
                    });
                    const server = new Server();
                    try {
                        await listenTCP(server, port, 'localhost');

                        await doTests(client, () => receive(server));
                    } finally {
                        if (server.listening) {
                            await close(server);
                        }
                    }
                }

                const client = new CarbonClient({
                    address: 'localhost',
                    port,
                    transport: 'TCP',
                    autoConnect: true,
                    sendBufferSize,
                });
                await doReconnectTests(client, async () => {
                    const server = new Server();
                    await listenTCP(server, port, 'localhost');
                    return server;
                });
            });

            test('UDP', async () => {
                {
                    const client = new CarbonClient({
                        address: 'localhost',
                        port,
                        transport: 'UDP',
                        family: 4,
                        autoConnect: true,
                        sendBufferSize,
                    });
                    const server = createDgramSocket('udp4');
                    try {
                        await bind(server, port, 'localhost')

                        await doTests(client, () => receiveMessage(server));
                    } finally {
                        await close(server);
                    }
                }

                // disconnecting unconnected client is allowed if autoConnect is true
                const client = new CarbonClient({
                    address: 'localhost',
                    port,
                    transport: 'UDP',
                    family: 4,
                    autoConnect: true,
                    sendBufferSize,
                });
                await client.flush();
                await client.disconnect();

                // UDP cannot connect server disconnect (because there's no such concept)
            });

            test('IPC', async () => {
                {
                    const client = new CarbonClient({
                        address: pipe,
                        transport: 'IPC',
                        autoConnect: true,
                        sendBufferSize,
                    });
                    const server = new Server();
                    try {
                        await listenIPC(server, pipe);

                        await doTests(client, () => receive(server));
                    } finally {
                        if (server.listening) {
                            await close(server);
                        }
                        await unlinkIfExists(pipe);
                    }
                }

                const client = new CarbonClient({
                    address: pipe,
                    transport: 'IPC',
                    autoConnect: true,
                    sendBufferSize,
                });
                await doReconnectTests(client, async () => {
                    const server = new Server();
                    await listenIPC(server, pipe);
                    return server;
                });
            });
        });
    }
});
