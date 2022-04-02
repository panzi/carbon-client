import { describe, it, expect, test, beforeAll } from '@jest/globals';
import { Socket as NetSocket, Server } from 'net';
import { Socket as DgramSocket, createSocket as createDgramSocket } from 'dgram';
import MockDate from 'mockdate';
import { CarbonClient, CarbonClientOptions, DEFAULT_PORT, DEFAULT_TRANSPORT, IPTransport, MetricMap, MetricParams, MetricTuple } from '../src';
import { resolve } from 'path';
import { tmpdir } from 'os';
import { createWriteStream, WriteStream } from 'fs';
import * as fs from 'fs/promises';

const DATE_TIME_0_STR = '2022-04-01T00:00:00+0200';
const DATE_TIME_1_STR = '2022-04-01T01:00:00+0200';
const DATE_TIME_2_STR = '2022-04-01T02:00:00+0200';
const MOCK_DATE_TIME = new Date(DATE_TIME_0_STR);

beforeAll(() => {
    MockDate.set(MOCK_DATE_TIME);
});

afterAll(() => {
    MockDate.reset();
});

describe('Initialize Client', () => {
    const host = 'localhost';
    const port = 3004;

    test('defaults', () => {
        for (const autoReconnect of [true, false]) {
            let client = autoReconnect ?
                new CarbonClient(host, port, undefined, autoReconnect) :
                new CarbonClient(host);

            expect(client.address).toBe(host);
            expect(client.port).toBe(autoReconnect ? port : DEFAULT_PORT);
            expect(client.transport).toBe(DEFAULT_TRANSPORT);
            expect(client.autoReconnect).toBe(autoReconnect);

            client = autoReconnect ?
                new CarbonClient({
                    address: host,
                    // default port
                    autoReconnect,
                }) :
                new CarbonClient({
                    address: host,
                    port,
                    // default autoReconnect
                });

            expect(client.address).toBe(host);
            expect(client.port).toBe(autoReconnect ? DEFAULT_PORT : port);
            expect(client.transport).toBe(DEFAULT_TRANSPORT);
            expect(client.autoReconnect).toBe(autoReconnect);
        }
    });

    for (const transport of ['TCP', 'UDP'] as IPTransport[]) {
        test(transport, () => {
            for (const autoReconnect of [true, false]) {
                let client = new CarbonClient(host, port, transport, autoReconnect);

                expect(client.address).toBe(host);
                expect(client.port).toBe(port);
                expect(client.transport).toBe(transport);
                expect(client.autoReconnect).toBe(autoReconnect);

                client = new CarbonClient({
                    address: host,
                    port,
                    autoReconnect,
                    transport,
                });

                expect(client.address).toBe(host);
                expect(client.port).toBe(port);
                expect(client.transport).toBe(transport);
                expect(client.autoReconnect).toBe(autoReconnect);
            }
        });
    }

    test('IPC', () => {
        const path = '/tmp/namedpipe';
        for (const autoReconnect of [true, false]) {
            let client = new CarbonClient(path, 'IPC', autoReconnect);

            expect(client.address).toBe(path);
            expect(client.port).toBe(-1);
            expect(client.transport).toBe('IPC');
            expect(client.autoReconnect).toBe(autoReconnect);

            client = new CarbonClient({
                address: path,
                autoReconnect,
                transport: 'IPC',
            });

            expect(client.address).toBe(path);
            expect(client.port).toBe(-1);
            expect(client.transport).toBe('IPC');
            expect(client.autoReconnect).toBe(autoReconnect);
        }
    });
});

function receive(server: Server): Promise<Buffer> {
    return new Promise<Buffer>((resolve, reject) => {
        server.once('error', reject);

        server.once('connection', socket => {
            const buf: Buffer[] = [];

            socket.on('data', data => {
                buf.push(data);
            });

            const onerror = (error: Error) => {
                reject(error);
                if (!socket.destroyed) {
                    socket.destroy(error);
                }
            };

            socket.once('end', () => {
                resolve(Buffer.concat(buf));
                server.off('error', reject);
                socket.off('error', onerror);
                if (!socket.destroyed) {
                    socket.destroy();
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

function waitEvent(client: CarbonClient, event: 'connect'|'close'|'error'): Promise<void> {
    return new Promise<void>((resolve, reject) => {
        const handler = () => {
            client.off(event, handler);
            resolve();
        };

        client.on(event, handler);
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

function isNodeError<T extends ErrorConstructor>(error: unknown, errorType: T): error is InstanceType<T> & NodeJS.ErrnoException {
    return error instanceof errorType;
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

describe('Connect', () => {
    const port = 2222;
    const pipe = resolve(tmpdir(), `carbon-client.test.${process.pid}.connect.pipe`);
    const file = resolve(tmpdir(), `carbon-client.test.${process.pid}.connect.file`);

    test('TCP', async () => {
        const server = new Server();
        try {
            await listenTCP(server, port, 'localhost');
            const promise = connect(server);

            const client = new CarbonClient('localhost', port, 'TCP');
            const isConnected = expectEvent(client, 'connect');
            const whenClosed = waitEvent(client, 'close');
            await client.connect();
            expect(isConnected()).toBe(true);
            (await promise).destroy();
            await client.disconnect();
            await whenClosed;
        } finally {
            if (server.listening) {
                await close(server);
            }
        }
    });

    test('UDP', async () => {
        const client = new CarbonClient('localhost', port, 'UDP');
        const isConnected = expectEvent(client, 'connect');
        const whenClosed = waitEvent(client, 'close');
        await client.connect();
        expect(isConnected()).toBe(true);
        await client.disconnect();
        await whenClosed;
    });

    test('IPC', async () => {
        const server = new Server();
        try {
            await listenIPC(server, pipe);
            const promise = connect(server);

            const client = new CarbonClient(pipe, 'IPC');
            const isConnected = expectEvent(client, 'connect');
            const whenClosed = waitEvent(client, 'close');
            await client.connect();
            expect(isConnected()).toBe(true);
            (await promise).destroy();
            await client.disconnect();
            await whenClosed;
        } finally {
            if (server.listening) {
                await close(server);
            }
            await unlinkIfExists(pipe);
        }
    });
});

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

describe('Write Metrics', () => {
    const port = 2222;
    const pipe = resolve(tmpdir(), `carbon-client.test.${process.pid}.write.pipe`);
    const file = resolve(tmpdir(), `carbon-client.test.${process.pid}.write.file`);

    const metrics: MetricTuple[] = [
        ['host.server.key1', +123.456, new Date(DATE_TIME_1_STR), { 'tag.name': 'tag.value' }],
        ['host.server.key2', -123.456, new Date(DATE_TIME_2_STR)],
        ['host.server.key3', Infinity],
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
        `host.server.key1;tag.name=tag.value 123.456 ${unixTime(DATE_TIME_1_STR)}\n`,
        `host.server.key2 -123.456 ${unixTime(DATE_TIME_2_STR)}\n`,
        `host.server.key3 Infinity ${unixTime(DATE_TIME_0_STR)}\n`,
    ];

    const expectedBatch = expectedLines.join('');

    async function doTests(client: CarbonClient, receive: () => Promise<Buffer>): Promise<void> {
        let promise = receive();
        let whenClosed = waitEvent(client, 'close');

        await client.connect();
        await client.write(metrics[0][0], metrics[0][1], metrics[0][2] as Date, metrics[0][3]);
        await client.disconnect();
        expect((await promise).toString()).toBe(expectedLines[0]);
        await whenClosed;

        whenClosed = waitEvent(client, 'close');
        promise = receive();
        await client.connect();
        await client.batchWrite(metrics);
        await client.disconnect();
        expect((await promise).toString()).toBe(expectedBatch);
        await whenClosed;

        whenClosed = waitEvent(client, 'close');
        promise = receive();
        await client.connect();
        await client.batchWrite(metricsMap);
        await client.disconnect();
        expect((await promise).toString()).toBe(expectedBatch);
        await whenClosed;
    }

    test('TCP', async () => {
        const server = new Server();
        try {
            await listenTCP(server, port, 'localhost');
            const client = new CarbonClient('localhost', port, 'TCP');

            await doTests(client, () => receive(server));
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
            const client = new CarbonClient('localhost', port, 'UDP');

            await doTests(client, () => receiveMessage(server));
        } finally {
            await close(server);
        }
    });

    test('IPC', async () => {
        const server = new Server();
        try {
            await listenIPC(server, pipe);
            const client = new CarbonClient(pipe, 'IPC');

            await doTests(client, () => receive(server));
        } finally {
            if (server.listening) {
                await close(server);
            }
            await unlinkIfExists(pipe);
        }
    });
});
