import { describe, expect, test, beforeAll, afterAll } from '@jest/globals';
import { Socket as NetSocket, Server } from 'net';
import { Socket as DgramSocket, createSocket as createDgramSocket } from 'dgram';
import MockDate from 'mockdate';
import { CarbonClient, EventMap, DEFAULT_PORT, DEFAULT_TRANSPORT, IPTransport, MetricMap, MetricParams, MetricTuple } from '../src';
import { resolve } from 'path';
import { tmpdir } from 'os';
import * as fs from 'fs/promises';
import * as tls from 'tls';

const DATE_TIME_0_STR = '2022-04-01T00:00:00+0200';
const DATE_TIME_1_STR = '2022-04-01T01:00:00+0200';
const DATE_TIME_2_STR = '2022-04-01T02:00:00+0200';
const MOCK_DATE_TIME = new Date(DATE_TIME_0_STR);

// openssl genrsa -out private-key.pem 1024
// openssl req -new -key private-key.pem -out csr.pem
// openssl x509 -req -in csr.pem -signkey private-key.pem -out public-cert.pem

const CLIENT_CERT = `\
-----BEGIN CERTIFICATE-----
MIICDDCCAXUCFB6qpTmOkw3ZbCloDxLgFjh6l0FUMA0GCSqGSIb3DQEBCwUAMEUx
CzAJBgNVBAYTAkFVMRMwEQYDVQQIDApTb21lLVN0YXRlMSEwHwYDVQQKDBhJbnRl
cm5ldCBXaWRnaXRzIFB0eSBMdGQwHhcNMjQxMjAzMDMzMDEzWhcNMjUwMTAyMDMz
MDEzWjBFMQswCQYDVQQGEwJBVTETMBEGA1UECAwKU29tZS1TdGF0ZTEhMB8GA1UE
CgwYSW50ZXJuZXQgV2lkZ2l0cyBQdHkgTHRkMIGfMA0GCSqGSIb3DQEBAQUAA4GN
ADCBiQKBgQCtgxHko0Uildu42WSQ/Rcd4ysgOcNqEjqCpzzTj9TFvEIN/FwoA28e
ZQWF6j14XanHX8M1sxhYRqIXbH0lHL3B6I/reEWERRTbXSkt8vdQwO6ehr+i0bCg
pAYMz+R8F8gFqn0DcZn8wIt3xQfDCVgbl1SD97JbukymKCeG7BvAmQIDAQABMA0G
CSqGSIb3DQEBCwUAA4GBAHUGJuInhg9JYrokQfjSYMwD1xpkxzgm1s4G7qyIKpS6
N2GgywMYRnKZ473sCnbKI0ECG0ZvEPa7gzteKeP8pnKC4N0hqZS8OPbDcaKx5Bc9
kqQvLlRI675/2tmdyzwCYYrh4QgGPrkIqEgBWZTgnO4lXxZEpJLAdru80R4Dk0QN
-----END CERTIFICATE-----
`;

const CLIENT_CSR = `\
-----BEGIN CERTIFICATE REQUEST-----
MIIBhDCB7gIBADBFMQswCQYDVQQGEwJBVTETMBEGA1UECAwKU29tZS1TdGF0ZTEh
MB8GA1UECgwYSW50ZXJuZXQgV2lkZ2l0cyBQdHkgTHRkMIGfMA0GCSqGSIb3DQEB
AQUAA4GNADCBiQKBgQCtgxHko0Uildu42WSQ/Rcd4ysgOcNqEjqCpzzTj9TFvEIN
/FwoA28eZQWF6j14XanHX8M1sxhYRqIXbH0lHL3B6I/reEWERRTbXSkt8vdQwO6e
hr+i0bCgpAYMz+R8F8gFqn0DcZn8wIt3xQfDCVgbl1SD97JbukymKCeG7BvAmQID
AQABoAAwDQYJKoZIhvcNAQELBQADgYEALU7NLk9sBG31S7Ax7dxz49Xpm3Q++HoL
XszWw3NMTYGJpXFoVdqueF8GDusO3PqltKW+UZyhu2zx/dKqEfqRb97/04zFlMGg
E+v2+D1XEgn8zmJZreFGtAHdxPWtJWNHk6OPAl+T2OSAS5GD5wh1FnhVyc1n3VRh
IG4aJqeYVc8=
-----END CERTIFICATE REQUEST-----
`;

const CLIENT_KEY = `\
-----BEGIN PRIVATE KEY-----
MIICdgIBADANBgkqhkiG9w0BAQEFAASCAmAwggJcAgEAAoGBAK2DEeSjRSKV27jZ
ZJD9Fx3jKyA5w2oSOoKnPNOP1MW8Qg38XCgDbx5lBYXqPXhdqcdfwzWzGFhGohds
fSUcvcHoj+t4RYRFFNtdKS3y91DA7p6Gv6LRsKCkBgzP5HwXyAWqfQNxmfzAi3fF
B8MJWBuXVIP3slu6TKYoJ4bsG8CZAgMBAAECgYB+QYmbnVKJQBKKB2YuOnu/u7V9
1YpkfK8msxqHt3lUCRDnrGJCm30X2NqT/0aLd1w7P2uEf7WPRpZcBQ1rG+bXJ/HT
iUZI6KJNrysM9vzAiQD12FXmbtzJBlK3znEl+kgc2GO6mISr3AAE4JoZGK7xSKQ8
0slvW1X7ZKb2M/zIAQJBANNibSaQWXTeSaW/xhIHkIWpnYH2yhz8huO3i1NN6Ng9
qqijoYn2s5tgcGamUrRzTtopVzl7Be9CF9V5HnxcZuECQQDSIk8P56joddZhADWU
AuhE4aIv98r7CA9tcbA/mZnB7ydtNeomK754NFfjhKUu1/LKicFMP0/BTovGaFkW
ymi5AkEAkCqr2MZQTJWiUwoVM4y3M4H364B+XgCYmsw+mKUlLf3426Ul8iswWcMP
ReMfuvR9jeruE0TlSkWgbbZ6ZUS74QJAaW/1o9Flm16lNv7X43CiAw4ER3VaUCN3
Oj81ZHQ6BmltqwrGdmi0pbP99Zd1GtAYbzA34X5TEnfLAr8RFLJzYQJAEDpp0i+J
IR9K56BJrezbmucguXccpK6dXy1af32cjdv6KzcS1hE0kE+qe8nab88rjIYA5n5m
DTJhPRpIBbJp5Q==
-----END PRIVATE KEY-----
`;

const SERVER_CERT = `\
-----BEGIN CERTIFICATE-----
MIICNDCCAZ0CFHHQHHOpNz3WmKX39dJQ9+e8SKnxMA0GCSqGSIb3DQEBCwUAMFkx
CzAJBgNVBAYTAkFVMRMwEQYDVQQIDApTb21lLVN0YXRlMSEwHwYDVQQKDBhJbnRl
cm5ldCBXaWRnaXRzIFB0eSBMdGQxEjAQBgNVBAMMCWxvY2FsaG9zdDAeFw0yNDEy
MDMwNDA2MDNaFw0yNTAxMDIwNDA2MDNaMFkxCzAJBgNVBAYTAkFVMRMwEQYDVQQI
DApTb21lLVN0YXRlMSEwHwYDVQQKDBhJbnRlcm5ldCBXaWRnaXRzIFB0eSBMdGQx
EjAQBgNVBAMMCWxvY2FsaG9zdDCBnzANBgkqhkiG9w0BAQEFAAOBjQAwgYkCgYEA
rBGObWfAAcrp3y+4AglMXAefo1+5X2raLUIOgpv6DK09P7tSaDtOeCTUIURFo/3b
cb4VaZxaizGErkG3jU8h4Jai+zfH7pbStoNfiUIOOqBzJ1e4WfsL74uaVPYBHmp+
Myf1Hv0bS6LSUK03mFf+Jz8HGXuGcv9lFPLjhnSNutECAwEAATANBgkqhkiG9w0B
AQsFAAOBgQBCCLLMzOmtwLC4TFxtqXfYKM6DtdmBDPeECuTpsTHw0MmGukDxjEmP
ZcoUrXIFsVny973mtfm7RTGDpLoXc+/i/GJQO3a22lZ720buetbySo1HQDhKYltk
5EAAjtvvajictMiIk/SzmTLWLFGl8HLSFbCg8EV+fw8u4Mfl1XL/dw==
-----END CERTIFICATE-----
`;

const SERVER_CSR = `\
-----BEGIN CERTIFICATE REQUEST-----
MIIBmTCCAQICAQAwWTELMAkGA1UEBhMCQVUxEzARBgNVBAgMClNvbWUtU3RhdGUx
ITAfBgNVBAoMGEludGVybmV0IFdpZGdpdHMgUHR5IEx0ZDESMBAGA1UEAwwJbG9j
YWxob3N0MIGfMA0GCSqGSIb3DQEBAQUAA4GNADCBiQKBgQCsEY5tZ8AByunfL7gC
CUxcB5+jX7lfatotQg6Cm/oMrT0/u1JoO054JNQhREWj/dtxvhVpnFqLMYSuQbeN
TyHglqL7N8fultK2g1+JQg46oHMnV7hZ+wvvi5pU9gEean4zJ/Ue/RtLotJQrTeY
V/4nPwcZe4Zy/2UU8uOGdI260QIDAQABoAAwDQYJKoZIhvcNAQELBQADgYEAiGKk
1fOMfyqMKi9MA/akArpKMcdkAnYXCO0sH20fiZDS3ivjJn2EQ/gv7+zkLLxOs6qq
KjYGqSzStRNW4lbw0PF7QDa+m7fCs8cMbDp6JjSDFN/0cB9epclIjvbQtlkKAJl4
y8pgfYDSWLQdgrQCO0LI+b6c7Ut3j+iYzL9rXJE=
-----END CERTIFICATE REQUEST-----
`;

const SERVER_KEY = `\
-----BEGIN PRIVATE KEY-----
MIICdwIBADANBgkqhkiG9w0BAQEFAASCAmEwggJdAgEAAoGBAKwRjm1nwAHK6d8v
uAIJTFwHn6NfuV9q2i1CDoKb+gytPT+7Umg7Tngk1CFERaP923G+FWmcWosxhK5B
t41PIeCWovs3x+6W0raDX4lCDjqgcydXuFn7C++LmlT2AR5qfjMn9R79G0ui0lCt
N5hX/ic/Bxl7hnL/ZRTy44Z0jbrRAgMBAAECgYAUDmvSltBLpTJDgJVrL1hGNeFG
ssaxt4u80MFOOg4YYi0Me7IsUhVgbbKIOiP/7HwisuxeBgqLxPbZNPHHN90T1oCF
shotVCjA3WhiJLdNIf/Wincbsu7FOZiDGrYWyVdsAdlBPbxIfmDD6CYYw1XyBHSp
X+yysCe4sPKZuRQQrQJBAORzfzs0H8biRnizyREgJZYQPshztGlhPlnF+rWnydrV
+MHnBcWX/qLi/86xP6IUCBezb2bASml1L4iejhDdG5sCQQDA0XnSJ7do0b1ZWW0c
Z9ZUP5GEqBJy65mfjrDOVXWwuguBhr1TnLIEWYpHUUQendYEN3x8H5UJRcJ5EnHh
zbgDAkEArib9zvwlXVARuOIVXWDMRmGL+vN5jPv8tCMgxGpsjs6fG/IpjEAadcHm
kIK+p6fto2O+gO4Fy+7xlYyJcIGeEQJADcQm5WkugA5RbXqj/p4vQC6Vrhntz0Sg
4DJozyJs16RAxAuhosGSOBtIcxULPwBX0k8/1QDQPCw92TUG6m8sjwJBAIK/d5iE
n8vv69lbihEkcrOjEnAhxqAf8AD8qVZKZpeqr82QATRsyyZySRjP8jE2xISHW79a
5T6xh5tyLl0bRuw=
-----END PRIVATE KEY-----
`;

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
                message: `"a": illegal date: Invalid Date`
            });

            await expect(client.batchWrite([], invalidDate)).rejects.toMatchObject({
                message: `illegal date: Invalid Date`
            });

            await expect(client.batchWrite([['a', -1, invalidDate]])).rejects.toMatchObject({
                message: `"a": illegal date: Invalid Date`
            });

            await expect(client.batchWrite({ a: { value: -1, timestamp: invalidDate } })).rejects.toMatchObject({
                message: `"a": illegal date: Invalid Date`
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

            test('TLS', async () => {
                const server = new tls.Server({
                    cert: SERVER_CERT,
                    key:  SERVER_KEY,
                    ca:  [CLIENT_CERT],
                    rejectUnauthorized: true,
                    requestCert: true,
                });
                try {
                    await listenTCP(server, port, 'localhost');
                    const promise = connect(server);

                    const client = new CarbonClient({
                        address: 'localhost',
                        port,
                        transport: 'TLS',
                        tlsCert: CLIENT_CERT,
                        tlsKey:  CLIENT_KEY,
                        tlsCA: [SERVER_CERT],
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

            test.todo('TLS');
        });
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

        test.todo('TLS');
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

        test.todo('TLS');
    });
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

            test.todo('TLS');
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

            test.todo('TLS');
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

            test.todo('TLS');
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

            test.todo('TLS');
        });
    }
});
