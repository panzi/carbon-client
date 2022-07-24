import { Socket as NetSocket } from 'net';
import { Socket as DgramSocket, createSocket as createDgramSocket } from 'dgram';
import * as dns from 'dns';

export abstract class CarbonClientError extends Error {
    constructor(message: string) {
        super(message);

        // The JavaScript Error class is weird and doesn't behave quite like one
        // would expect when subclassed. For that we make our own base class
        // and manually set the name and capture the stack trace. Otherwise the
        // fields name and stack will contain 'Error' and the place where the Error
        // class was subclassed!
        this.name = this.constructor.name;
        Error.captureStackTrace(this, this.constructor);
    }
}

export class NotConnected extends CarbonClientError {
    constructor(message?: string) {
        super(message ?? 'not connected');
    }
}

export class AlreadyConnected extends CarbonClientError {
    constructor(message?: string) {
        super(message ?? 'already connected');
    }
}

export class Disconnected extends CarbonClientError {
    constructor(message?: string) {
        super(message ?? 'disconnected');
    }
}

export class SocketGone extends CarbonClientError {
    constructor(message?: string) {
        super(message ?? 'socket gone');
    }
}

export class HostNotFound extends CarbonClientError {
    readonly hostname: string;

    constructor(hostname: string) {
        super(`host not found: ${hostname}`);
        this.hostname = hostname;
    }
}

export class IllegalArgument extends CarbonClientError {
    constructor(message?: string) {
        super(message ?? 'illegal argument');
    }
}

/**
 * @ignore
 */
export type EventMap = {
    connect: (this: CarbonClient) => void,
    error: (this: CarbonClient, error: Error) => void,
    close: (this: CarbonClient, hadError: boolean) => void,
};

const PATH_REGEXP = /^(?:[-+@$_a-zA-Z0-9]|%[0-9a-fA-F]{2})+(?:\.(?:[-+@$_a-zA-Z0-9]|%[0-9a-fA-F]{2})+)*$/;
const PREFIX_REGEXP = /^(?:[-+@$_a-zA-Z0-9]|%[0-9a-fA-F]{2})+(?:\.(?:[-+@$_a-zA-Z0-9]|%[0-9a-fA-F]{2})+)*\.?$/;
const TAG_NAME_REGEXP = PATH_REGEXP;
const TAG_VALUE_REGEXP = /^(?:[-+@$_.*:,&#|<>\[\](){}=!?^\/\\a-zA-Z0-9]|%[0-9a-fA-F]{2})+$/;

type Arg0<F extends Function> =
    F extends (arg0: infer T) => unknown ? T :
    F extends () => unknown ? void : never;

export type IPTransport = 'UDP'|'TCP';
export type Transport = IPTransport|'IPC';

/**
 * Default port used if none is explicitely specified.
 */
export const DEFAULT_PORT: number = 2003;

/**
 * Default transport used if none is explicitely specified.
 */
export const DEFAULT_TRANSPORT: Transport = 'TCP';

/**
 * Default size of send buffer.
 * 
 * The size of this buffer (1428 bytes) is dimensioned so that the buffer as
 * well as the TCP and IP header fit into one Ethernet frame and can (hopefully)
 * be delivered without fragmentation. 
 * 
 * @see <https://collectd.org/wiki/index.php/Plugin:Write_Graphite>
 */
export const DEFAULT_SEND_BUFFER_SIZE: number = 1428;

/**
 * Default buffer time between sends in milliseconds.
 */
export const DEFAULT_SEND_INTERVAL: number = 1000;

/**
 * Default time in milliseconds to wait after retrying on error.
 */
export const DEFAULT_RETRY_TIMEOUT: number = 1000;

/**
 * [[CarbonClient]] options.
 */
export interface CarbonClientOptions {
    /**
     * For TCP and UDP the hostname or IP address of the server, for
     * IPC the file name of the Unix domain socket.
     */
    address: string;

    /**
     * Server port to connect to. Defaults to [[DEFAULT_PORT]].
     */
    port?: number;

    /**
     * Automatically connect on write if not connected. Defaults to
     * `false`.
     * 
     * @see [[CarbonClient.autoConnect]]
     */
    autoConnect?: boolean;

    /**
     * Number of automatic retries on network error. Defaults to `0`.
     * 
     * @see [[CarbonClient.retryOnError]]
     */
    retryOnError?: number;

    /**
     * Time to wait before retrying after error. Defaults to [[DEFAULT_RETRY_TIMEOUT]].
     * 
     * @see [[CarbonClient.retryTimeout]]
     */
    retryTimeout?: number;

    /**
     * Transport layer protocol to use. Defaults to [[DEFAULT_TRANSPORT]].
     */
    transport?: Transport;

    /**
     * Size of send buffer. If set to `0` metrics are sent immediately.
     * Defaults to [[DEFAULT_SEND_BUFFER_SIZE]].
     */
    sendBufferSize?: number;

    /**
     * Buffer wait time. If set to `0` metrics are sent immediately.
     * Defaults to [[DEFAULT_SEND_INTERVAL]].
     */
    sendInterval?: number;

    /**
     * If [[CarbonClient.transport]] is `"UDP"` the
     * [dgram.SocketOptions.sendBufferSize](https://nodejs.org/dist/latest-v16.x/docs/api/dgram.html#dgramcreatesocketoptions-callback)
     * of UDP sockets.
     */
    udpSendBufferSize?: number;

    /**
     * For TCP and UDP the IP address family to use.
     * 
     * @see [[CarbonClient.family]]
     */
    family?: 4 | 6;

    /**
     * Prefix added to all metric paths.
     */
    prefix?: string;
}

/**
 * @see [[CarbonClient.batchWrite]] and [[CarbonClient.vbatchWrite]].
 */
export type MetricTuple =
    [path: string, value: number, timestamp: Date, tags?: Tags] |
    [path: string, value: number, tags?: Tags];

/**
 * @see [[CarbonClient.batchWrite]] and [[CarbonClient.vbatchWrite]].
 */
export type MetricParams = { value: number, timestamp?: Date, tags?: Tags };

/**
 * @see [[CarbonClient.batchWrite]] and [[CarbonClient.vbatchWrite]].
 */
export type MetricMap = { [path: string]: number|MetricParams };

/**
 * Tags key-value map.
 * 
 * @see [[CarbonClient]] for allowed tag names and values.
 */
export interface Tags {
    [tag: string]: string
}

function appendTags(buf: string[]|(string|number)[], tags: Tags): void {
    for (const tag in tags) {
        const value = tags[tag];

        if (!TAG_NAME_REGEXP.test(tag)) {
            throw new IllegalArgument(`illegal tag name: ${JSON.stringify(tag)}`);
        }

        if (!TAG_VALUE_REGEXP.test(value)) {
            throw new IllegalArgument(`illegal tag value: ${tag}=${JSON.stringify(value)}`);
        }

        buf.push(';', tag, '=', value);
    }
}

function sleep(milliseconds: number): Promise<void> {
    return new Promise(resolve => setTimeout(resolve, milliseconds));
}

interface CallbackInfo<Callback extends Function> {
    callback: Callback;
    once: boolean;
}

interface Callbacks<Callback extends Function> {
    callbacks: CallbackInfo<Callback>[];
    remove: Callback[];
    emitActive: boolean;
}

/**
 * Implementing of the [graphite](https://graphiteapp.org/) [carbon](https://github.com/graphite-project/carbon)
 * [plain text protocol](https://graphite.readthedocs.io/en/latest/feeding-carbon.html#the-plaintext-protocol).
 * 
 * #### Allowed Names and Values
 * 
 * This graphite carbon protocol implementation is a bit strict about the characters
 * allowed in metric paths, tag names, and tag values just to be safe. The plain text
 * protocol documentation itself isn't extremely precise and doesn't mention any
 * allowed or disallowed characters or encoding schemes. Given the protocol just
 * consists of lines in the form of `<metric path> <metric value> <metric timestamp>`
 * it is clear that the path may not contain any whitespace. Also since the tags are
 * just appended to the path name using `;` as a delimiter between the path and the
 * tags and `=` between tag name and value these characters can clearly also no occur
 * in path names. Also the whisper server seems to store metric on the filesystem
 * by splitting it on `.` and creating a filesystem hierarchy based on the split
 * path comonents. That means paths better not contain any characters not allowed
 * in file names of any common operating systems. Also no double-`.`, leading-`.`
 * or trailing-`.`. For tags there are also certain limitations around the `~`
 * character.
 * 
 * Therefore this library only allows paths and tag names in this schema:
 * 
 * ```shell
 * path           = path-component ('.' path-component)*
 * path-component = (letter | digit | '-' | '+' | '@' | '$' | '_' | hex-encoding)+
 * 
 * prefix         = path '.'?
 * 
 * tag-name       = path
 * 
 * tag-value      = (letter | digit | '-' | '+' | '@' | '$' | '_' | '.' | '*'
 *                  | ':' | ',' | '&' | '#' | '|' | '<' | '>' | '[' | ']'
 *                  | '(' | ')' | '{' | '}' | '=' | '!' | '?' | '^' | '/'
 *                  | '\' | hex-encoding)+
 * 
 * hex-encoding   = '%' hex-digit hex-digit
 * letter         = 'a' ... 'z' | 'A' ... 'Z'
 * digit          = '0' ... '9'
 * hex-digit      = '0' ... '9' | 'a' ... 'f' | 'A' ... 'F'
 * ```
 * 
 * Note that tag values may not be empty.
 * 
 * The `hex-encoding` is meant as a convention. If you really have to use metric
 * paths or tag names with other characters you can pass them through
 * `encodeURIComponent()`. Note that you have to also do the reverse
 * (`decodeURIComponent()`) when reading back metric paths, tag names and values.
 * 
 * Note that if the whisper server would really run on Windows (don't know if it ever
 * does) there are also problems with case-insensitivity (also true for macOS) and
 * for certain special file names: `COM1` `COM2` `COM3` `COM4` `COM5` `COM6` `COM7`
 * `COM8` `COM9` `LPT1` `LPT2` `LPT3` `LPT4` `LPT5` `LPT6` `LPT7` `LPT8` `LPT9` `CON`
 * `NUL` `PRN`
 * 
 * Also the tag name `name` is used to store the metric path.
 * 
 * This library does not take care of these cases.
 * 
 * @see [Plain Text Protocol](https://graphite.readthedocs.io/en/latest/feeding-carbon.html#the-plaintext-protocol)
 * @see [Tags](https://graphite.readthedocs.io/en/latest/tags.html#carbon)
 */
export class CarbonClient {
    /**
     * For TCP and UDP the hostname or IP address of the server, for
     * IPC the file name of the Unix domain socket.
     */
    readonly address: string;

    /**
     * Server port to connect to. Will be `-1` if [[CarbonClient.transport]] is `'IPC'`.
     */
    readonly port: number;

    /**
     * Transport layer protocol to use.
     */
    readonly transport: Transport;

    /**
     * If [[CarbonClient.transport]] is `"UDP"` the
     * [dgram.SocketOptions.sendBufferSize](https://nodejs.org/dist/latest-v16.x/docs/api/dgram.html#dgramcreatesocketoptions-callback)
     * of UDP sockets.
     */
    readonly udpSendBufferSize?: number;

    /**
     * Size of send buffer. If set to `0` metrics are sent immediately.
     * Defaults to [[DEFAULT_SEND_BUFFER_SIZE]].
     */
    readonly sendBufferSize: number = DEFAULT_SEND_BUFFER_SIZE;

    /**
     * Buffer wait time. If set to `0` metrics are sent immediately.
     * Defaults to [[DEFAULT_SEND_INTERVAL]].
     */
    readonly sendInterval: number = DEFAULT_SEND_INTERVAL;

    /**
     * For TCP and UDP the IP address family to use.
     * 
     * If not given it will be auto-detected. For UDP if there are IPv6 and IPv4
     * addresses of a hostname the IPv6 address will be preferred. Explicitely
     * pass `4` if you do not want this.
     */
    readonly family?: 4 | 6;

    /**
     * Prefix added to all metric paths.
     */
    readonly prefix: string = '';

    /**
     * Automatically connect on write if not connected.
     * 
     * Also if `ture` then calls of [[CarbonClient.connect]] when connected and
     * calls of [[CarbonClient.disconnect]] when not connected are not errors.
     */
    autoConnect: boolean;

    /**
     * Number of automatic retries on network error. Defaults to `0`.
     * 
     * Error handlers are still called.
     * 
     * @see [[CarbonClient.retryTimeout]]
     */
    retryOnError: number = 0;

    /**
     * Time to wait before retrying after error. Defaults to [[DEFAULT_RETRY_TIMEOUT]].
     * 
     * @see [[CarbonClient.retryOnError]]
     */
    retryTimeout: number = DEFAULT_RETRY_TIMEOUT;

    private _socket: NetSocket|DgramSocket|null = null;
    private _callbacks: { [key in keyof EventMap]: Callbacks<EventMap[key]> } = {
        connect: { callbacks: [], emitActive: false, remove: [] },
        error:   { callbacks: [], emitActive: false, remove: [] },
        close:   { callbacks: [], emitActive: false, remove: [] },
    };

    private readonly _sendBuffer?: Buffer;
    private _sendBufferOffset: number = 0;
    private _sendIntervalTimer: NodeJS.Timeout|null = null;

    /**
     * 
     * @param address hostname or IP address of carbon server
     * @param port port of carbon server, [[DEFAULT_PORT]] if not given
     * @param transport transport layer protocol to use, [[DEFAULT_TRANSPORT]] if not given
     * @param autoConnect automatically connect on write if not connected, `false` if not given
     * 
     * @throws [[IllegalArgument]]
     * @throws [[TypeError]]
     */
    constructor(address: string, port?: number, transport?: IPTransport, autoConnect?: boolean);

    /**
     * 
     * @param path path of Unix domain socket
     * @param transport always `'IPC'`
     * @param autoConnect automatically connect on write if not connected, `false` if not given
     * 
     * @throws [[IllegalArgument]]
     * @throws [[TypeError]]
     */
    constructor(path: string, transport: 'IPC', autoConnect?: boolean);

    constructor(options: CarbonClientOptions);

    constructor(arg1: string|CarbonClientOptions, arg2?: number|'IPC', arg3?: IPTransport|boolean, arg4?: boolean) {
        if (typeof arg1 === 'object') {
            const transport   = arg1.transport ?? DEFAULT_TRANSPORT;
            this.address      = arg1.address;
            this.port         = arg1.port ?? (transport === 'IPC' ? -1 : DEFAULT_PORT);
            this.transport    = transport;
            this.autoConnect  = arg1.autoConnect ?? false;

            const { prefix, sendBufferSize, udpSendBufferSize, sendInterval, retryTimeout, retryOnError } = arg1;

            if (prefix) {
                if (!PREFIX_REGEXP.test(prefix)) {
                    throw new IllegalArgument(`illegal prefix: ${JSON.stringify(prefix)}`);
                }
                this.prefix = prefix;
            }

            if (sendBufferSize != undefined) {
                if (!isFinite(sendBufferSize) || sendBufferSize < 0 || (sendBufferSize|0) !== sendBufferSize) {
                    throw new IllegalArgument(`illegal sendBufferSize: ${sendBufferSize}`);
                }
                this.sendBufferSize = sendBufferSize;
            }

            if (transport === 'UDP' && udpSendBufferSize != undefined) {
                if (!isFinite(udpSendBufferSize) || udpSendBufferSize <= 0 || (udpSendBufferSize|0) !== udpSendBufferSize) {
                    throw new IllegalArgument(`illegal udpSendBufferSize: ${udpSendBufferSize}`);
                }
                this.udpSendBufferSize = udpSendBufferSize;
            }

            if (sendInterval != undefined) {
                if (!isFinite(sendInterval) || sendInterval < 0) {
                    throw new IllegalArgument(`illegal sendInterval: ${sendInterval}`);
                }
                this.sendInterval = sendInterval;
            }

            if (retryOnError != undefined) {
                if (!isFinite(retryOnError) || retryOnError < 0 || (retryOnError|0) !== retryOnError) {
                    throw new IllegalArgument(`illegal retryOnError: ${retryOnError}`);
                }
                this.retryOnError = retryOnError;
            }

            if (retryTimeout != undefined) {
                if (!isFinite(retryTimeout) || retryTimeout < 0) {
                    throw new IllegalArgument(`illegal retryTimeout: ${retryTimeout}`);
                }
                this.retryTimeout = retryTimeout;
            }

            if (transport !== 'IPC') {
                this.family = arg1.family;
            }
        } else {
            this.address = arg1;
            this.retryOnError = 0;

            if (arg2 === 'IPC') {
                this.port      = -1;
                this.transport = 'IPC';

                const autoConnect = arg3 ?? false
                if (typeof autoConnect !== 'boolean') {
                    throw new TypeError(`autoConnect has illegal type: ${typeof autoConnect}`);
                }

                this.autoConnect = autoConnect;
            } else {
                const transport = arg3 ?? DEFAULT_TRANSPORT;
                if (typeof transport !== 'string') {
                    throw new TypeError(`transport has illegal type: ${typeof transport}`);
                }
                this.port = arg2 ?? (transport === 'IPC' ? -1 : DEFAULT_PORT);
                this.transport   = transport;
                this.autoConnect = arg4 ?? false;
            }
        }

        if (this.transport !== 'IPC') {
            const { port } = this;
            if (port <= 0 || !isFinite(port) || (port|0) !== port || port > 65_535) {
                throw new IllegalArgument(`illegal port number: ${port}`);
            }
        }

        if (this.sendBufferSize > 0 && this.sendInterval > 0) {
            this._sendBuffer = Buffer.alloc(this.sendBufferSize);
        }
    }

    private _onConnect = () => {
        this._emit('connect', undefined);
    };

    private _onError = (error: Error) => {
        this._emit('error', error);
    };

    private _onClose = (hadError: boolean) => {
        try {
            this._emit('close', hadError);
        } finally {
            try {
                if (this._socket) {
                    this._socket.off('connect', this._onConnect);
                    this._socket.off('error', this._onError);
                    if (this.transport === 'TCP' || this.transport === 'IPC') {
                        this._socket.off('close', this._onClose);
                        this._socket.off('end', this._onCloseOk);
                    } else {
                        this._socket.off('close', this._onCloseOk);
                    }
                }
            } finally {
                this._socket = null;
            }
        }
    };

    private _onCloseOk = () => {
        this._onClose(false);
    };

    connect(): Promise<void>;
    connect(callback: (error?: Error) => void): void;

    /**
     * Connect client to server.
     * 
     * This creates and connects the underlying socket. If there is another `connect()` in progress
     * it will attach to that. If that other `connect()` is performed in the context of an auto-connect
     * ([[CarbonClient.autoConnect]] is `true`) and [[CarbonClient.retryOnError]] is `> 0` then this
     * means this `connect()` call will wait until all the retries are finished. Note that normally
     * `connect()` only performs one connection attempt, even if [[CarbonClient.retryOnError]] is `> 0`.
     * 
     * Also, if you manually call `connect()` right before a send is happening in the background, that
     * send will attach to this `connect()` and as such will not retry on connection failure, even if
     * [[CarbonClient.retryOnError]] is `> 0`.
     * 
     * @throws [[AlreadyConnected]] if connected and [[CarbonClient.autoConnect]] is `false`.
     * @throws [[HostNotFound]] if the host referred to by [[CarbonClient.address]] cannot be resolved.
     * @throws [[Disconnected]] if [[CarbonClient.disconnect]] is called while connect is in progress.
     * @throws [[SocketGone]] if the socket goes away during the operation. (*should never happen*)
     * @throws [[TypeError]] if the socket changes type ([[DgramSocket]] <-> [[NetSocket]]) during
     *         the operation. (*should never happen*)
     * @throws [[Error]] based on the errors that can be thrown by the underlying used NodeJS APIs.
     */
    connect(callback?: (error?: Error) => void): Promise<void>|void {
        return this._connectWithRetry(0, callback);
    }

    private _connect(): Promise<void>;
    private _connect(callback: (error?: Error) => void): void;
    private _connect(callback?: (error?: Error) => void): Promise<void>|void;

    private _connect(callback?: (error?: Error) => void): Promise<void>|void {
        if (this._socket) {
            if (this.autoConnect) {
                if (callback) {
                    return callback();
                } else {
                    return Promise.resolve();
                }
            }
            const error = new AlreadyConnected();
            if (callback) {
                return callback(error);
            } else {
                return Promise.reject(error);
            }
        }

        let address: string;
        const executor = (resolve: () => void, reject: (error: Error) => void): void => {
            try {
                if (!this._socket) {
                    return reject(new SocketGone('socket gone before could connect'));
                }

                this._socket.on('connect', this._onConnect);
                this._socket.on('error', this._onError);

                const callback = () => {
                    try {
                        this._socket?.off('error', reject);
                    } finally {
                        resolve();
                    }
                };
                if (this.transport === 'IPC') {
                    if (this._socket instanceof DgramSocket) {
                        return reject(new TypeError('socket changed type!?'));
                    }
                    this._socket.once('error', reject);
                    this._socket.connect(address, callback);
                } else {
                    this._socket.once('error', reject);
                    if (this.family !== undefined && this._socket instanceof NetSocket) {
                        this._socket.connect({
                            host: address,
                            port: this.port,
                            family: this.family
                        }, callback);
                    } else {
                        this._socket.connect(this.port, address, callback);
                    }
                }
            } catch (error) {
                reject(error instanceof Error ? error : new Error(String(error)));
            }
        };

        if (this.transport === 'TCP' || this.transport === 'IPC') {
            address = this.address;
            this._socket = new NetSocket({ writable: true });

            this._socket.on('close', this._onClose);
            this._socket.on('end', this._onCloseOk);
        } else {
            if (this.family === undefined) {
                const dnsExecutor = (resolve: () => void, reject: (error: Error) => void): void => {
                    dns.lookup(this.address, { all: true }, (error, addresses) => {
                        if (error) {
                            return reject(error);
                        }
                        const adr = addresses.find(adr => adr.family === 6) ?? addresses.find(adr => adr.family === 4);
                        if (!adr) {
                            return reject(new HostNotFound(this.address));
                        }
                        address = adr.address;
                        try {
                            this._socket = createDgramSocket({
                                type: adr.family === 6 ? 'udp6' : 'udp4',
                                sendBufferSize: this.udpSendBufferSize,
                            });
                            this._socket.on('close', this._onCloseOk);
                        } catch (error) {
                            return reject(error instanceof Error ? error : new Error(String(error)));
                        }

                        executor(resolve, reject);
                    });
                };

                if (callback) {
                    return dnsExecutor(callback, callback);
                } else {
                    return new Promise(dnsExecutor);
                }
            } else {
                address = this.address;
                this._socket = createDgramSocket({
                    type: this.family === 6 ? 'udp6' : 'udp4',
                    sendBufferSize: this.udpSendBufferSize,
                });
                this._socket.on('close', this._onCloseOk);
            }
        }

        if (callback) {
            executor(callback, callback);
        } else {
            return new Promise(executor);
        }
    }

    /**
     * Returns true if the underlying socket was created and is writeable.
     */
    get isConnected(): boolean {
        if (this._socket instanceof NetSocket) {
            return this._socket.writable;
        }
        return this._socket !== null;
    }

    /**
     * Returns true if the client bufferes multiple writes into one send.
     * @see [[CarbonClient.sendBufferSize]] and [[CarbonClient.sendInterval]].
     */
    get isBuffered(): boolean {
        return !!this._sendBuffer;
    }

    /**
     * Returns the number of currently buffered bytes.
     * 
     * If [[CarbonClient.isBuffered]] is `false` this is always `0`.
     */
    get bufferedBytes(): number {
        return this._sendBufferOffset;
    }

    /**
     * Disconnect the client from the server.
     * 
     * **NOTE:** If you want currently buffered data to be sent before disconnect you need
     * to call flush() first! Calling disconnect() cancels currently ongoing connection
     * and send attempts.
     * 
     * This alternative exists because awaiting promises on graceful shutdown
     * seems to not work (the await never returns and the process just quits).
     * Normal callbacks do work.
     * 
     * @param callback Async callback for when the client is disconnected or an error during disconnect occured.
     * 
     * @throws Error if not connected and [[CarbonClient.autoConnect]] is `false`.
     */
    disconnect(callback: (error?: Error) => void): void;

    /**
     * Disconnect the client from the server.
     * 
     * **NOTE:** If you want currently buffered data to be sent before disconnect you need
     * to call flush() first! Calling disconnect() cancels currently ongoing connection
     * and send attempts.
     * 
     * Await the returned promise to wait for the disconnect to finish.
     * 
     * @throws [[NotConnected]] if not connected and [[CarbonClient.autoConnect]] is `false`.
     * @throws [[Error]] based on the errors that can be thrown by the underlying used NodeJS APIs.
     */
    disconnect(): Promise<void>;

    disconnect(callback?: (error?: Error) => void): Promise<void>|void {
        const executor = (resolve: () => void, reject: (error: Error) => void): void => {
            if (this._connectionWaiters !== null) {
                this._connectDone(new Disconnected());
            }

            if (!this._socket) {
                if (this.autoConnect) {
                    return resolve();
                }
                return reject(new NotConnected());
            }

            try {
                if (this._socket instanceof DgramSocket) {
                    this._socket.close(resolve);
                } else {
                    const doDisconnect = () => {
                        try {
                            if (this._socket instanceof NetSocket) {
                                this._socket.destroy();
                            }
                        } catch (error) {
                            return reject(error instanceof Error ? error : new Error(String(error)));
                        }
                        resolve();
                    };

                    if (this._socket.connecting || !this._socket.writable) {
                        // If the server went away doring connecting .end() never calles the callback (or waits for a TCP timeout?).
                        // Since we haven't (half-)sent anything if we're still connecting just kill the connection now. That is
                        // consistent with the description requiring a manual flush() if we want buffered data to be flushed before
                        // disconnect.
                        doDisconnect();
                    } else {
                        this._socket.end(doDisconnect);
                    }
                }
            } catch (error) {
                return reject(error instanceof Error ? error : new Error(String(error)));
            }
        };

        if (callback) {
            executor(callback, callback);
        } else {
            return new Promise(executor);
        }
    }

    flush(): Promise<void>;
    flush(callback: (error?: Error) => void): void;

    /**
     * Flush any buffered data.
     * 
     * Safe to call even if [[CarbonClient.isBuffered]] is `false`.
     * 
     * @throws [[NotConnected]] if not connected and [[CarbonClient.autoConnect]] is `false`.
     * @throws [[Error]] based on the errors that can be thrown by the underlying used NodeJS APIs.
     */
    flush(callback?: (error?: Error) => void): Promise<void>|void {
        const executor = (resolve: () => void, reject: (error: Error) => void): void => {
            if (!this._socket && !this.autoConnect) {
                return reject(new NotConnected());
            }

            if (this._sendIntervalTimer !== null) {
                clearTimeout(this._sendIntervalTimer);
                this._sendIntervalTimer = null;
            }

            if (this._sendBuffer && this._sendBufferOffset > 0) {
                const buf = this._sendBuffer.slice(0, this._sendBufferOffset);
                this._sendBufferOffset = 0;

                this._send(buf, error => {
                    if (error) {
                        return reject(error);
                    }
                    resolve();
                });
            } else {
                resolve();
            }
        };

        if (callback) {
            executor(callback, callback);
        } else {
            return new Promise(executor);
        }
    }

    on(event: 'connect', callback: () => void): void;
    on(event: 'error', callback: (error: Error) => void): void;
    on(event: 'close', callback: (hadError: boolean) => void): void;
    /**
     * @ignore
     */
    on<Event extends keyof EventMap>(event: Event, callback: EventMap[Event]): void;

    /**
     * Register an event handler.
     * 
     * Registering the same function multiple times will cause it to be called as often
     * as it was registered for one event.
     * 
     * If an error occures during event handling this error is itself dispatched to any
     * registered error event handlers. If the error happened in an error event handler
     * or if there are no error event handlers then the error is thrown, leading to an
     * unhandeled rejection.
     * 
     * @param event Event name to listen for.
     * @param callback Event handler to call.
     */
    on<Event extends keyof EventMap>(event: Event, callback: EventMap[Event]): void {
        this._callbacks[event].callbacks.push({ callback, once: false });
    }

    once(event: 'connect', callback: () => void): void;
    once(event: 'error', callback: (error: Error) => void): void;
    once(event: 'close', callback: (hadError: boolean) => void): void;
    /**
     * @ignore
     */
    once<Event extends keyof EventMap>(event: Event, callback: EventMap[Event]): void;

    /**
     * Register an event handler that is only called once.
     * 
     * Registering the same function multiple times will cause it to be called as often
     * as it was registered for one event.
     * 
     * @see [[CarbonClient.on]]
     * 
     * @param event Event name to listen for.
     * @param callback Event handler to call.
     */
    once<Event extends keyof EventMap>(event: Event, callback: EventMap[Event]): void {
        this._callbacks[event].callbacks.push({ callback, once: true });
    }

    off(event: 'connect', callback: () => void): void;
    off(event: 'error', callback: (error: Error) => void): void;
    off(event: 'close', callback: (hadError: boolean) => void): void;
    /**
     * @ignore
     */
    off<Event extends keyof EventMap>(event: Event, callback: EventMap[Event]): void;

    /**
     * Remove an event handler.
     * 
     * This removes the first registerd event handler matching the given event name and
     * event handler, no matter if it was registered with [[CarbonClient.on]] or
     * [[CarbonClient.once]]. If you've registered the same event handler multiple time
     * you need to call this function the same number of time to remove it completely
     * again.
     * 
     * @param event Event name of the registered event handler.
     * @param callback Event handler to remove.
     */
    off<Event extends keyof EventMap>(event: Event, callback: EventMap[Event]): void {
        const infos = this._callbacks[event];
        // avoid concurrent modification
        if (infos.emitActive) {
            infos.remove.push(callback);
        } else {
            const index = infos.callbacks.findIndex(info => info.callback === callback);
            if (index >= 0) {
                infos.callbacks.splice(index, 1);
            }
        }
    }

    private _emit<Event extends keyof EventMap>(event: Event, arg: Arg0<EventMap[Event]>): void {
        const infos = this._callbacks[event];
        const oldEmitActive = infos.emitActive;
        infos.emitActive = true;
        try {
            for (const info of infos.callbacks) {
                try {
                    info.callback.call(this, arg);
                } catch (error) {
                    if (event !== 'error' && this._callbacks.error.callbacks.length > 0) {
                        setImmediate(() =>
                            this._emit(
                                'error',
                                error instanceof Error ? error : new Error(String(error))));
                    } else {
                        throw error;
                    }
                } finally {
                    if (info.once) {
                        infos.remove.push(info.callback);
                    }
                }
            }
        } finally {
            infos.emitActive = oldEmitActive;
            if (!oldEmitActive && infos.remove.length > 0) {
                for (const callback of infos.remove) {
                    const index = infos.callbacks.findIndex(info => info.callback === callback);
                    if (index >= 0) {
                        infos.callbacks.splice(index, 1);
                    }
                }
                infos.remove = [];
            }
        }
    }

    private _sendCallback = () => {
        this._sendIntervalTimer = null;
        if (this._sendBuffer && this._sendBufferOffset > 0) {
            const buf = this._sendBuffer.slice(0, this._sendBufferOffset);
            this._sendBufferOffset = 0;
            this._send(buf, error => {
                if (error) {
                    this._onError(error);
                }
            });
        }
    };

    private _bufferedSend(data: string): Promise<void> {
        if (!this._socket && !this.autoConnect) {
            return Promise.reject(new NotConnected());
        }

        if (this._sendBuffer) {
            const byteCount = Buffer.byteLength(data);
            let newOffset = byteCount + this._sendBufferOffset;
            if (newOffset > this._sendBuffer.length && this._sendBufferOffset > 0) {
                const buf = this._sendBuffer.slice(0, this._sendBufferOffset);
                this._sendBufferOffset = 0;
                newOffset = byteCount;
                if (newOffset < this._sendBuffer.length) {
                    const promise = this._send(buf);

                    this._sendBuffer.write(data, this._sendBufferOffset);
                    this._sendBufferOffset = newOffset;

                    if (this._sendIntervalTimer === null) {
                        this._sendIntervalTimer = setTimeout(this._sendCallback, this.sendInterval);
                    }

                    return promise;
                } else {
                    // doesn't fit into buffer, send immediately
                    if (this.transport === 'UDP') {
                        // if at all possible don't exceed sendBufferSize in one send using UDP
                        return this._send(buf).then(() => this._send(data));
                    }
                    return this._send(Buffer.concat([ buf, Buffer.from(data) ]));
                }
            }

            if (newOffset < this._sendBuffer.length) {
                this._sendBuffer.write(data, this._sendBufferOffset);
                this._sendBufferOffset = newOffset;

                if (this._sendIntervalTimer === null) {
                    this._sendIntervalTimer = setTimeout(this._sendCallback, this.sendInterval);
                }

                return Promise.resolve();
            } else {
                // doesn't fit into buffer (or is exactly the buffer size), send immediately
                return this._send(data);
            }
        } else {
            return this._send(data);
        }
    }

    private _connectionRetryTimer: NodeJS.Timeout|null = null;
    private _connectionWaiters: ((error?: Error) => void)[]|null = null;

    private _connectDone(error?: Error): void {
        if (this._connectionRetryTimer !== null) {
            clearTimeout(this._connectionRetryTimer);
            this._connectionRetryTimer = null;
        }
        const waiters = this._connectionWaiters;
        this._connectionWaiters = null;
        if (waiters) {
            for (const waiter of waiters) {
                try {
                    waiter(error);
                } catch (error2) {
                    setImmediate(() =>
                        this._emit(
                            'error',
                            error2 instanceof Error ? error2 : new Error(String(error2))));
                }
            }
        }
    }

    private _connectWithRetry(retryOnError: number): Promise<void>;
    private _connectWithRetry(retryOnError: number, callback: (error?: Error) => void): void;
    private _connectWithRetry(retryOnError: number, callback?: (error?: Error) => void): Promise<void>|void;

    private _connectWithRetry(retryOnError: number, callback?: (error?: Error) => void): Promise<void>|void {
        let retryCount = 0;
        const connectCallback = (error?: Error) => {
            if (error) {
                if (retryCount < retryOnError) {
                    ++ retryCount;
                    this._connectionRetryTimer = setTimeout(() => {
                        this._connectionRetryTimer = null;
                        if (this._socket) {
                            this._connectDone();
                        } else if (this._connectionWaiters !== null) {
                            this._connect(connectCallback);
                        }
                    });
                    return;
                }
                return this._connectDone(error);
            }
            this._connectDone();
        };

        if (callback) {
            if (this._connectionWaiters === null) {
                this._connectionWaiters = [callback];
                this._connect(connectCallback);
            } else {
                this._connectionWaiters.push(callback);
            }
        } else {
            return new Promise((resolve, reject) => {
                if (this._connectionWaiters === null) {
                    this._connectionWaiters = [ error => error ? reject(error) : resolve() ];
                    this._connect(connectCallback);
                } else {
                    this._connectionWaiters.push(error => error ? reject(error) : resolve());
                }
            });
        }
    }

    private _send(data: string|Buffer): Promise<void>;
    private _send(data: string|Buffer, callback: (error?: Error) => void): void;

    private _send(data: string|Buffer, callback?: (error?: Error) => void): Promise<void>|void {
        const executor = (resolve: () => void, reject: (error: Error) => void): void => {
            try {
                let sendRetryCount = 0;
                const sendCallback = (error?: Error|null) => {
                    if (error) {
                        if (this._socket && sendRetryCount < this.retryOnError) {
                            ++ sendRetryCount;
                            setTimeout(() => {
                                if (this._socket) {
                                    doSend();
                                } else {
                                    reject(error);
                                }
                            }, this.retryTimeout);
                            return;
                        }
                        return reject(error);
                    }
                    resolve();
                };

                const doSend = () => {
                    if (!this._socket) {
                        return reject(new SocketGone('socket gone before could send data'));
                    }

                    if (this._socket instanceof DgramSocket) {
                        this._socket.send(data, sendCallback);
                    } else {
                        this._socket.write(data, sendCallback);
                    }
                };

                if (!this._socket) {
                    if (this.autoConnect) {
                        return this._connectWithRetry(this.retryOnError, error => {
                            if (error) {
                                return reject(error);
                            }
                            doSend();
                        });
                    }
                    return reject(new NotConnected());
                }

                doSend();
            } catch (error) {
                reject(error instanceof Error ? error : new Error(String(error)));
            }
        };

        if (callback) {
            executor(callback, callback);
        } else {
            return new Promise(executor);
        }
    }

    /**
     * Send a metric to the carbon server.
     * 
     * @returns Promise that returns when the metric is sent.
     */
    write(path: string, value: number, timestamp: Date, tags?: Tags): Promise<void>;

    /**
     * Send a metric to the carbon server.
     * 
     * The current time (via `Date.now()`) will be used.
     * 
     * **NOTE:** If [[CarbonClient.sendBufferSize]] is `> 0` then any error (except for
     * [[IllegalArgument]]) might not happen during `await` of the returned Promise, but
     * will only dispatched to any registered error handlers. See: [[CarbonClient.on]]
     * 
     * Also note that if the sent data doesn't fit into the buffer, this will issue a
     * send immediately and any promise rejections of that send *will* be directly returned
     * when `await`ing the returned Promise.
     * 
     * @returns Promise that returns when the metric is sent.
     * @throws [[IllegalArgument]]
     * @throws [[NotConnected]] if not connected and [[CarbonClient.autoConnect]] is `false`.
     * @throws [[SocketGone]] if socket went away during the operation.
     * @throws [[Error]] based on the errors that can be thrown by the underlying used NodeJS APIs.
     */
    write(path: string, value: number, tags?: Tags): Promise<void>;

    async write(path: string, value: number, arg3?: Date|Tags, arg4?: Tags): Promise<void> {
        if (!PATH_REGEXP.test(path)) {
            throw new IllegalArgument(`illegal path: ${JSON.stringify(path)}`);
        }

        let secs: number;
        let tags: Tags|undefined;

        if (arg3 instanceof Date) {
            secs = arg3.getTime() / 1000;
            tags = arg4;

            if (isNaN(secs)) {
                throw new IllegalArgument(`illegal date: ${arg3}`);
            }
        } else {
            secs = Date.now() / 1000;
            tags = arg3 ?? arg4;
        }

        // Couldn't find a clear *documentation* of the format/data types involved,
        // so I looked at the actual source of class MetricLineReceiver in
        // https://github.com/graphite-project/carbon/blob/master/lib/carbon/protocols.py
        // -> value and timestamp are both parsed using Python's float() type
        // constructor.

        const { prefix } = this;
        let data: string|Buffer;
        if (tags) {
            const buf: (string|number)[] = [ prefix, path ];
            appendTags(buf, tags);
            buf.push(' ', value, ' ', secs, '\n');
            data = buf.join('');
        } else {
            data = `${prefix}${path} ${value} ${secs}\n`;
        }

        return this._bufferedSend(data);
    }

    /**
     * Batch send multiple metrics all at once.
     * 
     * If you use UDP keep batch sizes small enough to fit into one UDP packet!
     * 
     * If `timestamp` is not provided the current time (via `Date.now()`) will be used.
     * 
     * **NOTE:** If [[CarbonClient.sendBufferSize]] is `> 0` then any error (except for
     * [[IllegalArgument]]) might not happen during `await` of the returned Promise, but
     * will only dispatched to any registered error handlers. See: [[CarbonClient.on]]
     * 
     * Also note that if the sent data doesn't fit into the buffer, this will issue a
     * send immediately and any promise rejections of that send *will* be directly returned
     * when `await`ing the returned Promise.
     * 
     * @param batch The metrics to write.
     * @param timestamp The timestamp to use for metrics that don't define it directly.
     * @returns Promise that returns when the metric is sent.
     * @throws [[IllegalArgument]]
     * @throws [[Error]] based on the errors that can be thrown by the underlying used NodeJS APIs.
     */
    async batchWrite(batch: MetricMap|MetricTuple[], timestamp?: Date): Promise<void> {
        const buf: (string|number)[] = [];
        const defaultSecs = (timestamp ? timestamp.getTime() : Date.now()) / 1000;
        const { prefix } = this;

        if (isNaN(defaultSecs)) {
            throw new IllegalArgument(`illegal date: ${timestamp}`);
        }

        if (Array.isArray(batch)) {
            for (const [path, value, arg3, arg4] of batch) {
                if (!PATH_REGEXP.test(path)) {
                    throw new IllegalArgument(`illegal path: ${JSON.stringify(path)}`);
                }

                let secs: number;
                let tags: Tags|undefined;
                if (arg3 instanceof Date) {
                    secs = arg3.getTime() / 1000;
                    tags = arg4;

                    if (isNaN(secs)) {
                        throw new IllegalArgument(`illegal date: ${arg3}`);
                    }
                } else {
                    secs = defaultSecs;
                    tags = arg3 ?? arg4;
                }

                buf.push(prefix, path);
                if (tags) {
                    appendTags(buf, tags);
                }

                buf.push(' ', value, ' ', secs, '\n');
            }
        } else {
            for (const path in batch) {
                const arg = batch[path];
                if (!PATH_REGEXP.test(path)) {
                    throw new IllegalArgument(`illegal path: ${JSON.stringify(path)}`);
                }

                let value: number;
                let secs: number;

                buf.push(prefix, path);
                if (typeof arg === 'number') {
                    value = arg;
                    secs = defaultSecs;
                } else {
                    value = arg.value;
                    const { timestamp, tags } = arg;
                    if (timestamp) {
                        secs = timestamp.getTime() / 1000;

                        if (isNaN(secs)) {
                            throw new IllegalArgument(`illegal date: ${timestamp}`);
                        }
                    } else {
                        secs = defaultSecs;
                    }

                    if (tags) {
                        appendTags(buf, tags);
                    }
                }

                buf.push(' ', value, ' ', secs, '\n');
            }
        }

        const data = buf.join('');

        return this._bufferedSend(data);
    }

    /**
     * Send a metric to the carbon server.
     * 
     * In contrast to [[CarbonClient.write]] this doesn't return a promise, but
     * instead if an error occurs during sending this error will be dispatched
     * to any registered `'error'` even handlers.
     * 
     * @param path Metric path.
     * @param value Metric value.
     * @param timestamp Metric timestamp.
     * @param tags Metric tags.
     */
    vwrite(path: string, value: number, timestamp: Date, tags?: Tags): void;

    /**
     * Send a metric to the carbon server.
     * 
     * The current time (via `Date.now()`) will be used.
     * 
     * In contrast to [[CarbonClient.write]] this doesn't return a promise, but
     * instead if an error occurs during sending this error will be dispatched
     * to any registered `'error'` even handlers.
     * 
     * @param path Metric path.
     * @param value Metric value.
     * @param tags Metric tags.
     */
    vwrite(path: string, value: number, tags?: Tags): void;

    vwrite(path: string, value: number, arg3?: Date|Tags, arg4?: Tags): void {
        this.write(path, value, arg3 as any, arg4).catch(this._onError);
    }

    /**
     * Batch send multiple metrics all at once.
     * 
     * If you use UDP keep batch sizes small enough to fit into one UDP packet!
     * 
     * If `timestamp` is not provided the current time (via `Date.now()`) will be used.
     * 
     * In contrast to [[CarbonClient.batchWrite]] this doesn't return a promise, but
     * instead if an error occurs during sending this error will be dispatched
     * to any registered `'error'` even handlers.
     * 
     * @param batch The metrics to write.
     * @param timestamp The timestamp to use for metrics that don't define it directly.
     */
    vbatchWrite(batch: MetricMap|MetricTuple[], timestamp?: Date): void {
        this.batchWrite(batch, timestamp).catch(this._onError);
    }
}
