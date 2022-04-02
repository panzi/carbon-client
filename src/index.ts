import { Socket as NetSocket } from 'net';
import { Socket as DgramSocket, createSocket as createDgramSocket } from 'dgram';
import * as dns from 'dns/promises';

type EventMap = {
    connect: (this: CarbonClient) => void,
    error: (this: CarbonClient, error: Error) => void,
    close: (this: CarbonClient, hadError: boolean) => void,
};

const PATH_REGEXP = /^[-_a-zA-Z0-9]+(\.[-_a-zA-Z0-9]+)*$/;
const TAG_NAME_REGEXP = PATH_REGEXP;
const TAG_VALUE_REGEXP = /^[-_.,:\/%+a-zA-Z0-9]*$/;

type Arg0<F extends Function> =
    F extends (arg0: infer T) => unknown ? T :
    F extends () => unknown ? void : never;

export type IPTransport = 'UDP'|'TCP';
export type Transport = IPTransport|'IPC';

export const DEFAULT_PORT = 2003;
export const DEFAULT_TRANSPORT: Transport = 'TCP';

export interface CarbonClientOptions {
    address: string;
    port?: number;
    autoReconnect?: boolean;
    transport?: Transport;
    sendBufferSize?: number;
}

export type MetricTuple =
    [path: string, value: number, timestamp: Date, tags?: Tags] |
    [path: string, value: number, tags?: Tags];

export type MetricParams = { value: number, timestamp?: Date, tags?: Tags };

export type MetricMap = { [path: string]: number|MetricParams };

export interface Tags {
    [tag: string]: string
}

function appendTags(buf: string[]|(string|number)[], tags: Tags): void {
    for (const tag in tags) {
        const value = tags[tag];

        if (!TAG_NAME_REGEXP.test(tag)) {
            throw new Error(`illegal tag name: ${JSON.stringify(tag)}`);
        }

        if (!TAG_VALUE_REGEXP.test(value)) {
            throw new Error(`illegal tag value: ${tag}=${JSON.stringify(value)}`);
        }

        buf.push(';', tag, '=', value);
    }
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

export class CarbonClient {
    readonly address: string;
    readonly port: number;
    readonly transport: Transport;
    readonly sendBufferSize?: number;
    autoReconnect: boolean;

    private socket: NetSocket|DgramSocket|null = null;
    private callbacks: { [key in keyof EventMap]: Callbacks<EventMap[key]> } = {
        connect: { callbacks: [], emitActive: false, remove: [] },
        error:   { callbacks: [], emitActive: false, remove: [] },
        close:   { callbacks: [], emitActive: false, remove: [] },
    };

    constructor(address: string, port?: number, transport?: IPTransport, autoReconnect?: boolean);
    constructor(path: string, transport: 'IPC', autoReconnect?: boolean);
    constructor(options: CarbonClientOptions);

    constructor(arg1: string|CarbonClientOptions, arg2?: number|'IPC', arg3?: IPTransport|boolean, arg4?: boolean) {
        if (typeof arg1 === 'object') {
            const transport = arg1.transport ?? DEFAULT_TRANSPORT;
            this.address       = arg1.address;
            this.port          = arg1.port ?? (transport === 'IPC' ? -1 : DEFAULT_PORT);
            this.transport     = transport;
            this.autoReconnect = arg1.autoReconnect ?? false;
            if (transport === 'UDP') {
                this.sendBufferSize = arg1.sendBufferSize;
            }
        } else {
            this.address = arg1;
            if (arg2 === 'IPC') {
                this.port      = -1;
                this.transport = 'IPC';

                const autoReconnect = arg3 ?? false
                if (typeof autoReconnect !== 'boolean') {
                    throw new TypeError(`autoReconnect has illegal type: ${typeof autoReconnect}`);
                }

                this.autoReconnect = autoReconnect;
            } else {
                const transport = arg3 ?? DEFAULT_TRANSPORT;
                if (typeof transport !== 'string') {
                    throw new TypeError(`transport has illegal type: ${typeof transport}`);
                }
                this.port = arg2 ?? (transport === 'IPC' ? -1 : DEFAULT_PORT);
                this.transport     = transport;
                this.autoReconnect = arg4 ?? false;
            }
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
                if (this.socket) {
                    this.socket.off('connect', this._onConnect);
                    this.socket.off('error', this._onError);
                    if (this.transport === 'TCP' || this.transport === 'IPC') {
                        this.socket.off('close', this._onClose);
                    } else {
                        this.socket.off('close', this._onCloseOk);
                    }
                }
            } finally {
                this.socket = null;
                if (hadError && this.autoReconnect) {
                    this.connect().catch(this._onError);
                }
            }
        }
    };

    private _onCloseOk = () => {
        this._onClose(false);
    };

    async connect(): Promise<void> {
        if (this.socket) {
            throw new Error('already connected');
        }

        let address: string;
        if (this.transport === 'TCP' || this.transport === 'IPC') {
            address = this.address;
            this.socket = new NetSocket({ writable: true });

            this.socket.on('close', this._onClose);
        } else {
            const addresses = await dns.lookup(this.address, { all: true });
            const adr = addresses.find(adr => adr.family === 6) ?? addresses.find(adr => adr.family === 4);
            if (!adr) {
                throw new Error(`host not found: ${this.address}`);
            }
            address = adr.address;
            this.socket = createDgramSocket({
                type: adr.family === 6 ? 'udp6' : 'udp4',
                sendBufferSize: this.sendBufferSize,
            });

            this.socket.on('close', this._onCloseOk);
        }

        this.socket.on('connect', this._onConnect);
        this.socket.on('error', this._onError);

        return new Promise((resolve, reject) => {
            try {
                if (!this.socket) {
                    return reject(new Error('socket gone before could connect'));
                }

                const callback = () => {
                    try {
                        this.socket?.off('error', reject);
                    } finally {
                        resolve();
                    }
                };
                if (this.transport === 'IPC') {
                    if (this.socket instanceof DgramSocket) {
                        return reject(new Error('socket changed type!?'));
                    }
                    this.socket.once('error', reject);
                    this.socket.connect(address, callback);
                } else {
                    this.socket.once('error', reject);
                    this.socket.connect(this.port, address, callback);
                }
            } catch (error) {
                reject(error);
            }
        });
    }

    get isConnected(): boolean {
        return this.socket !== null;
    }

    disconnect(callback: (error?: Error) => void): void;
    disconnect(): Promise<void>;

    disconnect(callback?: (error?: Error) => void): Promise<void>|void {
        if (!this.socket) {
            throw new Error('not connected');
        }

        if (callback) {
            if (this.socket instanceof DgramSocket) {
                this.socket.close(callback);
            } else {
                this.socket.end(() => {
                    try {
                        if (this.socket instanceof NetSocket) {
                            this.socket.destroy();
                        }
                    } catch (error) {
                        return callback(error instanceof Error ? error : new Error(String(error)));
                    }
                    callback();
                });
            }
        } else {
            return new Promise((resolve, reject) => {
                if (!this.socket) {
                    return resolve();
                }

                try {
                    if (this.socket instanceof DgramSocket) {
                        this.socket.close(resolve);
                    } else {
                        this.socket.end(() => {
                            try {
                                if (this.socket instanceof NetSocket) {
                                    this.socket.destroy();
                                }
                            } catch (error) {
                                return reject(error);
                            }
                            resolve();
                        });
                    }
                } catch (error) {
                    reject(error);
                }
            });
        }
    }

    on<Event extends keyof EventMap>(event: Event, callback: EventMap[Event]): void {
        this.callbacks[event].callbacks.push({ callback, once: false });
    }

    once<Event extends keyof EventMap>(event: Event, callback: EventMap[Event]): void {
        this.callbacks[event].callbacks.push({ callback, once: true });
    }

    off<Event extends keyof EventMap>(event: Event, callback: EventMap[Event]): void {
        const infos = this.callbacks[event];
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
        const infos = this.callbacks[event];
        const oldEmitActive = infos.emitActive;
        infos.emitActive = true;
        try {
            for (const info of infos.callbacks) {
                try {
                    info.callback.call(this, arg);
                } catch (error) {
                    if (event !== 'error' && this.callbacks.error.callbacks.length > 0) {
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

    private _send(data: string|Buffer): Promise<void> {
        return new Promise((resolve, reject) => {
            try {
                if (!this.socket) {
                    return reject(new Error('socket gone before data could be sent'));
                }

                if (this.socket instanceof DgramSocket) {
                    this.socket.send(data, (error, bytes) => {
                        if (error) {
                            return reject(error);
                        }
                        resolve();
                    });
                } else {
                    this.socket.write(data, error => {
                        if (error) {
                            return reject(error);
                        }
                        resolve();
                    });
                }
            } catch (error) {
                reject(error);
            }
        });
    }

    // Couldn't find a clear *documentation* of the format/data types involved, so I looked at
    // the actual source of class MetricLineReceiver in
    // https://github.com/graphite-project/carbon/blob/master/lib/carbon/protocols.py
    // -> value and timestamp are both parsed using Python's float() type constructor.

    write(path: string, value: number, timestamp: Date, tags?: Tags): Promise<void>;
    write(path: string, value: number, tags?: Tags): Promise<void>;

    async write(path: string, value: number, arg3?: Date|Tags, arg4?: Tags): Promise<void> {
        if (!this.socket) {
            throw new Error('not connected');
        }

        if (!PATH_REGEXP.test(path)) {
            throw new Error(`illegal path: ${JSON.stringify(path)}`);
        }

        let secs: number;
        let tags: Tags|undefined;

        if (arg3 instanceof Date) {
            secs = arg3.getTime() / 1000;
            tags = arg4;
        } else {
            secs = Date.now() / 1000;
            tags = arg3 ?? arg4;
        }

        let data: string|Buffer;
        if (tags) {
            const buf: (string|number)[] = [ path ];
            appendTags(buf, tags);
            buf.push(' ', value, ' ', secs, '\n');
            data = buf.join('');
        } else {
            data = `${path} ${value} ${secs}\n`;
        }

        return this._send(data);
    }

    async batchWrite(batch: MetricMap|MetricTuple[], timestamp: Date=new Date()): Promise<void> {
        const buf: (string|number)[] = [];
        const defaultSecs = timestamp.getTime() / 1000;

        if (Array.isArray(batch)) {
            for (const [path, value, arg3, arg4] of batch) {
                if (!PATH_REGEXP.test(path)) {
                    throw new Error(`illegal path: ${JSON.stringify(path)}`);
                }

                let secs: number;
                let tags: Tags|undefined;
                if (arg3 instanceof Date) {
                    secs = arg3.getTime() / 1000;
                    tags = arg4;
                } else {
                    secs = defaultSecs;
                    tags = arg3 ?? arg4;
                }

                buf.push(path);
                if (tags) {
                    appendTags(buf, tags);
                }

                buf.push(' ', value, ' ', secs, '\n');
            }
        } else {
            for (const path in batch) {
                const arg = batch[path];
                if (!PATH_REGEXP.test(path)) {
                    throw new Error(`illegal path: ${JSON.stringify(path)}`);
                }

                let value: number;
                let secs: number;

                buf.push(path);
                if (typeof arg === 'number') {
                    value = arg;
                    secs = defaultSecs;
                } else {
                    value = arg.value;
                    const { timestamp, tags } = arg;
                    secs = timestamp ? timestamp.getTime() / 1000 : defaultSecs;

                    if (tags) {
                        appendTags(buf, tags);
                    }
                }

                buf.push(' ', value, ' ', secs, '\n');
            }
        }

        const data = buf.join('');

        return this._send(data);
    }

    vwrite(path: string, value: number, timestamp: Date, tags?: Tags): void;
    vwrite(path: string, value: number, tags?: Tags): void;

    vwrite(path: string, value: number, arg3?: Date|Tags, arg4?: Tags): void {
        this.write(path, value, arg3 as any, arg4).catch(this._onError);
    }

    vbatchWrite(batch: MetricMap|MetricTuple[], timestamp?: Date): void {
        this.batchWrite(batch, timestamp).catch(this._onError);
    }
}
