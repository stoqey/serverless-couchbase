import {SofaConnection, Cluster, Collection, QueryResult, startSofa} from '@stoqey/sofa';

export interface SofaArgs {
    connectionString: string;
    bucketName: string;
    username: string;
    password: string;
}

export type ServerlessConfig = {
    /**
     * String or Function  Backoff algorithm to be used when retrying connections. Possible values are full and decorrelated, or you can also specify your own algorithm. See Connection Backoff for more information.  full
     */
    backoff?: string | Function;
    /**
     * Integer  Number of milliseconds added to random backoff values.  2
     */
    base?: number;
    /**
     * Integer  Maximum number of milliseconds between connection retries.  100
     */
    cap?: number;
    /**
     * Object  A Couchbase configuration object as defined here  {}
     */
    couchbaseConfig?: SofaArgs;
    /**
     * Number  The percentage of total connections to use when connecting to your Couchbase server. A value of 0.75 would use 75% of your total available connections.  0.8
     */
    connUtilization?: number;
    /**
     * Boolean  Flag indicating whether or not you want serverless-Couchbase to manage Couchbase connections for you.  true
     */
    manageConns?: boolean;
    /**
     * Integer  The number of milliseconds to cache lookups of @@max_connections.  15000
     */
    maxConnsFreq?: number;
    /**
     * Integer  Maximum number of times to retry a connection before throwing an error.  50
     */
    maxRetries?: number;
    /**
     * function  Event callback when the Couchbase connection fires an error.
     */
    onError?: Function;
    /**
     * function  Event callback when Couchbase connections are explicitly closed.
     */
    onClose?: Function;
    /**
     * function  Event callback when connections are succesfully established.
     */
    onConnect?: Function;
    /**
     * function  Event callback when connection fails.
     */
    onConnectError?: Function;
    /**
     * function  Event callback when connections are explicitly killed.
     */
    onKill?: Function;
    /**
     * function  Event callback when a connection cannot be killed.
     */
    onKillError?: Function;
    /**
     * function  Event callback when connections are retried.
     */
    onRetry?: Function;
    /**
     * Integer  The number of milliseconds to cache lookups of current connection usage.  0
     */
    usedConnsFreq?: number;
    /**
     * Integer  The maximum number of seconds that a connection can stay idle before being recycled.  900
     */
    zombieMaxTimeout?: number;
    /**
     * Integer  The minimum number of seconds that a connection must be idle before the module will recycle it.  3
     */
    zombieMinTimeout?: number;
};

/**
 * This module manages Couchbase connections in serverless applications.
 * More detail regarding the Couchbase ORM-used can be found here:
 * https://github.com/stoqey/sofa
 * @author Ceddy Muhoza <sup@ceddy.org>
 */

export class ServerlessCouchbaseConnection implements ServerlessConfig {
    private static _instance: ServerlessCouchbaseConnection;

    // Args
    counter: any;
    client: Cluster;
    retries: number;
    errors: any;
    this: any;
    bucket: any;
    cluster: any;

    public static get Instance(): ServerlessCouchbaseConnection {
        return this._instance || (this._instance = new this());
    }

    private constructor() {}

    backoff?: string | Function;
    base?: number;
    cap?: number;
    couchbaseConfig?: SofaArgs = null;
    connUtilization?: number;
    manageConns?: boolean;
    maxConnsFreq?: number;
    maxRetries?: number;
    onError?: Function;
    onClose?: Function;
    onConnect?: Function;
    onConnectError?: Function;
    onKill?: Function;
    onKillError?: Function;
    onRetry?: Function;
    usedConnsFreq?: number;
    zombieMaxTimeout?: number;
    zombieMinTimeout?: number;

    getCounter = () => this.counter;
    incCounter = () => this.counter++;
    resetCounter = () => (this.counter = 0);
    getClient = () => this.client;
    resetClient = () => (this.client = null);
    resetRetries = () => (this.retries = 0);
    getErrorCount = () => this.errors;
    getConfig = (): SofaArgs => this.couchbaseConfig;
    config = (args: SofaArgs): SofaArgs => (this.couchbaseConfig = args);
    delay = (ms: number) => new Promise((res) => setTimeout(res, ms));
    randRange = (min: number, max: number) => Math.floor(Math.random() * (max - min + 1)) + min;
    fullJitter = () => this.randRange(0, Math.min(this.cap, this.base * 2 ** this.retries));
    decorrelatedJitter = (sleep = 0) => Math.min(this.cap, this.randRange(this.base, sleep * 3));

    tooManyConnsErrors = [
        'PROTOCOL_CONNECTION_LOST', // if the connection is lost
        'PROTOCOL_SEQUENCE_TIMEOUT', // if the connection times out
        'ETIMEDOUT', // if the connection times out

        'CONNECTION_REFUSED', // Connection refused.
        'DRIVER_OPEN',
        'NO_CONNECTION',
        'UNREACHABLE_NETWORK',
    ];

    /**
     * start
     */
    public init(params: ServerlessConfig): SofaArgs {
        /********************************************************************/
        /**  INITIALIZATION                                                **/
        /********************************************************************/
        const cfg: ServerlessConfig = params;

        // Set defaults for connection management
        this.manageConns = cfg.manageConns === false ? false : true; // default to true
        this.cap = Number.isInteger(cfg.cap) ? cfg.cap : 100; // default to 100 ms
        this.base = Number.isInteger(cfg.base) ? cfg.base : 2; // default to 2 ms
        this.maxRetries = Number.isInteger(cfg.maxRetries) ? cfg.maxRetries : 50; // default to 50 attempts
        this.backoff =
            typeof cfg.backoff === 'function'
                ? cfg.backoff
                : cfg.backoff && ['full', 'decorrelated'].includes(cfg.backoff.toLowerCase())
                ? cfg.backoff.toLowerCase()
                : 'full'; // default to full Jitter
        this.connUtilization = !isNaN(cfg.connUtilization) ? cfg.connUtilization : 0.8; // default to 0.7
        this.zombieMinTimeout = Number.isInteger(cfg.zombieMinTimeout) ? cfg.zombieMinTimeout : 3; // default to 3 seconds
        this.zombieMaxTimeout = Number.isInteger(cfg.zombieMaxTimeout)
            ? cfg.zombieMaxTimeout
            : 60 * 15; // default to 15 minutes
        this.maxConnsFreq = Number.isInteger(cfg.maxConnsFreq) ? cfg.maxConnsFreq : 15 * 1000; // default to 15 seconds
        this.usedConnsFreq = Number.isInteger(cfg.usedConnsFreq) ? cfg.usedConnsFreq : 0; // default to 0 ms

        this.this.onConnect = typeof cfg.onConnect === 'function' ? cfg.onConnect : () => {}; // Event handlers
        this.onConnectError =
            typeof cfg.onConnectError === 'function' ? cfg.onConnectError : () => {};
        this.onRetry = typeof cfg.onRetry === 'function' ? cfg.onRetry : () => {};
        this.onClose = typeof cfg.onClose === 'function' ? cfg.onClose : () => {};
        this.onError = typeof cfg.onError === 'function' ? cfg.onError : () => {};
        this.onKill = typeof cfg.onKill === 'function' ? cfg.onKill : () => {};
        this.onKillError = typeof cfg.onKillError === 'function' ? cfg.onKillError : () => {};

        const connCfg: SofaArgs =
            typeof cfg.couchbaseConfig === 'object' && !Array.isArray(cfg.couchbaseConfig)
                ? cfg.couchbaseConfig
                : ({} as any);
        // Set Couchbase configs
        this.config(connCfg);
        return connCfg;
    }

    /**
     * getCollection
     */
    public getCollection(): Collection {
        return this.bucket.defaultCollection();
    }

    /**
     * shutdown cluster
     * WARNING: Never recommended to close share cluster connection
     */
    public shutdown(): void {
        return this.cluster.close();
    }

    /********************************************************************/
    /**  CONNECTION MANAGEMENT FUNCTIONS                               **/
    /********************************************************************/

    // Public connect method, handles backoff and catches
    // TOO MANY CONNECTIONS errors
    connect = async (wait?: number) => {
        try {
            await this._connect();
        } catch (e) {
            if (this.tooManyConnsErrors.includes(e.code) && this.retries < this.maxRetries) {
                this.retries++;
                wait = Number.isInteger(wait) ? wait : 0;
                const sleep =
                    this.backoff === 'decorrelated'
                        ? this.decorrelatedJitter(wait)
                        : typeof this.backoff === 'function'
                        ? this.backoff(wait, this.retries)
                        : this.fullJitter();
                this.onRetry(
                    e,
                    this.retries,
                    sleep,
                    typeof this.backoff === 'function' ? 'custom' : this.backoff
                ); // fire onRetry event
                await this.delay(sleep).then(() => this.connect(sleep));
            } else {
                this.onConnectError(e); // Fire onConnectError event
                throw new Error(e);
            }
        }
    }; // end connect

    // Internal connect method
    _connect = (): Promise<boolean> => {
        if (this.client === null) {
            // if no client connection exists

            this.resetCounter(); // Reset the total use counter

            // Return a new promise
            return new Promise((resolve, reject) =>
                startSofa(this.couchbaseConfig).then((started: boolean) => {
                    if (!started) {
                        this.resetClient();
                        return reject(new Error('error connecting to the couchbase'));
                    }

                    this.client = SofaConnection.Instance.cluster;
                    this.resetRetries();
                    this.onConnect(this.client);
                    return resolve(started);
                })
            ); // end promise

            // Else the client already exists
        } else {
            return Promise.resolve(true);
        } // end if-else
    }; // end _connect

    // Function that explicitly closes the Couchbase connection.
    public quit = (): void => {
        if (this.client !== null) {
            this.client.close(); // Quit the connection.
            this.resetClient(); // reset the client to null
            this.resetCounter(); // reset the reuse counter
            this.onClose(); // fire onClose event
        }
    };

    /********************************************************************/
    /**  QUERY FUNCTIONS                                               **/
    /********************************************************************/

    // Main query function
    public query = async (query: string, options?: any): Promise<QueryResult> => {
        // Establish connection
        await this.connect();

        // Run the query
        return new Promise((resolve, reject) => {
            if (this.client !== null) {
                // If no args are passed in a transaction, ignore query
                if (!query) {
                    return resolve({rows: [], meta: {}});
                }
                this.client.query(query, options, async (err: Error, results: QueryResult) => {
                    // TODO check error message
                    if (err) {
                        this.client.close(); // destroy connection on timeout
                        this.resetClient(); // reset the client
                        reject(err); // reject the promise with the error
                    }
                    // if (err && err.code === 'PROTOCOL_SEQUENCE_TIMEOUT') {
                    //     this.client.close(); // destroy connection on timeout
                    //     this.resetClient(); // reset the client
                    //     reject(err); // reject the promise with the error
                    // } else if (
                    //     err &&
                    //     (/^PROTOCOL_ENQUEUE_AFTER_/.test(err.code) ||
                    //         err.code === 'PROTOCOL_CONNECTION_LOST' ||
                    //         err.code === 'EPIPE')
                    // ) {
                    //     this.resetClient(); // reset the client
                    //     return resolve(this.query(...args)); // attempt the query again
                    // } else if (err) {
                    //     if (this && this.rollback) {
                    //         await this.query('ROLLBACK');
                    //         this.rollback(err);
                    //     }
                    //     reject(err);
                    // }
                    return resolve(results);
                });
            }
        });
    }; // end query
}

export default ServerlessCouchbaseConnection;
