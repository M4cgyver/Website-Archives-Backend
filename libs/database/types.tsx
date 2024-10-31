import type { Target } from "bun";
import type { PoolConfig } from "pg";
import net, { Socket } from 'net';

// Type definitions for responses
export interface dbResponseMeta {
    [key: string]: any;
}

export interface dbSearchResponseResult {
    response_id_r: number;
    uri_r: string;
    file_r: string;
    content_type_r: string;
    resource_type_r: string;
    ip_r: string;
    record_length_r: bigint;
    record_offset_r: bigint;
    content_offset_r: bigint;
    content_length_r: bigint;
    status_r: number;
    meta_r: dbResponseMeta;
    date_added_r: Date;
}

export interface dbInsertResponseParams {
    uri_string: string;
    file_string: string;
    content_type_string: string;
    resource_type_string: string;
    record_length: bigint;
    record_offset: bigint;
    content_offset: bigint;
    content_length: bigint;
    status: number;
    meta: dbResponseMeta;
}

export interface dbUpdateFileProgressParams {
    file:string,
    progress:number;
}

export interface dbSearchResponsesParams {
    search_uri_a: string;
    limit_num_a?: number;
    offset_num_a?: number;
    search_ip_a?: string;
    search_content_type_a?: string;
}

export interface dbRetrieveResponseResult {
    response_id_r: number;
    uri_r: string;
    file_r: string;
    content_type_r: string;
    resource_type_r: string;
    ip_r: string;
    record_length_r: bigint;
    record_offset_r: bigint;
    content_offset_r: bigint;
    content_length_r: bigint;
    status_r: number;
    meta_r: dbResponseMeta;
}

export interface dbRetrieveResponseFullResult {
    response_id_r: number;
    uri_r: string;
    file_r: string;
    content_type_r: string;
    resource_type_r: string;
    ip_r: string;
    record_length_r: bigint;
    record_offset_r: bigint;
    content_offset_r: bigint;
    content_length_r: bigint;
    status_r: number;
    meta_r: dbResponseMeta;
    date_added_r: string; // Date as ISO string
}

// Declare a global variable for the database client
export const dbConfig: PoolConfig = {
    user: 'bun_user', // Username for PostgreSQL connection
    host: 'm4cgyver-archives-backend-postgres', // Hostname or service name defined in Docker Compose
    database: 'webpages', // Database name
    password: 'bun_password', // Password for PostgreSQL connection
    port: 5432, // Port number for PostgreSQL connection
};

// Singleton worker instance
let worker: Worker | null = null;
// Singleton promises map
const promises = new Map<number, { resolve: (value: any) => void, reject: (reason?: any) => void }>();
// In your worker management file
export const getWorker = async (options?: { worker?: Worker }) => {
    if (!worker) {
        const build = await Bun.build({
            entrypoints: ['libs/database/worker.tsx'],
            outdir: 'libs/database/build',
            target: (process.env.WORKER_TARGET as Target) ?? "node",
            minify: false,
        });

        worker = new Worker(build.outputs[0].path);

        worker.onmessage = event => {
            const { id, status, data, message, action } = event.data;
            const handlers = promises.get(id);

            if (action) {
                if (action == "log") {
                    console.log(`WS-DB: ${message}`);
                }
            } else if (handlers) {
                if (status === 'error') {
                    handlers.reject(message);
                } else {
                    handlers.resolve(data);
                }

                // Clean up the handlers once they are used
                promises.delete(id);
            }
        };
    }

    return { worker, promises };
};


// In your net socket management file
export const getNetSocket = async (
    options: { host: string; port: number },
    promises: Map<number, { resolve: (value: any) => void; reject: (reason?: any) => void; }> // Pass promises as an argument
): Promise<{ socket: net.Socket; promises: Map<number, { resolve: (value: any) => void; reject: (reason?: any) => void; }> }> => {
    const { host, port } = options;

    return new Promise<{ socket: net.Socket; promises: Map<number, { resolve: (value: any) => void; reject: (reason?: any) => void; }> }>((resolve) => { // Removed reject
        // Create a net socket connecting to host and port
        const socket = net.createConnection({ host, port }, () => {
            postMessage({ action: 'log', message: `Connected to ${host}:${port}` }); // Changed console.log to postMessage
            resolve({ socket, promises }); // Resolve the promise with the socket instance and promises map
        });

        socket.on('data', (buffer: Buffer) => {
            const data = buffer.toString("utf-8");

            try {
                const { id, status, data: responseData, message, action } = JSON.parse(data);

                const handlers = promises.get(id); // Get the handler based on the ID

                if (action) {
                    if (action === "log") {
                        console.log(`WS-NET: ${message}`);
                    }
                } else if (handlers) {
                    if (status === 'error') {
                        handlers.reject(message);
                    } else {
                        handlers.resolve(responseData); // Use responseData to avoid conflict with the variable name
                    }

                    // Clean up the handlers once they are used
                    promises.delete(id);
                }
            } catch (error:any) {
                postMessage({ action: 'log', message: 'Failed to parse data: ' + error.message }); // Changed console.error to postMessage
            }
        });

        // Handle socket errors
        socket.on('error', (err: Error) => {
            postMessage({ action: 'log', message: `Socket error: ${err.message}` }); // Changed console.error to postMessage
            // Removed reject logic
        });

        // Handle socket disconnection
        socket.on('end', () => {
            postMessage({ action: 'log', message: 'Disconnected from the server' }); // Changed console.log to postMessage
        });
    });
};
