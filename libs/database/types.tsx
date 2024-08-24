import type { PoolConfig } from "pg";

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
export const dbConfig:PoolConfig = {
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

export const getWorker = async () => {
    if (!worker) {
        const build = await Bun.build({
            entrypoints: ['libs/database/worker.tsx'],
            outdir: 'libs/database/build',
            target: (process.env.WORKER_TARGET as Target) ?? "node",
            minify: true,
        })

        worker = new Worker(build.outputs[0].path);

        worker.onmessage = event => {
            const { id, status, data, message } = event.data;
            const handlers = promises.get(id);

            if (handlers) {
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