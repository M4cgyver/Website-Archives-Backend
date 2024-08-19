import { sleep } from "bun";
import fs from "fs";
import { Client } from 'pg';

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
const dbConfig = {
    user: 'bun_user', // Username for PostgreSQL connection
    host: 'm4cgyver-archives-backend-postgres', // Hostname or service name defined in Docker Compose
    database: 'webpages', // Database name
    password: 'bun_password', // Password for PostgreSQL connection
    port: 5432, // Port number for PostgreSQL connection
};
let globalDbClient: Client | null = null;

export const connectDb = async (): Promise<Client> => {
    if (!globalDbClient) {
        console.log("Connecting to the database...");
        globalDbClient = new Client(dbConfig);

        try {
            await globalDbClient.connect();
        } catch (error) {
            console.error("Error connecting to the database:", error);
            await sleep(1000);
            globalDbClient = null;
            return connectDb();
        }
    }

    return globalDbClient;
};

export const setupDb = async () => {
    const client = await connectDb();

    try {
        console.time("Resetting db");
        const sqlSetup = fs.readFileSync('libs/database/setup.pg.sql', 'utf8');
        console.timeEnd("Resetting db");
        console.log("Successfully reset db");
        await client.query(sqlSetup);
    } catch (err) {
        console.error("Error setting up the database:", err);
        throw err;
    }
};

// Insert response into the database
export async function dbInsertResponse(params: dbInsertResponseParams): Promise<void> {
    const client = await connectDb();

    const {
        uri_string,
        file_string,
        content_type_string,
        resource_type_string,
        record_length,
        record_offset,
        content_offset,
        content_length,
        status,
        meta,
    } = params;

    const query = `
        SELECT insert_response(
            $1, $2, $3, $4, $5, $6, $7, $8, $9, $10
        );
    `;

    try {
        await client.query(query, [
            uri_string,
            file_string,
            content_type_string,
            resource_type_string,
            record_length,
            record_offset,
            content_offset,
            content_length,
            status,
            JSON.stringify(meta),
        ]);
    } catch (error) {
        console.error("Error inserting response:", error);
        throw error;
    }
}

// Search for responses in the database
export async function dbSearchResponses(params: dbSearchResponsesParams): Promise<dbSearchResponseResult[]> {
    const {
        search_uri_a,
        limit_num_a = 32,
        offset_num_a = 0,
        search_ip_a = null,
        search_content_type_a = null,
    } = params;

    const client = await connectDb();

    const query = `
        SELECT * FROM search_responses(
            $1, $2, $3, $4, $5
        );
    `;

    try {
        const result = await client.query(query, [
            search_uri_a,
            limit_num_a,
            offset_num_a,
            search_ip_a,
            search_content_type_a,
        ]);

        return result.rows;
    } catch (error) {
        console.error("Error searching responses:", error);
        throw error;
    }
}

// Retrieve response by URI
export async function dbRetrieveResponse(uri_string: string): Promise<dbRetrieveResponseResult[]> {
    const client = await connectDb();

    const query = `
        SELECT * FROM retrieve_response(
            $1
        );
    `;

    try {
        const result = await client.query(query, [uri_string]);
        return result.rows;
    } catch (error) {
        console.error("Error retrieving response:", error);
        throw error;
    }
}

// Retrieve full response details by URI
export async function dbRetrieveResponseFull(uri_string: string): Promise<dbRetrieveResponseFullResult[]> {
    const client = await connectDb();

    const query = `
        SELECT * FROM retrieve_response_full(
            $1
        );
    `;

    try {
        const result = await client.query(query, [uri_string]);
        return result.rows;
    } catch (error) {
        console.error("Error retrieving full response:", error);
        throw error;
    }
}

// Retrieve the latest responses
export const dbRetrieveLatestResponses = async (total: number): Promise<dbSearchResponseResult[]> => {
    const client = await connectDb();

    const query = `SELECT * FROM latest_responses($1)`;

    try {
        const result = await client.query(query, [total]);
        return result.rows;
    } catch (error) {
        console.error("Error retrieving latest responses:", error);
        throw error;
    }
};
