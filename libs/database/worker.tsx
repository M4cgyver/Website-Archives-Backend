import { sleep } from "bun";
import fs from "fs";
import { Client, Pool, type PoolClient } from 'pg';
import { dbConfig, dbInsertResponseParams, dbSearchResponsesParams, dbSearchResponseResult, dbRetrieveResponseResult, dbRetrieveResponseFullResult } from "./types";

declare var self: Worker;

let globalDbPool: Pool | null = null;

/*
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
*/

// Create or reuse a connection pool
export const connectPool = async (pool?: Pool): Promise<Pool> => {
    if (!globalDbPool && pool) {
        globalDbPool = pool;
        return pool;
    }
    if (!globalDbPool) {
        console.log("Creating a new database connection pool...");
        globalDbPool = new Pool(dbConfig);

        try {
            // Test the pool by connecting a client
            const client = await globalDbPool.connect();
            client.release(); // Release the client back to the pool
        } catch (error) {
            console.error("Error creating the database connection pool:", error);
            await sleep(1000);
            globalDbPool = null;
            return connectPool();
        }
    }

    return globalDbPool;
};

export const connectDb = async () => {
    try {
        const pool = await connectPool();
        const client = await pool.connect();
        client.release();
        return { status: "connected" };
    } catch (error) {
        // Ensure `error` is of type `Error` to access `message`
        const errorMessage = error instanceof Error ? error.message : 'Unknown error';
        return { status: 'error', message: errorMessage };
    }
};

export const setupDb = async () => {
    const client = await connectPool();

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
    const client = await connectPool();

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

    const client = await connectPool();

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
    const client = await connectPool();

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
    const client = await connectPool();

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
    const client = await connectPool();

    const query = `SELECT * FROM latest_responses($1)`;

    try {
        const result = await client.query(query, [total]);
        return result.rows;
    } catch (error) {
        console.error("Error retrieving latest responses:", error);
        throw error;
    }
};

// Handle incoming messages
self.onmessage = async (event: MessageEvent) => {
    const { id, action, params } = event.data;
    let response;
    
    try {
        switch (action) {
            case 'connectDb':
                response = { status: 'sucess', data: await connectDb() };
                break;

            case 'setupDb':
                response = { status: 'sucess', data: await setupDb() };
                break;
            case 'dbInsertResponse':
                response = { status: 'sucess', data: await dbInsertResponse(params) };
                break;
            case 'dbSearchResponses':
                response = { status: 'sucess', data: await dbSearchResponses(params) };
                break;
            case 'dbRetrieveResponse':
                response = { status: 'sucess', data: await dbRetrieveResponse(params) };
                break;
            case 'dbRetrieveResponseFull':
                response = { status: 'sucess', data: await dbRetrieveResponseFull(params) };
                break;
            case 'dbRetrieveLatestResponses':
                response = { status: 'sucess', data: await dbRetrieveLatestResponses(params) };
                break;

            // Add other cases for different actions as needed
            default:
                response = { status: 'error', message: `Unknown action ${action}` };
        }
    } catch (e) {
        response = { status: 'error', message: 'Worker error' };
    }

    postMessage({ id, ...response });
};