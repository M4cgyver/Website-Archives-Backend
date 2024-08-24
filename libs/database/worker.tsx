import { sleep } from "bun";
import fs from "fs";
import { Client, Pool, type PoolClient, type PoolConfig } from 'pg';
import { dbConfig, type dbInsertResponseParams, type dbSearchResponsesParams, type dbSearchResponseResult, type dbRetrieveResponseResult, type dbRetrieveResponseFullResult } from "./types";

declare var self: Worker;

const timet = process.hrtime()[1];
let globalDbPool: Pool | null = null;

/*
const connectWorkerDb = async (): Promise<Client> => {
    if (!globalDbClient) {
        console.log("Connecting to the database...");
        globalDbClient = new Client(dbConfig);

        try {
            await globalDbClient.connect();
        } catch (error) {
            console.error("Error connecting to the database:", error);
            await sleep(1000);
            globalDbClient = null;
            return connectWorkerDb();
        }
    }

    return globalDbClient;
};
*/

// Create or reuse a connection pool
const connectWorkerPool = async (config?:PoolConfig): Promise<Pool> => {
    const conf = {...dbConfig, ...config}

    if (globalDbPool === null) {
        try {
            console.log(`Creating a new database connection pool ${timet} conns ${conf.max} ${conf.maxUses}...`);
            globalDbPool = new Pool({...dbConfig, ...config});
            globalDbPool.on('error', (err, client) => {
                console.error(`Database connection error at ${new Date().toISOString()}:`, err.message);

                if (client) {
                    console.error(`Client details: ${client}`);
                }
            });
        } catch (e) {
            console.log(`Failed to connect ${e.message} retrying...`)
            await new Promise(resolve=>setTimeout(resolve, 3000))
            return connectWorkerPool();
        }
    }

    return globalDbPool;
};

const connectWorkerDb = async () => {
    try {
        const pool = await connectWorkerPool();
        const client = await pool.connect();
        client.release();
        return { status: "connected" };
    } catch (error) {
        // Ensure `error` is of type `Error` to access `message`
        const errorMessage = error instanceof Error ? error.message : 'Unknown error';
        return { status: 'error', message: errorMessage };
    }
};

const setupWorkerDb = async () => {
    const client = await connectWorkerPool();

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

const closeWorkerDb = async () => {
    if (globalDbPool) {
        await globalDbPool.end();
        globalDbPool = null;
    }
}

// Insert response into the database
async function dbWorkerInsertResponse(params: dbInsertResponseParams): Promise<void> {
    const client = await connectWorkerPool();

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
async function dbWorkerSearchResponses(params: dbSearchResponsesParams): Promise<dbSearchResponseResult[]> {
    const {
        search_uri_a,
        limit_num_a = 32,
        offset_num_a = 0,
        search_ip_a = null,
        search_content_type_a = null,
    } = params;

    const client = await connectWorkerPool();

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
async function dbWorkerRetrieveResponse(uri_string: string): Promise<dbRetrieveResponseResult[]> {
    const client = await connectWorkerPool();

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
async function dbWorkerRetrieveResponseFull(uri_string: string): Promise<dbRetrieveResponseFullResult[]> {
    const client = await connectWorkerPool();

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
const dbWorkerRetrieveLatestResponses = async (total: number): Promise<dbSearchResponseResult[]> => {
    const client = await connectWorkerPool();

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
const handleMessage = async (event: MessageEvent) => {
    const { id, action, params } = event.data;
    let response;

    try {
        switch (action) {
            case 'connectDb':
                response = { status: 'sucess', data: await connectWorkerDb() };
                break;

            case 'setupDb':
                response = { status: 'sucess', data: await setupWorkerDb() };
                break;
            case 'closeDb':
                response = { status: 'sucess', data: await closeWorkerDb() };
                break;
            case 'dbInsertResponse':
                response = { status: 'sucess', data: await dbWorkerInsertResponse(params) };
                break;
            case 'dbSearchResponses':
                response = { status: 'sucess', data: await dbWorkerSearchResponses(params) };
                break;
            case 'dbRetrieveResponse':
                response = { status: 'sucess', data: await dbWorkerRetrieveResponse(params) };
                break;
            case 'dbRetrieveResponseFull':
                response = { status: 'sucess', data: await dbWorkerRetrieveResponseFull(params) };
                break;
            case 'dbRetrieveLatestResponses':
                response = { status: 'sucess', data: await dbWorkerRetrieveLatestResponses(params) };
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

self.onmessage = handleMessage;