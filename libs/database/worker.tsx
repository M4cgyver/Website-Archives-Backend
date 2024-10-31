import fs from "fs";
import { Client, Pool, type PoolClient, type PoolConfig } from "pg";
import {
    dbConfig,
    type dbInsertResponseParams,
    type dbSearchResponsesParams,
    type dbSearchResponseResult,
    type dbRetrieveResponseResult,
    type dbRetrieveResponseFullResult
} from "./types";
import net, { Socket } from "net";

declare var self: Worker;

const timet = process.hrtime()[1];
let globalDbPool: Pool | null = null;
let server: net.Server | null = null;
const port = 9824;
const host = "0.0.0.0";

//TODO: Implement in the db later
const parseWarcFilesProgress: Record<string, number> = {};

// Custom log function to send messages to the parent thread
const log = (message: string) => {
    postMessage({ action: 'log', message });
};

// Setup a worker socket server to accept connections
const setupWorkerDbSocket = () => {
    log("2 Setting up db socket...");
    server = net.createServer((socket: Socket) => {
        log(`Client connected: ${socket.remoteAddress}:${socket.remotePort}`);

        socket.on("data", async (data: Buffer) => {
            const jsonarrstr = `[${data.toString("utf-8").replace(/}{/g, '},{')}]`
            try {
                //const response = await handleMessage(JSON.parse(data.toString("utf-8")));
                //socket.write(JSON.stringify(response) + "\n");
                const arr = JSON.parse(jsonarrstr);
                arr.forEach((data:any) => handleMessage(data).then(ret=>socket.write(JSON.stringify(ret))));
            } catch (error: any) {
                log("Error handling message: " + error.message + "\r\n" + data.toString("utf-8") + "\r\n" + jsonarrstr);
                socket.write(JSON.stringify({ status: "error", message: error.message }) + "\n");
            }
        });

        socket.on("end", () => log("Client disconnected"));
        socket.on("error", (err: Error) => log("Socket error: " + err.message));
    });

    server.listen(port, host, () => log(`Server listening on ${host}:${port}`));
    server.on("error", (err: Error) => log("Server error: " + err.message));
};

// Create or reuse a connection pool
const connectWorkerPool = async (config?: PoolConfig): Promise<Pool> => {
    const conf = { ...dbConfig, ...config };

    if (!globalDbPool) {
        try {
            log(`Creating a new pool at ${timet}`);
            globalDbPool = new Pool(conf);

            globalDbPool.on("error", (err: Error) => {
                log(`Database error: ${err.message}`);
            });
        } catch (e: any) {
            log("Failed to connect: " + e.message);
            await new Promise(resolve => setTimeout(resolve, 3000));
            return connectWorkerPool(config);
        }
    }
    return globalDbPool;
};

// Establish a connection and start the server
const connectWorkerDb = async () => {
    try {
        log("Connecting to db...");
        const pool = await connectWorkerPool();
        const client = await pool.connect();
        log("Database connection established");
        client.release();
        log("Creating net socket...");
        setupWorkerDbSocket();
        return { status: "connected" };
    } catch (error: any) {
        const errorMessage = error instanceof Error ? error.message : "Unknown error";
        return { status: "error", message: errorMessage };
    }
};

// Setup the database with SQL script
const setupWorkerDb = async () => {
    const pool = await connectWorkerPool();

    try {
        console.time("Resetting db");
        const sqlSetup = fs.readFileSync("libs/database/setup.pg.sql", "utf8");
        await pool.query(sqlSetup);
        console.timeEnd("Resetting db");
        log("Successfully reset the database");
    } catch (err: any) {
        log("Error setting up the database: " + err.message);
        throw err;
    }
};

// Close the worker DB and server
const closeWorkerDb = async () => {
    if (globalDbPool) {
        await globalDbPool.end();
        globalDbPool = null;
    }
    if (server) {
        server.close();
        server = null;
    }
};

// Insert response into the database
const dbWorkerInsertResponse = async (params: dbInsertResponseParams): Promise<void> => {
    const pool = await connectWorkerPool();
    const query = `
    SELECT insert_response(
      $1, $2, $3, $4, $5, $6, $7, $8, $9, $10
    );
  `;
    const values = [
        params.uri_string, params.file_string, params.content_type_string,
        params.resource_type_string, params.record_length, params.record_offset,
        params.content_offset, params.content_length, params.status,
        JSON.stringify(params.meta)
    ];

    try {
        await pool.query(query, values);
    } catch (error: any) {
        log("Error inserting response: " + error.message);
        throw error;
    }
};

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

    console.log(`looking for uri`, uri_string)

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
const handleMessage = async (data: { id: number; action: string; params: any }) => {
    const { id, action, params } = data;
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
            
            case 'dbUpdateFileProgress':
                const {file, progress} = params;
                parseWarcFilesProgress[file] = progress;
                //log(`progress: ${JSON.stringify(parseWarcFilesProgress)}`)
                break;

            case 'dbRetrieveFileProgress':
                response = { status: 'sucess', data: parseWarcFilesProgress};
                ///log(`RETURN progress: ${JSON.stringify(parseWarcFilesProgress)}`)
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
    } catch (error: any) {
        log("Worker error: " + error.message);
        response = { status: "error", message: error.message };
    }

    return { id, ...response };
};

// Listen for messages from the parent thread
self.onmessage = async (event) => {
    const response = await handleMessage(event.data);
    postMessage(response);
};

// Initial log
log("Hello world!");
