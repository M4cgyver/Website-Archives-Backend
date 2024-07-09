import fs from "fs";
import { Client } from 'pg';

// Declare a global variable for the database client
let globalDbClient: Client | null = null;

export const connectDb = async (retries: number = 5): Promise<Client> => {
    if (!globalDbClient) {
        // Initialize the client if it hasn't been initialized yet
        globalDbClient = new Client({
            user: 'bun_user', // Username for PostgreSQL connection
            host: 'm4cgyver-archives-backend-postgres', // Hostname or service name defined in Docker Compose
            database: 'webpages', // Database name
            password: 'bun_password', // Password for PostgreSQL connection
            port: 5432, // Port number for PostgreSQL connection
        });

        // Event handler for connection errors
        globalDbClient.on('error', (err: any) => {
            console.error('Error connecting to PostgreSQL:', err);
        });

        for (let attempt = 0; attempt < retries; attempt++) {
            try {
                await globalDbClient.connect();
                console.log("Connected to the database");
                break; // Exit the loop on successful connection
            } catch (err) {
                console.error(`Failed to connect to the database (attempt ${attempt + 1}/${retries}):`, err);
                globalDbClient = null; // Reset client in case of error
                if (attempt < retries - 1) {
                    await new Promise(resolve => setTimeout(resolve, 5000)); // Wait 5 seconds before retrying
                } else {
                    throw err; // Throw error after exhausting retries
                }
            }
        }
    }

    return globalDbClient;
};


export const setupDb = async () => {
    const client = await connectDb();

    try {
        const sqlSetup = fs.readFileSync('libs/database/setup.pg.sql', 'utf8');
        return client.query(sqlSetup);
    } catch (err) {
        throw err;
    }
};

export const dbInsertResponse = async (
    uri: string, 
    location: string | null, 
    type: string, 
    filename: string, 
    offsetHeader: bigint, 
    offsetContent: bigint, 
    contentLength: bigint, 
    lastModified: Date | null, 
    date: Date | null, 
    status: number,
    transferEncoding: string | null,
) => {
    const client = await connectDb();

    const query = `
        SELECT insert_response($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11);
    `;

    const values = [uri, location, type, filename, offsetHeader, offsetContent, contentLength, lastModified, date, status, (transferEncoding) ? transferEncoding : "identity"];

    try {
        const ret = await client.query(query, values);
        //console.log('Response inserted successfully', uri);
        return ret;
    } catch (err) {
        console.error('Failed to insert response', err);
        throw err;
    }
};

// Function to retrieve responses
export const dbRetrieveResponses = async (
    uri: string | null = null,
    date: Date | undefined = undefined,
    limit: bigint | undefined = undefined,
    page: bigint | undefined = undefined,
    status: number | undefined = undefined,
    type: string | null = null
): Promise<any> => {
    const client = await connectDb();

    const query = `
        SELECT * FROM retrieve_responses($1, $2, $3, $4, $5, $6);
    `;

    const values = [uri, date, limit, page, status, type];

    try {
        const { rows } = await client.query(query, values);
        return rows;
    } catch (err) {
        console.error('Failed to retrieve responses', err);
        throw err;
    } 
};
