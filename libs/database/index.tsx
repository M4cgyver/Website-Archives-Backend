import type { PoolConfig } from "pg";
import {
    getNetSocket,
    getWorker,
    type dbInsertResponseParams,
    type dbRetrieveResponseFullResult,
    type dbRetrieveResponseResult,
    type dbSearchResponseResult,
    type dbSearchResponsesParams,
    type dbUpdateFileProgressParams
} from "./types";
import { genid } from "../genid";
import net, { Socket } from "net";

let worker: Worker | null = null;
let promises: Map<number, { resolve: (value: any) => void; reject: (reason?: any) => void; }> = new Map();
let method: "post" | "net" | null = null;
let socket: Socket | null = null;

// Function to handle incoming messages from the socket
const handleSocketMessage = (data: Buffer) => {
    const str = `[${data.toString("utf-8").trim().replace(/}{/g, '},{')}]`
    const jsonarrstr = JSON.parse(str)

    //console.log(`parsing ${str}`);

    jsonarrstr.forEach((responsestr:any) => {
        try {
            //console.log(responsestr)
            const { id, result, error } = responsestr;
    
            const promise = promises.get(id);
            if (promise) {
                promises.delete(id);
                if (error) {
                    promise.reject(error);
                } else {
                    promise.resolve(result);
                }
            }

            //console.log("parsed")
        } catch (err:any) {
            console.log(`Error ${err.message}: \r\n` + jsonarrstr + "\r\n" + responsestr)
        }
    });
};

const sendMessage = (options: { id: number; action: string; params?: any }) => {
    const { id, action, params } = options;

    // Ensure params is either included as null or not included at all
    const messageToSend = {
        id,
        action,
        params: params !== undefined ? params : {}, // Set to null if undefined
    };

    switch (method) {
        case "post":
            worker?.postMessage(messageToSend);
            break;
        case "net":
            if (socket) {
                socket.write(
                    JSON.stringify(messageToSend, (_, v) => typeof v === 'bigint' ? v.toString() : v),
                    (err) => {
                        if (err) {
                            console.error(`Error writing to socket: ${err.message}`);
                        }
                    });
            }
            break;
    }
};


const callAction = (action: string, params?: any): Promise<any> => {
    return new Promise((resolve, reject) => {
        const id = genid(); // Unique ID for each request
        promises.set(id, { resolve, reject });
        sendMessage({ id, action, params });
    });
};

export const setupDb = async (): Promise<void> => callAction('setupDb');
export const dbUpdateFileProgress = async (params: dbUpdateFileProgressParams): Promise<void> => callAction('dbUpdateFileProgress', params);
export const dbRetrieveFileProgress = async (): Promise<any> => callAction('dbRetrieveFileProgress');
export const dbInsertResponse = async (params: dbInsertResponseParams): Promise<void> => callAction('dbInsertResponse', params);
export const dbSearchResponses = async (params: dbSearchResponsesParams): Promise<any[]> => callAction('dbSearchResponses', params);
export const dbRetrieveResponse = async (uri_string: string): Promise<dbRetrieveResponseResult[]> => callAction('dbRetrieveResponse', uri_string);
export const dbRetrieveResponseFull = async (uri_string: string): Promise<dbRetrieveResponseFullResult[]> => callAction('dbRetrieveResponseFull', uri_string);
export const dbRetrieveLatestResponses = async (total: number): Promise<dbSearchResponseResult[]> => callAction('dbRetrieveLatestResponses', total);
export const dbConnectWorker = async (): Promise<void> => callAction('connectDb');

export const connectDb = async (params?: PoolConfig, options?: { connType: "post" | "net"; host?: string; port?: number }): Promise<any> => {
    const { connType = "post" } = options || {};

    switch (connType) {
        case "post":
            const workerResult = await getWorker();
            worker = workerResult.worker;
            promises = workerResult.promises;
            method = "post"; // Set method to "post"
            console.log("Connecting worker")
            await dbConnectWorker();
            break;

        case "net":
            if (!options?.host || !options?.port) {
                throw new Error("Host and port must be provided for net connection");
            }
            const netSocketResult = await getNetSocket({ host: options.host, port: options.port }, promises);
            socket = netSocketResult.socket;
            promises = netSocketResult.promises;
            method = "net"; // Set method to "net"

            // Handle incoming messages from the socket
            socket.on('data', handleSocketMessage);
            socket.on('error', (err) => {
                console.error(`Socket error: ${err.message}`);
                promises.forEach(p => p.reject(err)); // Reject all promises on socket error
                promises.clear();
            });
            socket.on('end', () => {
                console.log('Socket connection closed');
                socket = null; // Clear socket on end
            });
            break;
    }
};

export const closeDb = async (): Promise<void> => {
    switch (method) {
        case "post":
            if (worker) {
                await callAction('closeDb');
                worker.terminate(); // Properly terminate the worker
                worker = null;      // Nullify the worker reference
            }
            break;

        case "net":
            if (socket) {
                socket.destroy();   // Ensure the socket is properly closed
                socket = null;      // Nullify the socket reference
            }
            break;
    }

    promises.clear(); // Clear all promises
};
