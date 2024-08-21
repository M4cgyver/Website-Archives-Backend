import { dbInsertResponseParams, dbSearchResponsesParams, dbSearchResponseResult, dbRetrieveResponseResult, dbRetrieveResponseFullResult } from "./types";

const worker = new Worker(new URL("./worker.tsx", import.meta.url), { type: 'module' });

type ActionResponse = {
    id: number;
    status: 'success' | 'error';
    data?: any;
    message?: string;
};

// Map to keep track of promises by their IDs
const promises = new Map<number, { resolve: (value: any) => void, reject: (reason?: any) => void }>();

worker.onmessage = (event: MessageEvent<ActionResponse>) => {
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

const callAction = (action: string, params?: any): Promise<any> => {
    return new Promise((resolve, reject) => {
        //const id = Date.now(); // Unique ID for each request
        const id = process.hrtime()[1];
        promises.set(id, { resolve, reject });

        worker.postMessage({ id, action, params });
    });
};

export const connectDb = async (): Promise<any> => callAction('connectDb');
export const setupDb = async (): Promise<void> => callAction('setupDb');
export const dbInsertResponse = async (params: dbInsertResponseParams): Promise<void> => callAction('dbInsertResponse', params);
export const dbSearchResponses = async (params: dbSearchResponsesParams): Promise<any[]> => callAction('dbSearchResponses', params);
export const dbRetrieveResponse = async (uri_string: string): Promise<dbRetrieveResponseResult[]> => callAction('dbRetrieveResponse', uri_string);
export const dbRetrieveResponseFull = async (uri_string: string): Promise<dbRetrieveResponseFullResult[]> => callAction('dbRetrieveResponseFull', uri_string);
export const dbRetrieveLatestResponses = async (total: number): Promise<dbSearchResponseResult[]> => callAction('dbRetrieveLatestResponses', total);
