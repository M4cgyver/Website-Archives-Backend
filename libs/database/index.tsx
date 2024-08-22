import { getWorker, type dbInsertResponseParams, type dbRetrieveResponseFullResult, type dbRetrieveResponseResult, type dbSearchResponseResult, type dbSearchResponsesParams } from "./types";

const { worker, promises } = getWorker();

const callAction = (action: string, params?: any): Promise<any> => {
    return new Promise((resolve, reject) => {
        const id = process.hrtime()[1]; // Unique ID for each request
        promises.set(id, { resolve, reject });

        worker.postMessage({ id, action, params });
    });
};

export const connectDb = async (): Promise<any> => callAction('connectDb');
export const setupDb = async (): Promise<void> => callAction('setupDb');
export const closeDb = async (): Promise<void> => callAction('closeDb');
export const dbInsertResponse = async (params: dbInsertResponseParams): Promise<void> => callAction('dbInsertResponse', params);
export const dbSearchResponses = async (params: dbSearchResponsesParams): Promise<any[]> => callAction('dbSearchResponses', params);
export const dbRetrieveResponse = async (uri_string: string): Promise<dbRetrieveResponseResult[]> => callAction('dbRetrieveResponse', uri_string);
export const dbRetrieveResponseFull = async (uri_string: string): Promise<dbRetrieveResponseFullResult[]> => callAction('dbRetrieveResponseFull', uri_string);
export const dbRetrieveLatestResponses = async (total: number): Promise<dbSearchResponseResult[]> => callAction('dbRetrieveLatestResponses', total);
