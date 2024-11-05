import { createReadStream } from 'fs';
import fs from 'fs/promises';
import { open } from 'fs/promises';
import { mWarcParseEtag, mWarcParseResponseContent, mWarcParseResponses, type mWarcReadFunction } from '../mwarcparser';
import { closeDb, connectDb, dbInsertResponse } from '../database';
import { genid } from '../genid';

declare var self: Worker;

const promises: Promise<any>[] = [];
const promiseRets = new Map<number, { resolve: (value: any) => void, reject: (reason?: any) => void }>();

// Convert BigInt to Number, with checks
const convertBigIntToNumber = (obj: any): any => {
    if (typeof obj === 'bigint') {
        if (obj > Number.MAX_SAFE_INTEGER || obj < Number.MIN_SAFE_INTEGER) {
            throw new Error('BigInt value out of safe Number range');
        }
        return Number(obj);
    } else if (typeof obj === 'object' && obj !== null) {
        for (const key in obj) {
            if (obj.hasOwnProperty(key)) {
                obj[key] = convertBigIntToNumber(obj[key]);
            }
        }
    }
    return obj;
};

const readFile = async (filename: string): Promise<mWarcReadFunction> => {

    const fd = await open(filename, 'r');
    const fileStats = await fd.stat();
    const fileSize = fileStats.size;

    return async (offset: bigint, size: bigint): Promise<Buffer> => {
        if (offset >= fileSize || offset + size > fileSize) {
            throw new RangeError('Out of bounds read attempt');
        }

        const buffer = Buffer.alloc(Number(size));
        const { bytesRead } = await fd.read(buffer, 0, Number(size), Number(offset));
        if (bytesRead !== Number(size)) {
            throw new RangeError('Failed to read the expected number of bytes');
        }
        return buffer;
    };
};



const parseWarcFile = async (file: string) => {

    const fileDir = `warcs/${file}`;
    const fd = await open(fileDir, 'r');
    const warc = mWarcParseResponses(await readFile(fileDir), { skipContent: true });
    console.log(`   Parsing ${file}...`);

    const fileSize = (await fs.stat(fileDir)).size;
    let lastPercent = 0;
    const promises: Promise<void>[] = []; // Array to hold all db insert promises

    try {
        for await (const [header, http, content, metadata] of warc) {
            const {
                "warc-type": warcType,
                "warc-record-id": recordId,
                "warc-warcinfo-id": warcinfoId,
                "warc-concurrent-to": concurrentTo,
                "warc-target-uri": targetUri,
                "warc-date": warcDate,
                "warc-ip-address": ipAddress,
                "warc-block-digest": blockDigest,
                "warc-payload-digest": payloadDigest,
                "content-type": contentType,
                "content-length": contentLength
            } = header;

            const { recordWarcOffset, recordResponseOffset, recordContentOffset } = metadata;

            const {
                date,
                location,
                "content-type": responseType,
                "content-length": responseContentLength,
                "last-modified": lastModified,
                "transfer-encoding": transferEncoding,
                status
            } = http;

            const etag = /* (http["etag"] as string | undefined) ?? */ await mWarcParseEtag(mWarcParseResponseContent(fd.createReadStream({
                start: Number(recordContentOffset),
                end: Number(recordContentOffset) + Number(responseContentLength) - 1,
            }), transferEncoding)).catch((e:any)=>{
                console.log(`Failed to parse etag for record ${e.message}`, targetUri);
            });

            //console.log(etag);

            if(!etag) continue;

            const recordData = {
                uri_string: targetUri.replace(/<|>/g, ''),
                file_string: `warcs/${file}`,
                content_type_string: responseType ?? "application/unknown",
                resource_type_string: 'response',
                record_length: BigInt(recordResponseOffset),
                record_offset: BigInt(recordWarcOffset),
                content_length: BigInt(responseContentLength),
                content_offset: BigInt(recordContentOffset),
                status: status,
                meta: convertBigIntToNumber(http),
            };

            promises.push(dbInsertResponse(recordData).then(async () => {
                const percent = Math.round((Number(recordWarcOffset) / fileSize) * 100);
                if (percent > lastPercent) {
                    lastPercent = percent;
                    postMessage({ file: file, status: "progress", progress: percent });
                }
            }).catch((e: any) => {
                console.log(`Failed to insert record ${e.message}`, recordData);
            }));
        }
    } catch (error) {
        if (error instanceof RangeError) {
            console.log("RangeError encountered, exiting parsing loop.");
        } else {
            console.error("An unexpected error occurred:", error);
            throw error; // Re-throw non-RangeErrors for further handling if needed
        }
    } finally {
        // Wait for all insertions to finish
        await Promise.allSettled(promises).then(() => {
            console.log(`   Parsed ${file}!`);
            closeDb().then(() => {
                postMessage({ file: file, status: "complete" });
            });
        });
    }
};

self.onmessage = async (event: MessageEvent) => {
    console.log("entry");

    const data = event.data;
    const { file, channel } = data;

    if (typeof data !== 'object' || !file) {
        console.log(`WARC Worker, invalid format ${data}`);
        return;
    }

    await connectDb(undefined, { connType: "net", host: "127.0.0.1", port: 9824 });

    console.log(`WARC Worker: starting to parse file: ${file} ${channel}`);

    parseWarcFile(data.file);
};
