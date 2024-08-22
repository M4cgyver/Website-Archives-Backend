import { MultiProgressBars } from 'multi-progress-bars';
import fs from 'fs/promises';
import { open } from 'fs/promises';
import { mWarcParseResponses, type mWarcReadFunction } from '../mwarcparser';
import { dbInsertResponse } from '../database';

declare var self: Worker;

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
            throw new Error('Failed to read the expected number of bytes');
        }
        return buffer;
    };
};

const parseWarcFile = async (file: string) => {
    const warc = mWarcParseResponses(await readFile(`warcs/${file}`), { skipContent: true });

    console.log(`   Parsing ${file}...`);

    const fileSize = (await fs.stat(`warcs/${file}`)).size;
    let lastPercent = 0;

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

        const {
            date,
            location,
            "content-type": responseType,
            "content-length": responseContentLength,
            "last-modified": lastModified,
            "transfer-encoding": transferEncoding,
            status
        } = http;

        const { recordWarcOffset, recordResponseOffset, recordContentOffset } = metadata;

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
            meta: convertBigIntToNumber(http)
        };

        try {
            await dbInsertResponse(recordData);

            const percent = Math.round((Number(recordWarcOffset) / fileSize) * 100);

            if (percent > lastPercent) {
                lastPercent = percent;
                postMessage({ file: file, status: "progress", progress: percent });
            }
        } catch (e: any) {
            console.log(`Failed to insert record ${e.message}`, recordData);
        }
    }

    console.log(`   Parsed ${file}!`);
    postMessage({ file: file, status: "complete" });
};

self.onmessage = (event: MessageEvent) => {
    const data = event.data;

    if (typeof data !== 'object' || !('file' in data)) {
        console.log(`WARC Worker, invalid format ${data}`);
        return;
    }

    console.log(`WARC Worker: starting to parse file: ${data.file}`);
    parseWarcFile(data.file);
};
