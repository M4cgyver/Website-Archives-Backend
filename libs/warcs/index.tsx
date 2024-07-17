import { MultiProgressBars } from 'multi-progress-bars';
import fs from 'fs/promises';
import { splitArrayIntoFour } from '../array';
import { open } from 'fs/promises';
import { mWarcParse, mWarcParseResponses, type mWarcReadFunction } from '../mwarcparser';
import { dbInsertResponse } from '../database';

const read = async (filename: string): Promise<mWarcReadFunction> => {
    const fd = await open(filename, 'r');
    const fileStats = await fd.stat();
    const fileSize = fileStats.size;

    return async (offset: bigint, size: bigint): Promise<Buffer> => {
        if (offset >= fileSize || offset + size > fileSize) {
            throw new RangeError('Out of bounds read attempt');
        }

        const buffer = Buffer.alloc(Number(size));
        const { bytesRead } = await fd.read(buffer, 0, Number(size), Number(offset));
        //console.log("read", offset, size);
        return buffer;
    };
}

export const parseWarcFiles = async () => {
    console.log("Parsing warc files...");

    /*
    const mpd = new MultiProgressBars({
        initMessage: "$ Parsing files...",
        anchor: 'top',
        persist: true,
        border: true,
    })
    */

    try {
        const files = (await fs.readdir("warcs/")).filter(file => file.endsWith('.warc'));
        const filesspit = splitArrayIntoFour(files);

        await Promise.all(filesspit.map(async files => {
            for (const file of files) { // Corrected the loop syntax
                const warc = mWarcParseResponses(await read(`warcs/${file}`), { skipContent: true });

                console.log(`   Parsing ${file}...`);

                //mpd.addTask(file, { type: 'percentage' });

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

                    //console.log(lastModified, date);
                    //if(responseContentLength == null)
                    //console.log(targetUri, responseContentLength);;

                    //console.log(targetUri);

                    dbInsertResponse(
                        targetUri.replace(/<|>/g, ''),                                          //uri: string, 
                        location,                                           //location: string, 
                        responseType,                                           //type: string, 
                        `warcs/${file}`,                                               //filename: string, 
                        recordWarcOffset,                                   //offsetHeader: bigint, 
                        recordContentOffset,                                //offsetContent: bigint, 
                        responseContentLength,                              //contentLength: bigint, 
                        (lastModified) ? new Date(lastModified) : null,     //lastModified: string, 
                        (date) ? new Date(date) : null,                     //date: string, 
                        status,                                                //status: number
                        transferEncoding,
                    ).then(()=>{
                        const percent = Math.round((Number(recordWarcOffset) / fileSize)*100);

                        if(percent > lastPercent) {
                            //mpd.updateTask(file, {percentage: percent/100})
                            lastPercent = percent;
                            console.log(file, percent)
                        }
                    });
                }
                console.log(`   Parsed ${file}!`);
            }
        }));
    } catch (error) {
        console.error('Error occurred while parsing WARC files:', error);
    }
}
