import { MultiProgressBars } from 'multi-progress-bars';
import fs from 'fs/promises';
import { splitArrayIntoFour } from '../array';
import { open } from 'fs/promises';
import { mWarcParse, mWarcParseResponses, type mWarcReadFunction } from '../mwarcparser';
import { dbInsertResponse } from '../database';

const progressType: string = process.env.WARC_PROCESSING_STATUS ?? "bar";

export const parseWarcFiles = async () => {
    /*
    console.log("Parsing warc files...");

    const mpd = new MultiProgressBars({
        initMessage: "$ Parsing files...",
        anchor: 'top',
        persist: true,
        border: true,
    })

    try {
        const files = (await fs.readdir("warcs/")).filter(file => file.endsWith('.warc'));
        const filesspit = splitArrayIntoFour(files);

        await Promise.all(filesspit.map(async files => {
            for (const file of files) { // Corrected the loop syntax
                const warc = mWarcParseResponses(await read(`warcs/${file}`), { skipContent: true });

                console.log(`   Parsing ${file}...`);

                mpd.addTask(file, { type: 'percentage' });

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

                    //console.log(responseType);

                    await dbInsertResponse(
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
                            mpd.updateTask(file, {percentage: percent/100})
                            lastPercent = percent;
                        }
                        //console.log(file, Number(responseContentLength), fileSize)
                    });
                }
                console.log(`   Parsed ${file}!`);
            }
        }));
    } catch (error) {
        console.error('Error occurred while parsing WARC files:', error);
    }
    */

    const workers: Worker[] = [];
    const files = (await fs.readdir("warcs/")).filter(file => file.endsWith('.warc')).sort();

    const mpd = new MultiProgressBars({
        initMessage: "$ Parsing files...",
        anchor: 'top',
        persist: true,
        border: true,
    });

    /*
    files.forEach(file => {
        mpd.addTask(file, { type: 'percentage' });

        const worker = new Worker("libs/warcs/worker.ts");

        worker.onmessage = event => {
            const { file, status, progress } = event.data;
            
            if(status == "progress")
                mpd.updateTask(file, {percentage: progress/100})
            else if (status == "complete") 
                mpd.updateTask(file, {percentage: 1})    
        }

        worker.postMessage({ file: file });
        workers.push(worker);
    }) */

    const startWorker = async (file: string) => {
        if (mpd)
            mpd.addTask(file, { type: 'percentage' });

        const worker = new Worker("libs/warcs/worker.ts");

        worker.onmessage = event => {
            const { file, status, progress } = event.data;

            if (status == "progress")
                if (mpd)
                    mpd.updateTask(file, { percentage: progress / 100 })
                else 
                    console.log(`${file} processed ${progress}`)
            else if (status == "complete") {
                if (mpd)
                    mpd.updateTask(file, { percentage: 1 })
                else 
                    console.log(`${file} complete!`);

                const nextFile = files.pop();

                if (nextFile)
                    startWorker(nextFile);
            }
        }

        worker.postMessage({ file: file });
        workers.push(worker);

        return worker;
    }

    for (let file = files.pop(); file && workers.length < (parseInt(process.env.MAX_PARRALLEL_WARC_PROCESSING ?? '4') ?? 4); file = files.pop())
        await startWorker(file);
}
