import express, { Request, Response } from "express";
import { dbRetrieveResponses } from "../database";
import { open } from 'fs/promises';
import { Transform } from 'stream';
import { hexToBn } from "../bignumber";
import { mWarcParseResponseContent } from "../mwarcparser";
import { httpRedirect } from "../http";

export const view = async (req: Request, res: Response) => {
    const uri: string | undefined = req.query.uri as string | undefined;
    const dateArchived: string | undefined = req.query.dateArchived as string | undefined;
    const lastModified: string | undefined = req.query.lastModified as string | undefined;
    const redirect: boolean | undefined = req.query.redirect === 'true';

    /*
    // Check if the URI parameter is provided
    if (!uri) {
        return res.status(400).json({ error: "Missing query parameter: uri" });
    }
    */

    try {
        const data: any = await dbRetrieveResponses(uri, undefined, 1n, 1n);

        if (data.length === 0) {
            return res.status(404).json({ error: "No data found for the given URI" });
        }

        const status: number = parseInt(data[0].status);
        const location: string | null = data[0].location;
        const filename: string = data[0].filename;
        const targetUri: string = data[0].uri;
        const contentLength: string = data[0].content_length;
        const offsetContent: string = data[0].offset_content;
        const transferEncoding: string = data[0].transfer_encoding;
        const contentType: string = data[0].type;

        if ((status == 301 || status == 302 || status == 303 || status == 307 || status == 308) && location !== null && location !== undefined && location !== "") {
            req.query.uri = location;
            view(req, res); // Ensure view function is correctly defined and imported
            return;
        }        

        const fd = await open(filename, 'r');
        const streamOptions = {
            start: Number(offsetContent),
            end: Number(offsetContent) + Number(contentLength) - 1,
        };
        const readStream = fd.createReadStream(streamOptions);

        res.set('Content-Type', contentType);

        if (contentLength)
            res.set('Content-Length', contentLength);

        res.status(status);

        if (redirect && contentType.includes("text/")) {
            const finalStream = await mWarcParseResponseContent(readStream, transferEncoding)

            const promise = new Promise((res, rej) => {
                let str = "";

                finalStream.on('data', chunk => {
                    str += chunk;
                });

                finalStream.on('end', () => {
                    res(str);
                });

                finalStream.on('error', err => {
                    rej(err);
                });
            });

            promise.then((str: any) => {
                return httpRedirect(str, targetUri, "http://localhost:4002/api/view?redirect=true", contentType);
            }).then(parsed => {
                res.send(parsed);
            });

        } else {
            const finalStream = await mWarcParseResponseContent(readStream, transferEncoding).pipe(res);

            finalStream.on('end', () => {
                fd.close();
            });

            finalStream.on('error', (err: any) => {
                console.error('Stream error:', err);
                res.status(500).send('Internal Server Error');
            });
        }

    } catch (error: any) {
        console.error('Failed to retrieve responses', error);
        res.status(500).json({ error: "Failed to retrieve responses", message: error.message });
    }
};
