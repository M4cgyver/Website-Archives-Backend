import { open } from 'fs/promises';
import { Transform } from 'stream';
import { mWarcParseResponseContent } from "../mwarcparser";
import { httpRedirect } from "../http";
import { dbRetrieveResponse } from "../database";
import type { BunRequest } from 'bunrest/src/server/request';
import type { BunResponse } from 'bunrest/src/server/response';
import { id } from '.';

export const view = async (req: BunRequest, res: BunResponse) => {
    const uri = req.query.uri as string | undefined;
    const redirect = req.query.redirect === 'true';

    console.log(`WS-EXPRESS ${id}: Requesting to view: ${uri}`);

    if (!uri) {
        console.error(`WS-EXPRESS ${id}: "Missing query parameter: uri - ${uri}`);
        return res.status(400).json({ error: "Missing query parameter: uri", data: {path: req.path, uri: req.query}});
    }

    try {
        const datares = await dbRetrieveResponse(uri);

        console.log(`WS-EXPRESS ${id}: Found record ~ `, uri, datares);

        if (!datares) {
            //No data from db?????????????/
            console.log(`WS-EXPRESS ${id}: no data from DB???? retrying`);
            return view(req, res);
        }

        const [record] = datares;

        if (!record) {
            return res.status(404).json({ error: `No record found with the given uri ${uri}` });
        }

        const {
            file_r: filepath,
            content_type_r: contentType,
            content_offset_r: contentOffset,
            content_length_r: contentLength,
            status_r: status,
            meta_r: meta,
        } = record;

        console.log(`WS-EXPRESS ${id}: found record: ${/*record*/""}`);

        const { location, 'transfer-encoding': transferEncoding } = meta || {};

        // Handle redirection
        //console.log(status, location, meta);
        if ([301, 302, 303, 307, 308].includes(status) && location) {
            req.query.uri = location;
            return view(req, res);
        }

        // Open the file and create a stream
        const fd = await open(filepath, 'r');
        const streamOptions = {
            start: Number(contentOffset),
            end: Number(contentOffset) + Number(contentLength) - 1,
        };
        const readStream = fd.createReadStream(streamOptions);

        res.setHeader('Content-Type', contentType);
        if (contentLength) {
            res.setHeader('Content-Length', contentLength);
        }

        res.status(status);

        const finalStream = await mWarcParseResponseContent(readStream, transferEncoding);

        console.log(`WS-EXPRESS ${id}: loaded content, prepairing to send...`);

        if (redirect && contentType.includes("text/")) {
            return new Promise((suc, er) => {
                console.log(`WS-EXPRESS ${id}: loaded content, prepairing to send... need to parse though first...`);

                let str = "";
                finalStream.on('data', chunk => {
                    str += chunk;
                });
    
                finalStream.on('end', async () => {
                    try {
                        console.log(`WS-EXPRESS ${id}: chunk size ${str.length}`);
                        const redirectUrl = process.env.REDIRECT_BASE_URL ?? "https://redirect-url-not-defined";
                        const parsed = await httpRedirect(str, uri, redirectUrl, contentType);
                        console.log(`WS-EXPRESS ${id}: parsed chunk size ${parsed.length}`);
                        res.status(200).send(parsed);
                        suc(null);
                    } catch (err:any) {
                        console.error('Redirect error:', err);
                        res.status(500).send({error: 'Internal Server Error, failed to parse', message: err.message});
                        er(err)
                    } finally {
                        await fd.close();
                    }
                });
    
            })
            
        } else {
            res.status(200).send(finalStream)

            finalStream.on('end', async () => {
                await fd.close();
            });
        }

        finalStream.on('error', err => {
            console.error('Stream error:', err);
            res.status(500).send('Internal Server Error');
        });

    } catch (error) {
        console.error("Error handling request:", error);
        res.status(500).json({ error: "Internal server error" });
    }
};
