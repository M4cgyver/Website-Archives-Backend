import express, { Request, Response } from "express";
import { open } from 'fs/promises';
import { Transform } from 'stream';
import { mWarcParseResponseContent } from "../../mwarcparser";
import { httpRedirect } from "../../http";
import { dbRetrieveResponse } from "../../database";

export const view = async (req: Request, res: Response) => {
    const uri = req.query.uri as string | undefined;
    const redirect = req.query.redirect === 'true';
    const timestr = `Viewing ${uri} ${process.hrtime()}...`;
    console.time(timestr);

    if (!uri) {
        console.timeEnd(timestr);
        return res.status(400).json({ error: "Missing query parameter: uri" });
    }

    try {
        const [record] = await dbRetrieveResponse(uri);

        if (!record) {
            console.timeEnd(timestr);
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

        const { location, 'transfer-encoding': transferEncoding } = meta || {};

        // Handle redirection
        //console.log(status, location, meta);
        if ([301, 302, 303, 307, 308].includes(status) && location) {
            req.query.uri = location;
            console.timeEnd(timestr);
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

        if (redirect && contentType.includes("text/")) {
            let str = "";
            finalStream.on('data', chunk => {
                str += chunk;
            });

            finalStream.on('end', async () => {
                try {
                    const redirectUrl = process.env.REDIRECT_BASE_URL ?? "https://redirect-url-not-defined";
                    const parsed = await httpRedirect(str, uri, redirectUrl, contentType);
                    res.send(parsed);
                } catch (err) {
                    console.error('Redirect error:', err);
                    res.status(500).send('Internal Server Error');
                } finally {
                    console.timeEnd(timestr);
                    await fd.close();
                }
            });

        } else {
            finalStream.pipe(res);

            finalStream.on('end', async () => {
                await fd.close();
                console.timeEnd(timestr);
            });
        }

        finalStream.on('error', err => {
            console.error('Stream error:', err);
            res.status(500).send('Internal Server Error');
            console.timeEnd(timestr);
        });

    } catch (error) {
        console.error("Error handling request:", error);
        res.status(500).json({ error: "Internal server error" });
        console.timeEnd(timestr);
    }
};
