import express, { Request, Response } from "express";
import { dbRetrieveResponses } from "../database";

export const search = (req: Request, res: Response) => {
    const uri: string | undefined = req.query.uri as string | undefined;
    const total: string | undefined = req.query.total as string | undefined;
    const page: string | undefined = req.query.page as string | undefined;
    const statusParam: string | undefined = req.query.status as string | undefined;
    const type: string | null = req.query.type as string | null;

    // Validate 'status' query parameter
    let status: number | undefined;
    if (statusParam !== undefined) {
        status = parseInt(statusParam, 10);
        if (isNaN(status)) {
            console.error('Invalid query parameter: status must be a number');
            return res.status(400).json({ error: "Invalid query parameter: status must be a number" });
        }
    }

    // Validate 'total' and 'page' query parameters
    let totalEntries: bigint;
    let pageNumber: bigint;

    try {
        totalEntries = BigInt(total ?? "32");
    } catch (e) {
        console.error('Invalid query parameter: total must be a BigInt');
        return res.status(400).json({ error: "Invalid query parameter: total must be a BigInt" });
    }

    try {
        pageNumber = BigInt(page ?? "1");
    } catch (e) {
        console.error('Invalid query parameter: page must be a BigInt');
        return res.status(400).json({ error: "Invalid query parameter: page must be a BigInt" });
    }

    // Call the database function to retrieve responses
    dbRetrieveResponses(uri, undefined, totalEntries, pageNumber, status, type)
        .then((data: any) => {
            console.log(data);
            res.json(data); // Return the retrieved responses as JSON
        })
        .catch((error: any) => {
            // Log the error details
            console.error('Failed to retrieve responses:', {
                message: error.message,
                stack: error.stack,
                name: error.name
            });

            // Return detailed error message in the response
            res.status(500).json({
                error: "Failed to retrieve responses",
                details: error.message
            });
        });
};
