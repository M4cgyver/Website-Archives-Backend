import type { BunRequest } from "bunrest/src/server/request";
import { dbSearchResponses } from "../database";
import type { BunResponse } from "bunrest/src/server/response";

export const search = async (req: BunRequest, res: BunResponse) => {
    // Extract and validate query parameters
    const uri = req.query.uri as string | undefined;
    const total = parseInt(req.query.total as string, 10) || 32; // Default to 32 if not provided
    const page = parseInt(req.query.page as string, 10) || 1; // Default to page 1
    const statusParam = req.query.status as string | undefined;
    const type = req.query.type as string | undefined;

    // Validate and process status parameter
    let status: number | undefined;
    if (statusParam) {
        status = parseInt(statusParam, 10);
        if (isNaN(status)) {
            return res.status(400).json({ error: "Invalid query parameter: status must be a number" });
        }
    }

    // Calculate the offset for pagination
    const offset = (page - 1) * total;

    try {
        // Fetch responses from the database
        const responses = await dbSearchResponses({
            search_uri_a: uri ?? '', // Default to empty string if uri is undefined
            limit_num_a: total,
            offset_num_a: offset,
            search_ip_a: status ? status.toString() : undefined,
            search_content_type_a: type,
        });

        // Return the fetched responses as JSON
        res.json(responses);
    } catch (error:any) {
        // Log and return detailed error information
        console.error('Error retrieving responses:', error.message, error)

        res.status(500).json({
            error: "Failed to retrieve responses",
            details: (error as Error).message,
        });
    }
};
