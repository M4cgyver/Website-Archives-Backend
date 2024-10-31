import type { BunRequest } from "bunrest/src/server/request";
import type { BunResponse } from "bunrest/src/server/response";
import { dbRetrieveLatestResponses } from "../../database"; // Adjust the import based on your actual file structure

export const latest = (req: BunRequest, res: BunResponse) => {
    const total = parseInt(req.query.total as string) || 20; // Default to 20 if the total query parameter is not provided

    return dbRetrieveLatestResponses(total)
        .then(latestResponses => {
            res.json(latestResponses);
        })
        .catch(error => {
            console.error("Error retrieving latest responses:", error);
            res.status(500).json({ error: "Internal server error" });
        });
};