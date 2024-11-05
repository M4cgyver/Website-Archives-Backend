import type { BunRequest } from "bunrest/src/server/request";
import type { BunResponse } from "bunrest/src/server/response";
import { dbRetrieveFileProgress, dbRetrieveLatestResponses } from "../../database";

export const progress = async (req: BunRequest, res: BunResponse) => {

    /*
    try {
        res.json(parseWarcFilesProgress);
    } catch (error) {
        console.error("Error retrieving parsing progress:", error);
        res.status(500).json({ error: "Internal server error" });
    }
    */
    console.log(`WS-EXPRESS ${process.pid}: Getting stats...`)
    const dbData = await dbRetrieveFileProgress()
    console.log(`STATS`, dbData)
    res.status(200).json(dbData);

};