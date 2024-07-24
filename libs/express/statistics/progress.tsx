import express, { Request, Response } from "express";
import { dbRetrieveLatestResponses } from "../../database";
import { parseWarcFilesProgress } from "../../warcs";

export const progress = async (req: Request, res: Response) => {
    const total = parseInt(req.query.total as string) || 20; // Default to 20 if the total query parameter is not provided

    try {
        res.json(parseWarcFilesProgress);
    } catch (error) {
        console.error("Error retrieving parsing progress:", error);
        res.status(500).json({ error: "Internal server error" });
    }
};