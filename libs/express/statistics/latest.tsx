import express, { Request, Response } from "express";
import { dbRetrieveLatestResponses } from "../../database";

export const latest = async (req: Request, res: Response) => {
    const total = parseInt(req.query.total as string) || 20; // Default to 20 if the total query parameter is not provided

    try {
        const latestResponses = await dbRetrieveLatestResponses(total);
        res.json(latestResponses);
    } catch (error) {
        console.error("Error retrieving latest responses:", error);
        res.status(500).json({ error: "Internal server error" });
    }
};