import express, { Request, Response } from "express";
import fs from "fs";
import os from "os";
import path from "path";

export const statistics = async (req: Request, res: Response) => {
    const warcsDirectory = "warcs/"; // Replace with your actual warcs directory path

    try {
        // Function to calculate total size of a directory recursively
        const getTotalUsedSpace = (dirPath: string): number => {
            let totalSize = 0;

            const walkSync = (currentPath: string) => {
                const files = fs.readdirSync(currentPath);

                files.forEach((file) => {
                    const filePath = path.join(currentPath, file);
                    const stats = fs.statSync(filePath);

                    if (stats.isDirectory()) {
                        walkSync(filePath); // Recursively walk through directories
                    } else {
                        totalSize += stats.size; // Add file size to total
                    }
                });
            };

            walkSync(dirPath);
            return totalSize;
        };

        // Get total used space asynchronously
        const usedPromise = new Promise<number>((resolve, reject) => {
            fs.promises.stat(warcsDirectory)
                .then(stats => {
                    if (!stats.isDirectory()) {
                        reject(new Error(`${warcsDirectory} is not a directory`));
                    } else {
                        resolve(getTotalUsedSpace(warcsDirectory));
                    }
                })
                .catch(reject);
        });

        // Get free space asynchronously
        const freePromise = new Promise<number>((resolve, reject) => {
            const free = os.freemem();
            resolve(free);
        });

        // Await results and construct response
        const [used, free] = await Promise.all([usedPromise, freePromise]);

        const stats = {
            used: used,
            free: free
        };

        res.json(stats);
    } catch (error) {
        console.error("Error calculating statistics:", error);
        res.status(500).json({ error: "Internal server error" });
    }
};
