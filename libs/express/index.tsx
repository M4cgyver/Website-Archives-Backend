import { connect, spawn, type Subprocess } from "bun";
import server from "bunrest";
import type { BunRequest } from "bunrest/src/server/request";
import type { BunResponse } from "bunrest/src/server/response";
import express from "express";
import { connectDb } from "../database";
import { search } from "./search";
import { view } from "./view";
import { statistics } from "./statistics";
import { latest } from "./statistics/latest";
import { progress } from "./statistics/progress";

export const workers: Array<Worker | Subprocess<any>> = [];
let app: any = null;
export const id = process.pid;

export const setupExpress = async () => {
    console.debug(`WS-EXPRESS ${id}:    Setting up Express server...`);

    app = server();
    app.use(express.json());

    app.use((req:BunRequest, res:BunResponse, next:any, err:any) => {
        res.status(500).send('Error happened');
    });

    await connectDb(undefined, {connType: "net", host: "0.0.0.0", port:9824})

    console.debug(`WS-EXPRESS ${id}:    Express server setup complete.`);
}

export const listenExpress = async () => {
    if (!app) {
        console.debug(`WS-EXPRESS ${id}:App not initialized. Calling setupExpress...`);
        await setupExpress();
    } else {
        console.debug(`WS-EXPRESS ${id}:App already initialized. Skipping setupExpress.`);
    }

    app.listen(3000, undefined, {
        reusePort: true
    });

    app.get('/test', (req: any, res: any) => {
        res.status(200).json({ message: `Hello World! ${id}` });
    });

    app.get("/api/search", search)
    app.get("/api/view", view)
    app.get("/api/statistics", statistics)
    app.get("/api/statistics/latest", latest)
    app.get("/api/statistics/progress", progress)

    console.debug(`WS-EXPRESS ${id}:Express server is listening on port 3000.`);
}

export const setupExpressWithWorkers = () => {
    console.debug("Setting up workers...");
    for (let i = 0; i < navigator.hardwareConcurrency; i++) {
        console.debug(`Spawning worker ${i + 1}...`);
        workers.push(spawn({
            cmd: ["bun", "./libs/express/index.tsx", "listen"],
            stdout: "inherit",
            stderr: "inherit",
            stdin: "inherit",
        }));
        console.debug(`Worker ${i + 1} spawned.`);
    }
    console.debug("All workers have been set up.");
}

// Check command args
const args = process.argv.slice(2); // Get command-line arguments
if (args.includes("listen")) {
    console.debug(`WS-EXPRESS ${id}:Detected 'listen' argument. Starting the Express server...`);
    listenExpress();
} else {
    console.debug(`WS-EXPRESS ${id}:Listening not initiated. No 'listen' argument found.`);
}
