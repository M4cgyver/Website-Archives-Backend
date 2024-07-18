import express, { Request, Response } from "express";
import { view } from "./view";
import { search } from "./search";
import { statistics } from "./statistics/index";
import { latest } from "./statistics/latest";

let app: express.Application | undefined = undefined;

export const setupExpress = () => {
    if (app !== undefined) return;

    app = express();

    // Define your route
    app.get("/api/search", search);
    app.get("/api/view", view);

    app.get("/api/statistics", statistics);
    app.get("/api/statistics/latest", latest);

};

export const listenExpress = () => {
    if(app == null) setupExpress();

    // Start the Express server
    app.listen(3000, () => {
        console.log("Server is running on port 3000");
    });
}
