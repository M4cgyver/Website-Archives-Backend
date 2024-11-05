import { connect, resolve } from "bun";
import { hexToBn } from "./libs/bignumber";
import { connectDb, setupDb } from "./libs/database";
import { listenExpress, setupExpress, setupExpressWithWorkers } from "./libs/express";
import { parseWarcFiles } from "./libs/warcs";
import { getWorker } from "./libs/database/types";

console.log("starting api");

if (!process.argv.includes('--listen')) {
    await connectDb();
    await setupDb();

    parseWarcFiles();

    await setupExpressWithWorkers();
}
