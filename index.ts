import { resolve } from "bun";
import { hexToBn } from "./libs/bignumber";
import { connectDb, setupDb } from "./libs/database";
import { listenExpress, setupExpress } from "./libs/express";
import { parseWarcFiles } from "./libs/warcs";

console.log("starting api");

await connectDb();
await setupDb();

parseWarcFiles();

await listenExpress();