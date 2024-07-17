import { hexToBn } from "./libs/bignumber";
import { connectDb, setupDb } from "./libs/database";
import { listenExpress, setupExpress } from "./libs/express";
import { parseWarcFiles } from "./libs/warcs";

await connectDb();
await setupDb();

parseWarcFiles();

await listenExpress();