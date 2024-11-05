// @bun
// libs/warcs/worker.ts
import fs2 from "fs/promises";
import { open } from "fs/promises";

// libs/mwarcparser/index.tsx
import { Transform } from "stream";

// libs/bignumber/index.tsx
function hexToBn(hex, options) {
  const unsigned = options?.unsigned ?? false;
  if (hex.length % 2 || hex.length === 0) {
    hex = "0" + hex;
  }
  const bn = BigInt("0x" + hex);
  if (unsigned) {
    return bn;
  }
  const highByte = parseInt(hex.slice(0, 2), 16);
  if (highByte >= 128) {
    const flipped = bn ^ (1n << BigInt(hex.length * 4)) - 1n;
    return -flipped - 1n;
  }
  return bn;
}

// libs/mwarcparser/index.tsx
import fs from "fs";
import { Readable as NodeReadable } from "stream";
var mWarcParseHeader = (s) => {
  const lines = s.split("\r\n");
  const firstLine = lines[0];
  const httpRegex = /^HTTP\/(\d+\.\d+)\s+(\d+)\s+(.*)$/;
  const httpMatches = firstLine.match(httpRegex);
  const httpHeader = httpMatches ? {
    http: httpMatches[1],
    status: parseInt(httpMatches[2], 10),
    message: httpMatches[3].trim()
  } : {};
  const headers = lines.slice(1).filter((line) => line.trim() !== "").reduce((acc, line) => {
    const index = line.indexOf(":");
    if (index !== -1) {
      const key = line.slice(0, index).trim().toLowerCase();
      const value = line.slice(index + 1).trim();
      return { ...acc, [key]: value };
    }
    return acc;
  }, {});
  return { ...headers, ...httpHeader };
};
var mWarcParseResponses = (read, options) => {
  const skipContent = options?.skipContent || false;
  const readBufferSize = options?.readBufferSize || 512n + 256n;
  let backbuffer = "";
  let offset = 0n;
  let done = false;
  return {
    [Symbol.asyncIterator]() {
      return {
        async next() {
          if (done) {
            return { done: true, value: undefined };
          }
          let content = undefined;
          let header;
          let http;
          let metadata = {
            recordWarcOffset: 0n,
            recordResponseOffset: 0n,
            recordContentOffset: 0n
          };
          try {
            do {
              let bufferToParse = backbuffer;
              let bufferChunks = [];
              metadata.recordWarcOffset = offset - BigInt(bufferToParse.length);
              while (bufferChunks.length < 2) {
                const newChunks = bufferToParse.split("\r\n\r\n");
                bufferChunks = newChunks.length > 1 ? newChunks : bufferChunks;
                if (bufferChunks.length < 2) {
                  const possiblePromise = await read(offset, readBufferSize);
                  bufferToParse += possiblePromise.toString();
                  offset += readBufferSize;
                }
              }
              header = mWarcParseHeader(bufferChunks[0]);
              const contentLength = header["content-length"] ? BigInt(header["content-length"]) : 0n;
              if (header["warc-type"] !== "response" || contentLength === 0n) {
                const remainingBytes = contentLength - BigInt(bufferToParse.length - bufferChunks[0].length - 4);
                offset += remainingBytes > 0n ? remainingBytes : 0n;
                backbuffer = remainingBytes < 0n ? bufferToParse.slice(bufferToParse.length + Number(remainingBytes) + 4) : "";
              } else {
                while (bufferChunks.length < 3) {
                  const newChunks = bufferToParse.split("\r\n\r\n");
                  bufferChunks = newChunks.length > 2 ? newChunks : bufferChunks;
                  if (bufferChunks.length < 3) {
                    const possiblePromise = await read(offset, readBufferSize);
                    bufferToParse += possiblePromise.toString();
                    offset += readBufferSize;
                  }
                }
                http = mWarcParseHeader(bufferChunks[1]);
                const httpContentLength = contentLength - BigInt(bufferChunks[1].length);
                const remainingBytes = httpContentLength - BigInt(bufferToParse.length - bufferChunks[0].length - bufferChunks[1].length - 8);
                content = skipContent ? undefined : Buffer.from(bufferToParse.slice(bufferChunks[0].length + bufferChunks[1].length + 8, bufferChunks[0].length + bufferChunks[1].length + 8 + Number(httpContentLength)) + (remainingBytes > 0n ? (await read(offset, remainingBytes)).toString() : ""));
                offset += remainingBytes > 0n ? remainingBytes : 0n;
                backbuffer = remainingBytes < 0n ? bufferToParse.slice(bufferToParse.length + Number(remainingBytes) + 4) : "";
                metadata.recordResponseOffset = metadata.recordWarcOffset + BigInt(bufferChunks[0].length + 4);
                metadata.recordContentOffset = metadata.recordResponseOffset + BigInt(bufferChunks[1].length + 4);
                http["content-length"] = httpContentLength;
              }
            } while (header && header["warc-type"] !== "response");
            if (!header) {
              done = true;
              return { done: true, value: undefined };
            }
            return {
              done: false,
              value: [header, http, content, metadata]
            };
          } catch (err) {
            console.error("Error encountered:", err);
            if (err instanceof RangeError) {
              done = true;
              return { done: true, value: undefined };
            } else {
              throw err;
            }
          }
        }
      };
    }
  };
};
var mWarcParseResponseContent = (content, transferEncoding) => {
  switch (transferEncoding) {
    case "chunked":
      let chHex = "";
      let chOffset = 0n;
      let chPosition = 0n;
      const chunkPromise = (chunk) => {
        let filtered = "";
        if (chOffset > BigInt(chunk.length)) {
          chOffset -= BigInt(chunk.length);
          return chunk;
        }
        chPosition = chOffset;
        while (chPosition < chunk.length) {
          const chHexC = chunk instanceof Buffer ? String.fromCharCode(chunk[Number(chPosition)]) : typeof chunk === "string" ? chunk.charAt(Number(chPosition)) : "";
          chHex += chHexC;
          chPosition++;
          if (chHex.endsWith("\r\n") && chHex !== "0\r\n") {
            chOffset = hexToBn(chHex.slice(0, chHex.length - 2), { unsigned: true });
            const startSlice = chPosition;
            const endSlice = chPosition + chOffset;
            const slice = chunk.slice(Number(startSlice), Number(endSlice));
            filtered += slice.toString();
            chPosition += chOffset + 2n;
            const rem = chunk.length - Number(chPosition);
            chOffset -= BigInt(slice.length) - 2n;
            chHex = "";
          } else if (chHex.endsWith("\r\n") && chHex === "0\r\n") {
            break;
          }
        }
        return filtered;
      };
      return content.pipe(new Transform({
        async transform(chunk, encoding, callback) {
          try {
            const result = await chunkPromise(chunk);
            callback(null, result);
          } catch (error) {
            callback(error);
          }
        }
      }));
    default:
      return content;
  }
};
var mWarcParseEtag = (content) => {
  const hasher = new Bun.CryptoHasher("sha256");
  return new Promise((resolve, reject) => {
    if (Buffer.isBuffer(content)) {
      hasher.update(content);
      resolve(`"${hasher.digest("hex")}"`);
    } else if (content instanceof NodeReadable) {
      content.on("data", (chunk) => hasher.update(chunk));
      content.on("end", () => resolve(`"${hasher.digest("hex")}"`));
      content.on("error", reject);
    } else if (content instanceof fs.ReadStream) {
      content.on("data", (chunk) => hasher.update(chunk));
      content.on("end", () => resolve(`"${hasher.digest("hex")}"`));
      content.on("error", reject);
    } else if (content instanceof globalThis.ReadableStream) {
      const reader = content.getReader();
      const read = async () => {
        try {
          const { done, value } = await reader.read();
          if (done) {
            resolve(`"${hasher.digest("hex")}"`);
          } else if (value) {
            hasher.update(Buffer.from(value));
            read();
          }
        } catch (error) {
          reject(error);
        }
      };
      read();
    } else {
      reject(new Error("Unsupported content type"));
    }
  });
};

// libs/database/types.tsx
import net from "net";
var worker = null;
var promises = new Map;
var getWorker = async (options) => {
  if (!worker) {
    const build = await Bun.build({
      entrypoints: ["libs/database/worker.tsx"],
      outdir: "libs/database/build",
      target: process.env.WORKER_TARGET ?? "node",
      minify: false
    });
    worker = new Worker(build.outputs[0].path);
    worker.onmessage = (event) => {
      const { id, status, data, message, action } = event.data;
      const handlers = promises.get(id);
      if (action) {
        if (action == "log") {
          console.log(`WS-DB: ${message}`);
        }
      } else if (handlers) {
        if (status === "error") {
          handlers.reject(message);
        } else {
          handlers.resolve(data);
        }
        promises.delete(id);
      }
    };
  }
  return { worker, promises };
};
var getNetSocket = async (options, promises2) => {
  const { host, port } = options;
  return new Promise((resolve) => {
    const socket = net.createConnection({ host, port }, () => {
      postMessage({ action: "log", message: `Connected to ${host}:${port}` });
      resolve({ socket, promises: promises2 });
    });
    socket.on("data", (buffer) => {
      const data = buffer.toString("utf-8");
      try {
        const { id, status, data: responseData, message, action } = JSON.parse(data);
        const handlers = promises2.get(id);
        if (action) {
          if (action === "log") {
            console.log(`WS-NET: ${message}`);
          }
        } else if (handlers) {
          if (status === "error") {
            handlers.reject(message);
          } else {
            handlers.resolve(responseData);
          }
          promises2.delete(id);
        }
      } catch (error) {
        postMessage({ action: "log", message: "Failed to parse data: " + error.message });
      }
    });
    socket.on("error", (err) => {
      postMessage({ action: "log", message: `Socket error: ${err.message}` });
    });
    socket.on("end", () => {
      postMessage({ action: "log", message: "Disconnected from the server" });
    });
  });
};

// libs/genid/index.tsx
var genid = () => {
  const timestamp = Date.now();
  const randomComponent = Math.floor(Math.random() * 1000);
  return timestamp + randomComponent;
};

// libs/database/index.tsx
var worker2 = null;
var promises2 = new Map;
var method = null;
var socket = null;
var handleSocketMessage = (data) => {
  const str = `[${data.toString("utf-8").trim().replace(/}{/g, "},{")}]`;
  const jsonarrstr = JSON.parse(str);
  jsonarrstr.forEach((responsestr) => {
    try {
      const { id, result, status, error, message, data: data2 } = responsestr;
      const promise = promises2.get(id);
      if (promise) {
        promises2.delete(id);
        if (status !== "sucess") {
          promise.reject(message);
        } else {
          promise.resolve(data2);
        }
      }
    } catch (err) {
      console.log(`Error ${err.message}: \r\n` + jsonarrstr + "\r\n" + responsestr);
    }
  });
};
var sendMessage = (options) => {
  const { id, action, params } = options;
  const messageToSend = {
    id,
    action,
    params: params !== undefined ? params : {}
  };
  switch (method) {
    case "post":
      worker2?.postMessage(messageToSend);
      break;
    case "net":
      if (socket) {
        socket.write(JSON.stringify(messageToSend, (_, v) => typeof v === "bigint" ? v.toString() : v), (err) => {
          if (err) {
            console.error(`Error writing to socket: ${err.message}`);
          }
        });
      }
      break;
  }
};
var callAction = (action, params) => {
  return new Promise((resolve, reject) => {
    const id = genid();
    promises2.set(id, { resolve, reject });
    sendMessage({ id, action, params });
  });
};
var dbInsertResponse = async (params) => callAction("dbInsertResponse", params);
var dbConnectWorker = async () => callAction("connectDb");
var connectDb = async (params, options) => {
  const { connType = "post" } = options || {};
  switch (connType) {
    case "post":
      const workerResult = await getWorker();
      worker2 = workerResult.worker;
      promises2 = workerResult.promises;
      method = "post";
      console.log("Connecting worker");
      await dbConnectWorker();
      break;
    case "net":
      if (!options?.host || !options?.port) {
        throw new Error("Host and port must be provided for net connection");
      }
      const netSocketResult = await getNetSocket({ host: options.host, port: options.port }, promises2);
      socket = netSocketResult.socket;
      promises2 = netSocketResult.promises;
      method = "net";
      socket.on("data", handleSocketMessage);
      socket.on("error", (err) => {
        console.error(`Socket error: ${err.message}`);
        promises2.forEach((p) => p.reject(err));
        promises2.clear();
      });
      socket.on("end", () => {
        console.log("Socket connection closed");
        socket = null;
      });
      break;
  }
};
var closeDb = async () => {
  switch (method) {
    case "post":
      if (worker2) {
        await callAction("closeDb");
        worker2.terminate();
        worker2 = null;
      }
      break;
    case "net":
      if (socket) {
        socket.destroy();
        socket = null;
      }
      break;
  }
  promises2.clear();
};

// libs/warcs/worker.ts
var promiseRets = new Map;
var convertBigIntToNumber = (obj) => {
  if (typeof obj === "bigint") {
    if (obj > Number.MAX_SAFE_INTEGER || obj < Number.MIN_SAFE_INTEGER) {
      throw new Error("BigInt value out of safe Number range");
    }
    return Number(obj);
  } else if (typeof obj === "object" && obj !== null) {
    for (const key in obj) {
      if (obj.hasOwnProperty(key)) {
        obj[key] = convertBigIntToNumber(obj[key]);
      }
    }
  }
  return obj;
};
var readFile = async (filename) => {
  const fd = await open(filename, "r");
  const fileStats = await fd.stat();
  const fileSize = fileStats.size;
  return async (offset, size) => {
    if (offset >= fileSize || offset + size > fileSize) {
      throw new RangeError("Out of bounds read attempt");
    }
    const buffer = Buffer.alloc(Number(size));
    const { bytesRead } = await fd.read(buffer, 0, Number(size), Number(offset));
    if (bytesRead !== Number(size)) {
      throw new RangeError("Failed to read the expected number of bytes");
    }
    return buffer;
  };
};
var parseWarcFile = async (file) => {
  const fileDir = `warcs/${file}`;
  const fd = await open(fileDir, "r");
  const warc = mWarcParseResponses(await readFile(fileDir), { skipContent: true });
  console.log(`   Parsing ${file}...`);
  const fileSize = (await fs2.stat(fileDir)).size;
  let lastPercent = 0;
  const promises3 = [];
  try {
    for await (const [header, http, content, metadata] of warc) {
      const {
        "warc-type": warcType,
        "warc-record-id": recordId,
        "warc-warcinfo-id": warcinfoId,
        "warc-concurrent-to": concurrentTo,
        "warc-target-uri": targetUri,
        "warc-date": warcDate,
        "warc-ip-address": ipAddress,
        "warc-block-digest": blockDigest,
        "warc-payload-digest": payloadDigest,
        "content-type": contentType,
        "content-length": contentLength
      } = header;
      const { recordWarcOffset, recordResponseOffset, recordContentOffset } = metadata;
      const {
        date,
        location,
        "content-type": responseType,
        "content-length": responseContentLength,
        "last-modified": lastModified,
        "transfer-encoding": transferEncoding,
        status
      } = http;
      const etag = await mWarcParseEtag(mWarcParseResponseContent(fd.createReadStream({
        start: Number(recordContentOffset),
        end: Number(recordContentOffset) + Number(responseContentLength) - 1
      }), transferEncoding)).catch((e) => {
        console.log(`Failed to parse etag for record ${e.message}`, targetUri);
      });
      if (!etag)
        continue;
      const recordData = {
        uri_string: targetUri.replace(/<|>/g, ""),
        file_string: `warcs/${file}`,
        content_type_string: responseType ?? "application/unknown",
        resource_type_string: "response",
        record_length: BigInt(recordResponseOffset),
        record_offset: BigInt(recordWarcOffset),
        content_length: BigInt(responseContentLength),
        content_offset: BigInt(recordContentOffset),
        status,
        meta: convertBigIntToNumber(http)
      };
      promises3.push(dbInsertResponse(recordData).then(async () => {
        const percent = Math.round(Number(recordWarcOffset) / fileSize * 100);
        if (percent > lastPercent) {
          lastPercent = percent;
          postMessage({ file, status: "progress", progress: percent });
        }
      }).catch((e) => {
        console.log(`Failed to insert record ${e.message}`, recordData);
      }));
    }
  } catch (error) {
    if (error instanceof RangeError) {
      console.log("RangeError encountered, exiting parsing loop.");
    } else {
      console.error("An unexpected error occurred:", error);
      throw error;
    }
  } finally {
    await Promise.allSettled(promises3).then(() => {
      console.log(`   Parsed ${file}!`);
      closeDb().then(() => {
        postMessage({ file, status: "complete" });
      });
    });
  }
};
self.onmessage = async (event) => {
  console.log("entry");
  const data = event.data;
  const { file, channel } = data;
  if (typeof data !== "object" || !file) {
    console.log(`WARC Worker, invalid format ${data}`);
    return;
  }
  await connectDb(undefined, { connType: "net", host: "127.0.0.1", port: 9824 });
  console.log(`WARC Worker: starting to parse file: ${file} ${channel}`);
  parseWarcFile(data.file);
};
