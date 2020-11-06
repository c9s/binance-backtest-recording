
const WebSocket = require('ws');
const MongoClient = require('mongodb').MongoClient;
const https = require('https')

const mongoUrl = "mongodb://localhost:27017/";
const symbol = "BTCUSDT"
const depthLimit = 1000
const depthURL= `https://api.binance.com/api/v3/depth?symbol=${symbol}&limit=${depthLimit}`

let commandId = 1

function getJSON(url, cb) {
    https.get(depthURL, (resp) => {
        let data = '';

        // A chunk of data has been recieved.
        resp.on('data', (chunk) => {
            data += chunk;
        });

        // The whole response has been received. Print out the result.
        resp.on('end', () => {
            cb(JSON.parse(data));
        });

    }).on("error", (err) => {
        console.log("Error: " + err.message);
    });
}

function subscribeCommand(params, id = null) {
    return JSON.stringify({
        "method": "SUBSCRIBE",
        // "params": [ "btcusdt@aggTrade", "btcusdt@depth" ],
        "params": params,
        "id": (id ? id : commandId++)
    })
}


MongoClient.connect(mongoUrl, (err, db) => {
    if (err) {
        throw err;
    }

    var dbo = db.db("binance");
    var depths = dbo.collection("depths")
    var lastDepth = null;
    var timer = null;
    var lastDiff = null;

    const getDepthAndInsert = () => {
        getJSON(depthURL, (depth) => {
            lastDepth = depth

            let doc = {
                "type": "depthSnapshot",
                "insertionTime": (new Date).getTime(),
            }

            depths.insertOne(doc, function(err, res) {
                if (err) throw err;

                console.log(`depth snapshot, lastUpdateId = ${depth.lastUpdateId}`);
            });
        })
    };


    const ws = new WebSocket(`wss://stream.binance.com:9443/ws`, {
        perMessageDeflate: false
    });

    ws.on('open', () => {
        console.log("connection open")

        ws.send(subscribeCommand([
            `${symbol.toLowerCase()}@depth`,
            `${symbol.toLowerCase()}@kline_1m`,
            `${symbol.toLowerCase()}@kline_5m`,
            `${symbol.toLowerCase()}@kline_15m`,
            `${symbol.toLowerCase()}@kline_30m`,
            `${symbol.toLowerCase()}@kline_1h`,
            `${symbol.toLowerCase()}@kline_2h`,
            `${symbol.toLowerCase()}@kline_4h`,
            `${symbol.toLowerCase()}@kline_8h`,
            `${symbol.toLowerCase()}@kline_12h`,
            `${symbol.toLowerCase()}@kline_1d`,
        ]))

        getDepthAndInsert()

        // start timer
        timer = setInterval(getDepthAndInsert, 1000 * 60);
    });

    ws.on('close', () => {
        clearInterval(timer);
    });

    ws.on('message', (data) => {
        var e = JSON.parse(data);
        if (e.e == "kline") {

            if (!e.k.x) {
                return
            }

            let doc = {
                "type": "kline",
                "insertionTime": (new Date).getTime(),
                "eventTime": e.E,
                "kline": e,
            }

            depths.insertOne(doc, function(err, res) {
                if (err) throw err;

                console.log(`inserted kline, record: ${res.insertedId}: event: ${e.E}, O: ${e.o} H: ${e.o} L: ${e.o} C: ${e.o}`)
            });

        } else if (e.e == "depthUpdate") {
            if (lastDepth) {
                // The first processed event should have U <= lastUpdateId+1 AND u >= lastUpdateId+1.
                if (!lastDiff) {
                    if (e.U > lastDepth.lastUpdateId + 1) {
                        console.log(`dropping diff because the first id is too new: ${e.U} > ${lastDepth.lastUpdateId + 1}`)
                        return
                    }

                    if (e.u < lastDepth.lastUpdateId + 1) {
                        console.log(`dropping diff because the final id is too old: ${e.u} > ${lastDepth.lastUpdateId + 1}`)
                        return
                    }
                }

                lastDiff = e

                let doc = {
                    "type": "depthDiff",
                    "insertionTime": (new Date).getTime(),
                    "eventTime": e.E,
                    "diff": e,
                }

                depths.insertOne(doc, function(err, res) {
                    if (err) throw err;

                    console.log(`inserted diff, record: ${res.insertedId}: event: ${e.E}, bids: ${e.b.length}, asks: ${e.a.length}`)
                });
            }
        }
    });

    // db.close();
});
/*
*/
