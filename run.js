
const WebSocket = require('ws');
const MongoClient = require('mongodb').MongoClient;
const https = require('https')

const mongoUrl = "mongodb://localhost:27017/";
const symbol = "BTCUSDT"
const depthLimit = 1000
const depthURL= `https://api.binance.com/api/v3/depth?symbol=${symbol}&limit=${depthLimit}`

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
                "type": "snapshot",
                "insertionTime": (new Date).getTime(),
            }

            depths.insertOne(doc, function(err, res) {
                if (err) throw err;

                console.log(`depth snapshot, lastUpdateId = ${depth.lastUpdateId}`);
            });
        })
    };


    const ws = new WebSocket(`wss://stream.binance.com:9443/ws/${symbol.toLowerCase()}@depth`, {
        perMessageDeflate: false
    });

    ws.on('open', () => {
        console.log("connection open")

        getDepthAndInsert()

        // start timer
        timer = setInterval(getDepthAndInsert, 1000 * 60);
    });

    ws.on('close', () => {
        clearInterval(timer);
    });

    ws.on('message', (data) => {
        var diff = JSON.parse(data);

        if (lastDepth) {
            // The first processed event should have U <= lastUpdateId+1 AND u >= lastUpdateId+1.
            if (!lastDiff) {
                if (diff.U > lastDepth.lastUpdateId + 1) {
                    console.log(`dropping diff because the first id is too new: ${diff.U} > ${lastDepth.lastUpdateId + 1}`)
                    return
                }

                if (diff.u < lastDepth.lastUpdateId + 1) {
                    console.log(`dropping diff because the final id is too old: ${diff.u} > ${lastDepth.lastUpdateId + 1}`)
                    return
                }
            }

            lastDiff = diff

            let doc = {
                "type": "diff",
                "insertionTime": (new Date).getTime(),
                "eventTime": diff.E,
                "diff": diff,
            }

            depths.insertOne(doc, function(err, res) {
                if (err) throw err;

                console.log(`inserted diff, record: ${res.insertedId}: event: ${diff.E}, bids: ${diff.b.length}, asks: ${diff.a.length}`)
            });
        }
    });

    // db.close();
});
/*
*/
