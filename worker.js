const { parentPort, workerData } = require("worker_threads");
const csv = require("csv-parser");
const fs = require("fs");

const { path, type } = workerData;

const map = new Map();

fs.createReadStream(path)
  .pipe(csv())
  .on("data", (data) => {
    let key;
    switch (type) {
      case "SALES_TRAN_DETAIL":
        key = `${data.TRAN_NO}-${data.TRAN_DETAIL_SEQ_NO}`;
        map.set(key, data);
        break;
      case "SALES_TRAN":
        key = data.TRAN_NO;
        map.set(key, data);
        break;
      case "CUSTOMER_DATA":
        key = data.CUST_ID_EC;
        map.set(key, data);
        break;
      default:
        break;
    }
  })
  .on("end", () => {
    parentPort.postMessage({ type, msg: "succeed", map });
  });
