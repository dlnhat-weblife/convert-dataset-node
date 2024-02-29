const { Worker } = require("worker_threads");
const fs = require("fs");

const SALES_TRAN_DETAIL_path = "./input/SALES_TRAN_DETAIL.csv"; // Replace with actual SALES_TRAN_DETAIL path or replace the SALES_TRAN_DETAIL file
const SALES_TRAN_path = "./input/SALES_TRAN.csv"; // Replace with actual SALES_TRAN path or replace the SALES_TRAN file
const CUSTOMER_DATA_path = "./input/CUSTOMER_DATA.csv"; // Replace with actual CUSTOMER_DATA path or replace the CUSTOMER_DATA file

let std = new Map();
let st = new Map();
let cd = new Map();
const rs = [
  [
    "Metafield: retail.cust_id [single_line_text_field]",
    "Email",
    "Number",
    "Tags",
    "Tags Command",
    "Created At",
    "Line: Type",
    "Line: Product Handle",
    "Line: Title",
    "Line: Price",
    "Line: Quantity",
    "Line: Total",
    "Tax: Included",
    "Tax: Total",
    "Payment: Status",
    "Line: Taxable",
    "Line: Requires Shipping",
    "Order Fulfillment Status",
    "Fulfillment: Status",
  ],
];

const assignWorker = async () => {
  [
    {
      path: SALES_TRAN_DETAIL_path,
      type: "SALES_TRAN_DETAIL",
    },
    {
      path: SALES_TRAN_path,
      type: "SALES_TRAN",
    },
    {
      path: CUSTOMER_DATA_path,
      type: "CUSTOMER_DATA",
    },
  ].forEach(async (props) => {
    try {
      await readCSV(props);
    } catch (err) {
      console.error(err);
    }
  });
};

const readCSV = (props) => {
  return new Promise((resolve, reject) => {
    const worker = new Worker("./worker.js", {
      workerData: props,
    });

    worker.on("message", (data) => {
      switch (data.type) {
        case "SALES_TRAN_DETAIL":
          std = data.map;
          break;
        case "SALES_TRAN":
          st = data.map;
          break;
        case "CUSTOMER_DATA":
          cd = data.map;
          break;
        default:
          break;
      }
      handleData();
      resolve();
    });
    worker.on("error", reject);
    worker.on("exit", (code) => {
      if (code !== 0) reject(new Error(`stopped with ${code} exit code`));
    });
  });
};

const handleData = () => {
  while (std.size) {
    const [key, value] = std.entries().next().value;
    const {
      TRAN_NO,
      STORE_CODE,
      SALES_DATE,
      PRODUCT_CD,
      PRODUCT_NAME,
      UNIT_PRICE,
      SALES_QTY,
      SALES_UNIT_PRICE,
      SALES_TAX,
    } = value;
    const { CUST_ID } = st.get(TRAN_NO);
    let EMAIL_ADDRESS = "";
    if (CUST_ID && cd.has(CUST_ID)) {
      EMAIL_ADDRESS = cd.get(CUST_ID).EMAIL_ADDRESS;
    }
    rs.push([
      CUST_ID,
      EMAIL_ADDRESS,
      STORE_CODE + ",実店舗",
      "REPLACED",
      SALES_DATE,
      "Line Item",
      PRODUCT_CD,
      PRODUCT_NAME,
      UNIT_PRICE,
      SALES_QTY,
      SALES_UNIT_PRICE,
      "FALSE",
      SALES_TAX,
      "paid",
      "TRUE",
      "FALSE",
      "fulfilled",
      "success",
    ]);
    std.delete(key);
  }
  writeToFile();
};

const writeToFile = () => {
  fs.writeFileSync(
    "./output/orders.csv",
    rs.map((r) => r.join(",")).join("\n"),
    "utf-8",
    (err) => {
      if (err) {
        console.log("Error writing file", err, "\n");
      } else {
        console.log("File written successfully\n");
      }
    }
  );
};

assignWorker().catch(console.error);

// async function readCSVs() {
//   const promise1 = new Promise((resolve, reject) => {
//     fs.createReadStream(SALES_TRAN_DETAIL_path)
//       .pipe(csv())
//       .on("data", (data) => {
//         const key = `${data.TRAN_NO}-${data.TRAN_DETAIL_SEQ_NO}`;
//         std.set(key, data);
//       })
//       .on("end", () => resolve())
//       .on("error", (err) => reject(err));
//   });
//   const promise2 = new Promise((resolve, reject) => {
//     fs.createReadStream(SALES_TRAN_path)
//       .pipe(csv())
//       .on("data", (data) => {
//         const key = data.TRAN_NO;
//         st.set(key, data);
//       })
//       .on("end", () => resolve())
//       .on("error", (err) => reject(err));
//   });
//   const promise3 = new Promise((resolve, reject) => {
//     fs.createReadStream(CUSTOMER_DATA_path)
//       .pipe(csv())
//       .on("data", (data) => {
//         const key = data.CUST_ID_EC;
//         cd.set(key, data);
//       })
//       .on("end", () => resolve())
//       .on("error", (err) => reject(err));
//   });
//   await Promise.all([promise1, promise2, promise3]);
//   return;
// }

// readCSVs()
//   .then(() => {
//     if (std.size === 0 || st.size === 0 || cd.size === 0)
//       throw new Error("Invalid data in CSVs\n");
//     while (std.size) {
//       const [key, value] = std.entries().next().value;
//       const {
//         TRAN_NO,
//         STORE_CODE,
//         SALES_DATE,
//         PRODUCT_CD,
//         PRODUCT_NAME,
//         UNIT_PRICE,
//         SALES_QTY,
//         SALES_UNIT_PRICE,
//         SALES_TAX,
//       } = value;
//       const { CUST_ID } = st.get(TRAN_NO);
//       let EMAIL_ADDRESS = "";
//       if (CUST_ID && cd.has(CUST_ID)) {
//         EMAIL_ADDRESS = cd.get(CUST_ID).EMAIL_ADDRESS;
//       }
//       rs.push([
//         CUST_ID,
//         EMAIL_ADDRESS,
//         STORE_CODE + ",実店舗",
//         "REPLACED",
//         SALES_DATE,
//         "Line Item",
//         PRODUCT_CD,
//         PRODUCT_NAME,
//         UNIT_PRICE,
//         SALES_QTY,
//         SALES_UNIT_PRICE,
//         "FALSE",
//         SALES_TAX,
//         "paid",
//         "TRUE",
//         "FALSE",
//         "fulfilled",
//         "success",
//       ]);
//       std.delete(key);
//     }
//     console.log("Data processed successfully\n");
//     return;
//   })
//   .then(() => {
//     fs.writeFileSync(
//       "./output/orders.csv",
//       rs.map((r) => r.join(",")).join("\n"),
//       "utf-8",
//       (err) => {
//         if (err) {
//           console.log("Error writing file", err, "\n");
//         } else {
//           console.log("File written successfully\n");
//         }
//       }
//     );
//   });
