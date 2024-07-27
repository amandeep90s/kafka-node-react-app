const express = require("express");
const mysql = require("mysql2");
const cors = require("cors");
const dotenv = require("dotenv");

dotenv.config();

const app = express();
const port = 8080;

app.use(cors());

// Create a connection to the database
const pool = mysql.createPool({
  connectionLimit: 10,
  host: process.env.MYSQL_DBHOST,
  user: process.env.MYSQL_DBUSER,
  password: process.env.MYSQL_DBPASS,
  database: process.env.MYSQL_DBNAME,
});

app.use(express.json());

app.get("/api/v1/active-trains", (req, res) => {
  const limit = Number(req.query.limit) || 10;
  const offset = Number(req.query.offset) || 10;
  const sql = "SELECT * FROM active_trains LIMIT ? OFFSET ?";
  pool.query(sql, [limit, offset], (error, results) => {
    if (error) {
      console.error("Error fetching active trains:", error);
      res.status(500).send("Internal Server Error");
    } else {
      res.status(200).json(results);
    }
  });
});

app.get("/api/v1/cancelled-trains", (req, res) => {
  const limit = parseInt(req.query.limit, 10) || 10;
  const offset = parseInt(req.query.offset, 10) || 0;

  const sql = "SELECT * FROM cancelled_trains LIMIT ? OFFSET ?";
  pool.query(sql, [limit, offset], (error, results) => {
    if (error) {
      console.error("Error fetching cancelled trains:", error);
      return res.status(500).json({ error: "Internal Server Error" });
    }
    res.json(results);
  });
});

app.listen(port, () =>
  console.log(`Server running on http://localhost:${port}`)
);
