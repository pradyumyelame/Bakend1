const express = require("express");
const bodyParser = require("body-parser");
const cors = require("cors");
const cassandra = require("cassandra-driver");
const path = require("path");
require("dotenv").config();

const app = express();
const port = process.env.PORT || 5000;

app.use(cors());
app.use(bodyParser.json());

// Cassandra setup
const client = new cassandra.Client({
  cloud: {
    secureConnectBundle: path.resolve(__dirname, process.env.ASTRA_DB_BUNDLE_PATH),
  },
  credentials: {
    username: process.env.ASTRA_DB_CLIENT_ID,
    password: process.env.ASTRA_DB_SECRET,
  },
  keyspace: process.env.ASTRA_DB_KEYSPACE,
});

// Connect to Astra DB
client.connect()
  .then(() => console.log("✅ Connected to Astra DB (Cassandra)"))
  .catch((err) => console.error("❌ Connection error:", err));

// POST - Insert country
app.post("/submit", async (req, res) => {
  const { country, capital, population } = req.body;

  // Validation check for input data
  if (!country || !capital || !population) {
    return res.status(400).json({ error: "All fields (country, capital, population) are required." });
  }

  // Validate population is a positive number
  if (isNaN(population) || population <= 0) {
    return res.status(400).json({ error: "Population must be a valid positive number." });
  }

  try {
    const query = "INSERT INTO countries (country, capital, population) VALUES (?, ?, ?)";
    await client.execute(query, [country, capital, parseInt(population)], { prepare: true });
    res.status(200).json({ message: "✅ Data inserted successfully" });
  } catch (err) {
    console.error("❌ Error inserting data:", err);
    res.status(500).json({ error: "Error inserting data" });
  }
});

// GET - Fetch all countries with pagination
app.get("/countries", async (req, res) => {
  const { page = 1, limit = 10 } = req.query;  // Pagination
  const offset = (page - 1) * limit;

  try {
    const query = "SELECT * FROM countries LIMIT ? OFFSET ?";
    const result = await client.execute(query, [parseInt(limit), parseInt(offset)], { prepare: true });
    res.status(200).json(result.rows);
  } catch (err) {
    console.error("❌ Error fetching countries:", err);
    res.status(500).json({ error: "Error fetching countries" });
  }
});

// DELETE - Delete a country by name
app.delete("/countries/:country", async (req, res) => {
  const { country } = req.params;

  try {
    // Check if the country exists
    const countryCheck = await client.execute('SELECT * FROM countries WHERE country = ?', [country]);
    if (countryCheck.rows.length === 0) {
      return res.status(404).json({ error: "Country not found." });
    }

    const query = "DELETE FROM countries WHERE country = ?";
    await client.execute(query, [country], { prepare: true });
    res.status(200).json({ message: "✅ Country deleted successfully" });
  } catch (err) {
    console.error("❌ Error deleting country:", err);
    res.status(500).json({ error: "Error deleting country" });
  }
});

// PUT - Update a country's details
app.put("/countries/:country", async (req, res) => {
  const { country } = req.params;
  const { newCountry, capital, population } = req.body;

  // Validation check for input data
  if (!newCountry || !capital || !population) {
    return res.status(400).json({ error: "All fields (newCountry, capital, population) are required." });
  }

  // Validate population is a positive number
  if (isNaN(population) || population <= 0) {
    return res.status(400).json({ error: "Population must be a valid positive number." });
  }

  try {
    // Check if the country exists
    const countryCheck = await client.execute('SELECT * FROM countries WHERE country = ?', [country]);
    if (countryCheck.rows.length === 0) {
      return res.status(404).json({ error: "Country not found." });
    }

    const query = `
      UPDATE countries 
      SET country = ?, capital = ?, population = ? 
      WHERE country = ?
    `;
    await client.execute(query, [newCountry, capital, parseInt(population), country], { prepare: true });

    res.status(200).json({ message: "✅ Country updated successfully" });
  } catch (err) {
    console.error("❌ Error updating country:", err);
    res.status(500).json({ error: "Error updating country" });
  }
});

// Start server
app.listen(port, () => {
  console.log(`🚀 Server running on http://localhost:${port}`);
});
