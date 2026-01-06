# 📉 Global Tech Layoffs – Data Cleaning Project (SQL)

## 📌 Overview

This project focuses on **cleaning and preparing a real-world global layoffs dataset** using **SQL**. The dataset contains information about company layoffs across industries, locations, funding stages, and dates.

The goal of this project is to transform **raw, inconsistent data** into a **clean, analysis-ready table**, following industry-standard data cleaning practices.

This repository is ideal for:

* Practicing **SQL data cleaning**
* Learning **real-world data preprocessing**
* Building a **data analyst portfolio project**

---

## 🗂 Dataset Description

The dataset (`layoffs.csv`) contains records of layoffs from companies worldwide.

### Key Columns:

| Column Name             | Description                          |
| ----------------------- | ------------------------------------ |
| `company`               | Name of the company                  |
| `location`              | City where layoffs occurred          |
| `industry`              | Industry sector                      |
| `total_laid_off`        | Number of employees laid off         |
| `percentage_laid_off`   | Percentage of workforce laid off     |
| `date`                  | Date of the layoff                   |
| `stage`                 | Company funding stage                |
| `country`               | Country of the company               |
| `funds_raised_millions` | Total funds raised (in millions USD) |

---

## 🛠 Tools & Technologies

* **SQL (MySQL)**
* **CSV dataset**
* **Relational database concepts**

---

## 🧼 Data Cleaning Steps Performed

The cleaning logic is implemented in `DataCleaning.sql`.

### 1️⃣ Removed Duplicate Records

* Identified and eliminated duplicate rows to ensure data accuracy.

### 2️⃣ Standardized Text Data

* Trimmed extra spaces
* Unified inconsistent naming conventions (e.g., industries, countries)

### 3️⃣ Handled NULL & Missing Values

* Investigated missing values in critical columns
* Retained NULLs where data was genuinely unavailable

### 4️⃣ Date Formatting

* Converted the `date` column from text to proper SQL `DATE` format

### 5️⃣ Column Data Type Fixes

* Ensured numeric columns were correctly typed
* Improved schema consistency for analysis

### 6️⃣ Removed Irrelevant Records

* Deleted rows where layoff data was not meaningful or usable

---

## 🧾 SQL File Structure

```sql
-- Select raw data
SELECT * FROM layoffs;

-- Remove duplicates
-- Standardize text fields
-- Handle null values
-- Convert date format
-- Final cleaned table
```

Each step is clearly commented in the SQL file for easy understanding.

---

## 📊 Final Output

The cleaned dataset is:

* Free of duplicates
* Properly formatted
* Ready for **exploratory data analysis**, **visualization**, or **dashboarding**

This dataset can now be used in:

* Power BI / Tableau
* Python (Pandas, Matplotlib)
* Advanced SQL analysis

---

## 🚀 How to Use This Project

1. Import `layoffs.csv` into your SQL database
2. Run `DataCleaning.sql` step-by-step
3. Query the cleaned table for insights

---

## 📁 Repository Structure

```
📦 Global-Tech-Layoffs
 ┣ 📄 layoffs.csv
 ┣ 📄 DataCleaning.sql
 ┗ 📄 README.md
```

---

## 🌟 Why This Project Matters

This project demonstrates:

* Real-world **data cleaning skills**
* Strong **SQL fundamentals**
* Attention to **data quality and reliability**

Perfect for showcasing on GitHub or discussing in interviews.

---

## 🙌 Acknowledgements

Dataset inspired by publicly available layoff tracking data used for educational purposes.

---

