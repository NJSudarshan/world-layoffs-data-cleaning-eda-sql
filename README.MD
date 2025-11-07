# 🌍 World Layoffs Analysis (MySQL)

_Analyzing total laid off and percent laid off over different factors to understand the trends aand patterns of layoff using MySql._

---


## 📌 Table of Contents
- <a href="#overview">Overview</a>
- <a href="#dataset-description">Dataset Description</a>
- <a href="#tools--technologies">Tools & Technologies</a>
- <a href="#project-structure">Project Structure</a>
- <a href="#data-cleaning-steps">Data Cleaning Steps</a>
- <a href="#exploratory-data-analysis-eda">Exploratory Data Analysis (EDA)</a>
- <a href="#key-insights">Key Insights</a>
- <a href="#author--contact">Author & Contact</a>


---
<h2><a class="anchor" id="overview"></a>Overview</h2>

This project analyzes global layoff trends using real-world data. It focuses on data cleaning, preprocessing, and exploratory analysis using MySQL. The goal is to uncover patterns across industries, countries, and time periods, and provide actionable insights for business leaders and policymakers.


---
<h2><a class="anchor" id="dataset-description"></a>Dataset Description</h2>

The dataset includes:
- Company 
- Location
- Industry
- Total laid off 
- Percent laid off
- date
- stage
- Country
- Funds raised millions


---
<h2><a class="anchor" id="tools--technologies"></a>Tools & Technologies</h2>

- MySql (Common table expressions, Joins, Window functions)
- Power BI (Data Visualization)
- Github


---
<h2><a class="anchor" id="project-structure"></a>Project Structure</h2>

```
├── Data/
│   ├── raw_data.csv
│   ├── cleaned_data.csv
├── SQL Scripts/
│   ├── data_cleaning.sql
│   ├── eda_queries.sql
├── README.md
├── Insights/
│   ├── summary_notes.txt
│   ├── dashboard_screenshots/
```


---
<h2><a class="anchor" id="data-cleaning-steps"></a>Data Cleaning Steps</h2>

Performed in MySQL using SQL queries:
- Removed duplicates 
- standardize data and fixed errors
- Converted date formats to `YYYY-MM-DD`
- Populated null and blanks using joins
- Removed columns and rows that are not necessar 


---

<h2><a class="anchor" id="exploratory-data-analysis-eda"></a>Exploratory Data Analysis (EDA)</h2>

### Key questions addressed:
- 📈 Which industries faced the most layoffs?
- 🌐 Which countries were most affected?
- 🕰️ What were the peak months/years for layoffs?
- 🏢 Which companies had the largest layoffs?


---
<h2><a class="anchor" id="key-insights"></a>Key Insights</h2>

- Tech and retail sectors saw the highest layoffs globally.
- Layoffs peaked during early 2020 and mid-2022.
- Startups in Series B and C stages were disproportionately affected.
- The U.S. accounted for over 60% of recorded layoffs.


---
<h2><a class="anchor" id="author--contact"></a>Author & Contact</h2>

**NJ Sudarshan Achar**  
Data Analyst  
📧 Email: sudarshanachar39@gmail.com  
🔗 [LinkedIn](https://www.linkedin.com/in/nj-sudarshan-b34392193/)  
🔗 [Portfolio](https://github.com/NJSudarshan)
