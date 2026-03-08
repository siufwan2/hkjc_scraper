# 🏇 Hong Kong Horse Racing Data Scraper


# How to run the spider

```
pwd # /Users/uname/Desktop/hkjc_scraper
cd hkjc_scraper # Become /Users/uname/Desktop/hkjc_scraper/hkjc_scraper
```


1. Run the hkjc_race_spider
```
scrapy crawl hkjc_race_spider
python -m scrapy crawl hkjc_race_spider
```

2. Run the check_horse.py
```
python check_horse.py
```
- This step is just to get the unique horse from the race DB

3. Run the hkjc_horse_spider
```
scrapy crawl hkjc_horse_spider
python -m scrapy crawl hkjc_horse_spider

```

# The Vision
Horse racing is not just a sport—it's a complex ecosystem of data. From betting strategies to performance analysis, every race tells a story through numbers. This spider was developed to unlock the power of historical racing data and make it accessible for analysis, machine learning, and insights generation.

# The Motivation
As a data enthusiast and racing fan, I recognized that:
- 📊 Valuable racing data is trapped in unstructured HTML pages
- 🔍 No centralized dataset exists for Hong Kong racing history
- 🤖 Machine learning needs quality data to predict race outcomes
- 📈 Betting strategies require historical patterns to be effective


# The Challenge
Hong Kong horse racing generates massive amounts of data daily, but:
- Data is scattered across multiple web pages
- Historical records are hard to aggregate manually
- No API exists for programmatic access
- Time-series performance data (分段时间) is particularly valuable but difficult to collect
- Researchers and analysts lack structured datasets for modeling

# Manual Collection is Impossible
- One race meeting = 8-10 races
- Each race = 10-14 horses
- Each horse = 6分段时间 records
- Collecting 5 years of data manually = thousands of hours of work

# 💡 Project Goals
## Primary Objectives
- Automate Data Collection: Build a reliable spider that scrapes race results systematically
- Structure Unstructured Data: Convert HTML tables into clean, analysis-ready DataFrames
- Preserve Historical Context: Maintain race conditions, track details, and horse performance metrics
- Enable Advanced Analytics: Create datasets suitable for:
    - Predictive modeling
    - Performance trend analysis
    - Betting pattern recognition
    - Jockey/trainer effectiveness studies

## Secondary Goals
- Build a scalable architecture that can handle incremental updates
- Create a flexible system that adapts to website changes
- Document the process for other racing data enthusiasts
- Share knowledge about web scraping best practices


# Important Disclaimer
This project is developed strictly for educational and research purposes only.
All rights reserved. The data collected through this scraper:
- Is owned by the respective copyright holders (Hong Kong Jockey Club)
- Should not be used for commercial purposes
- Should not be redistributed or republished
- Should respect the website's terms of service and robots.txt


