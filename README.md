# End-to-End-Cloud-Data-Pipeline-with-BigQuery

# Google Play Store Reviews Analysis
# End-to-End Customer Feedback Impact on Product Ratings

# Business Problem
**Objective**: Analyze Google Play Store reviews to determine if review sentiment and length have a statistically significant impact on star ratings. This helps the product team decide whether to focus on encouraging longer, more detailed reviews.

# Key Business Questions
1. Do positive sentiment reviews correlate with higher star ratings?
2. Does review length impact the rating users provide?
3. Should we encourage users to write longer, more detailed reviews?

# Tech Stack
- **Cloud Data Warehouse**: Google BigQuery
- **Data Storage**: Google Cloud Storage (GCS)
- **Programming**: Python (Pandas, scipy.stats)
- **Visualization**: Looker Studio
- **Statistical Analysis**: Hypothesis Testing (T-test)

# Expected Business Impact
- **Product Strategy**: Data-driven decisions on review encouragement
- **User Experience**: Optimize review collection process
- **App Store Optimization**: Improve overall app ratings
- **Resource Allocation**: Focus efforts on high-impact review strategies

# Project Architecture

```
Raw Data (Kaggle) → Python Script → GCS Bucket → BigQuery Raw Table
                                                        ↓
                                              SQL Transformations
                                                        ↓
                                              BigQuery Clean Table
                                                        ↓
                                    Python Analysis ← → Looker Studio Dashboard
                                                        ↓
                                              Statistical Insights & Recommendations

# Implementation Phases

# Phase 1: Data Acquisition & Ingestion
- Download Google Play Store reviews dataset from Kaggle
- Set up Google Cloud environment (GCS + BigQuery)
- Python script for data cleaning and upload
- Load data into BigQuery raw table

# Phase 2: Data Modeling & Transformation
- Advanced SQL transformations in BigQuery
- Feature engineering (review length, sentiment categories)
- Data quality checks and validation
- Create analysis-ready table

# Phase 3: Statistical Analysis
- Hypothesis formulation and testing
- Python-based statistical analysis
- T-test for sentiment impact on ratings
- Correlation analysis for review length

# Phase 4: Visualization & Insights
- Looker Studio dashboard creation
- Executive summary with key findings
- Actionable business recommendations
- Statistical significance reporting

---

# Key Metrics & KPIs

# Primary Metrics
- **Average Rating by Sentiment**: Positive vs Negative reviews
- **Review Length Impact**: Correlation with star ratings
- **Statistical Significance**: P-value from hypothesis testing

# Secondary Metrics
- **Total Reviews Analyzed**: Volume of data processed
- **Sentiment Distribution**: Breakdown of positive/negative/neutral
- **Rating Distribution**: 1-5 star rating patterns

---

# Expected Outcomes

# Statistical Findings
- **Hypothesis Test Result**: P-value < 0.05 (statistically significant)
- **Effect Size**: Quantified difference between sentiment groups
- **Confidence Intervals**: Range of expected differences

# Business Recommendations
1. **Encourage Positive Reviewers**: Prompt satisfied users to leave detailed reviews
2. **Review Length Strategy**: Optimal review length for maximum impact
3. **Sentiment Monitoring**: Track sentiment trends over time
4. **Product Improvements**: Address common negative feedback themes

# Skills Demonstrated

# Technical Skills
- **Cloud Data Warehousing**: BigQuery setup and management
- **SQL Proficiency**: Complex queries, window functions, CTEs
- **Python Programming**: Data manipulation, statistical analysis
- **Data Visualization**: Dashboard creation and storytelling

# Analytical Skills
- **Hypothesis Testing**: Formal statistical methodology
- **Business Analysis**: Translating data into actionable insights
- **Data Quality**: Cleaning and validation processes
- **Statistical Interpretation**: P-values, confidence intervals

# Business Skills
- **Problem Framing**: Clear business question definition
- **Stakeholder Communication**: Executive-level reporting
- **Recommendation Development**: Actionable next steps
- **Impact Measurement**: Quantified business value

## 📁 Project Structure
---
```
google_play_reviews_analysis/
├── README.md
├── data/
│   └── raw_reviews.csv
├── scripts/
│   ├── 01_data_ingestion.py
│   ├── 02_statistical_analysis.py
│   └── requirements.txt
├── sql/
│   ├── 01_create_raw_table.sql
│   ├── 02_transform_data.sql
│   └── 03_analysis_queries.sql
├── dashboards/
│   └── looker_studio_specs.md
└── results/
    ├── statistical_results.md
    └── business_recommendations.md
```
---
