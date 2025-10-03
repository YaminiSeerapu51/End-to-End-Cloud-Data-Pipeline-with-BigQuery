"""
Google Play Store Reviews Analysis - Statistical Analysis Script
Phase 3: Hypothesis testing and statistical analysis

This script performs statistical analysis on the cleaned data in BigQuery
to test if review sentiment significantly impacts star ratings.

Author: Data Analyst
Date: 2024
"""

import pandas as pd
import numpy as np
from google.cloud import bigquery
import scipy.stats as stats
import matplotlib.pyplot as plt
import seaborn as sns
from scipy.stats import ttest_ind, pearsonr, spearmanr
import warnings
warnings.filterwarnings('ignore')

class PlayStoreStatisticalAnalysis:
    """
    Class to perform statistical analysis on Google Play Store reviews
    """
    
    def __init__(self, project_id, dataset_name):
        """
        Initialize the statistical analysis
        
        Args:
            project_id (str): Google Cloud Project ID
            dataset_name (str): BigQuery dataset name
        """
        self.project_id = project_id
        self.dataset_name = dataset_name
        self.client = bigquery.Client(project=project_id)
        
        # Statistical results storage
        self.results = {}
        self.data = {}
        
        print(f"🔬 Statistical Analysis initialized for project: {project_id}")
    
    def load_data_for_analysis(self):
        """
        Load data from BigQuery for statistical analysis
        """
        print("📊 Loading data from BigQuery...")
        
        # Query 1: T-test data (sentiment vs rating)
        ttest_query = f"""
        SELECT sentiment, rating, review_length, word_count
        FROM `{self.project_id}.{self.dataset_name}.ttest_data`
        """
        
        self.data['ttest'] = self.client.query(ttest_query).to_dataframe()
        print(f"✅ Loaded {len(self.data['ttest'])} records for t-test analysis")
        
        # Query 2: Correlation data (length vs rating)
        correlation_query = f"""
        SELECT review_length, rating, sentiment, word_count, length_category
        FROM `{self.project_id}.{self.dataset_name}.length_rating_correlation`
        """
        
        self.data['correlation'] = self.client.query(correlation_query).to_dataframe()
        print(f"✅ Loaded {len(self.data['correlation'])} records for correlation analysis")
        
        # Query 3: Descriptive statistics
        desc_stats_query = f"""
        SELECT * FROM `{self.project_id}.{self.dataset_name}.sentiment_descriptive_stats`
        """
        
        self.data['descriptive'] = self.client.query(desc_stats_query).to_dataframe()
        print(f"✅ Loaded descriptive statistics")
        
        # Query 4: Executive summary
        summary_query = f"""
        SELECT * FROM `{self.project_id}.{self.dataset_name}.executive_summary`
        """
        
        self.data['summary'] = self.client.query(summary_query).to_dataframe()
        print(f"✅ Loaded executive summary data")
    
    def perform_hypothesis_test(self):
        """
        Perform the main hypothesis test: Do positive reviews have higher ratings?
        
        H0: There is no significant difference between positive and negative review ratings
        H1: Positive reviews have significantly higher ratings than negative reviews
        """
        print("\n🧪 HYPOTHESIS TESTING")
        print("="*50)
        
        # Extract positive and negative ratings
        positive_ratings = self.data['ttest'][self.data['ttest']['sentiment'] == 'Positive']['rating']
        negative_ratings = self.data['ttest'][self.data['ttest']['sentiment'] == 'Negative']['rating']
        
        print(f"Sample sizes:")
        print(f"  Positive reviews: {len(positive_ratings)}")
        print(f"  Negative reviews: {len(negative_ratings)}")
        
        # Check sample size adequacy
        if len(positive_ratings) < 30 or len(negative_ratings) < 30:
            print("⚠️  WARNING: Small sample size. Results may not be reliable.")
        
        # Descriptive statistics
        print(f"\nDescriptive Statistics:")
        print(f"  Positive reviews - Mean: {positive_ratings.mean():.3f}, Std: {positive_ratings.std():.3f}")
        print(f"  Negative reviews - Mean: {negative_ratings.mean():.3f}, Std: {negative_ratings.std():.3f}")
        
        # Perform independent t-test
        t_statistic, p_value = ttest_ind(positive_ratings, negative_ratings, alternative='greater')
        
        # Calculate effect size (Cohen's d)
        pooled_std = np.sqrt(((len(positive_ratings) - 1) * positive_ratings.var() + 
                             (len(negative_ratings) - 1) * negative_ratings.var()) / 
                            (len(positive_ratings) + len(negative_ratings) - 2))
        cohens_d = (positive_ratings.mean() - negative_ratings.mean()) / pooled_std
        
        # Store results
        self.results['hypothesis_test'] = {
            'positive_mean': positive_ratings.mean(),
            'negative_mean': negative_ratings.mean(),
            'mean_difference': positive_ratings.mean() - negative_ratings.mean(),
            't_statistic': t_statistic,
            'p_value': p_value,
            'cohens_d': cohens_d,
            'positive_sample_size': len(positive_ratings),
            'negative_sample_size': len(negative_ratings)
        }
        
        # Interpret results
        print(f"\n📈 STATISTICAL TEST RESULTS:")
        print(f"  T-statistic: {t_statistic:.4f}")
        print(f"  P-value: {p_value:.6f}")
        print(f"  Cohen's d (effect size): {cohens_d:.4f}")
        
        alpha = 0.05
        if p_value < alpha:
            print(f"\n✅ RESULT: REJECT NULL HYPOTHESIS (p < {alpha})")
            print(f"   Positive reviews have significantly higher ratings than negative reviews.")
            print(f"   Mean difference: {positive_ratings.mean() - negative_ratings.mean():.3f} stars")
        else:
            print(f"\n❌ RESULT: FAIL TO REJECT NULL HYPOTHESIS (p >= {alpha})")
            print(f"   No significant difference found between positive and negative review ratings.")
        
        # Effect size interpretation
        if abs(cohens_d) < 0.2:
            effect_interpretation = "negligible"
        elif abs(cohens_d) < 0.5:
            effect_interpretation = "small"
        elif abs(cohens_d) < 0.8:
            effect_interpretation = "medium"
        else:
            effect_interpretation = "large"
        
        print(f"   Effect size: {effect_interpretation}")
        
        return self.results['hypothesis_test']
    
    def analyze_length_rating_correlation(self):
        """
        Analyze correlation between review length and ratings
        """
        print("\n📏 REVIEW LENGTH vs RATING ANALYSIS")
        print("="*50)
        
        # Calculate correlations
        length_rating_pearson, pearson_p = pearsonr(
            self.data['correlation']['review_length'], 
            self.data['correlation']['rating']
        )
        
        length_rating_spearman, spearman_p = spearmanr(
            self.data['correlation']['review_length'], 
            self.data['correlation']['rating']
        )
        
        # Store results
        self.results['correlation_analysis'] = {
            'pearson_correlation': length_rating_pearson,
            'pearson_p_value': pearson_p,
            'spearman_correlation': length_rating_spearman,
            'spearman_p_value': spearman_p
        }
        
        print(f"Pearson correlation: {length_rating_pearson:.4f} (p = {pearson_p:.6f})")
        print(f"Spearman correlation: {length_rating_spearman:.4f} (p = {spearman_p:.6f})")
        
        # Interpret correlation strength
        if abs(length_rating_pearson) < 0.1:
            correlation_strength = "negligible"
        elif abs(length_rating_pearson) < 0.3:
            correlation_strength = "weak"
        elif abs(length_rating_pearson) < 0.5:
            correlation_strength = "moderate"
        elif abs(length_rating_pearson) < 0.7:
            correlation_strength = "strong"
        else:
            correlation_strength = "very strong"
        
        print(f"Correlation strength: {correlation_strength}")
        
        if pearson_p < 0.05:
            print("✅ Correlation is statistically significant")
        else:
            print("❌ Correlation is not statistically significant")
        
        # Analyze by length categories
        print(f"\n📊 Rating by Length Category:")
        length_category_stats = self.data['correlation'].groupby('length_category')['rating'].agg([
            'count', 'mean', 'std'
        ]).round(3)
        
        for category in length_category_stats.index:
            stats_row = length_category_stats.loc[category]
            print(f"  {category}: {stats_row['count']} reviews, avg rating: {stats_row['mean']:.2f}")
        
        return self.results['correlation_analysis']
    
    def perform_additional_analyses(self):
        """
        Perform additional statistical analyses
        """
        print("\n🔍 ADDITIONAL ANALYSES")
        print("="*50)
        
        # 1. ANOVA: Rating differences across sentiment groups
        positive_ratings = self.data['ttest'][self.data['ttest']['sentiment'] == 'Positive']['rating']
        negative_ratings = self.data['ttest'][self.data['ttest']['sentiment'] == 'Negative']['rating']
        
        # Add neutral if available
        if 'Neutral' in self.data['ttest']['sentiment'].values:
            neutral_ratings = self.data['ttest'][self.data['ttest']['sentiment'] == 'Neutral']['rating']
            f_statistic, anova_p = stats.f_oneway(positive_ratings, negative_ratings, neutral_ratings)
            print(f"One-way ANOVA (3 groups): F = {f_statistic:.4f}, p = {anova_p:.6f}")
        else:
            f_statistic, anova_p = stats.f_oneway(positive_ratings, negative_ratings)
            print(f"One-way ANOVA (2 groups): F = {f_statistic:.4f}, p = {anova_p:.6f}")
        
        # 2. Word count analysis
        if 'word_count' in self.data['correlation'].columns:
            word_rating_corr, word_rating_p = pearsonr(
                self.data['correlation']['word_count'], 
                self.data['correlation']['rating']
            )
            print(f"Word count vs Rating correlation: {word_rating_corr:.4f} (p = {word_rating_p:.6f})")
        
        # 3. Distribution tests (normality)
        from scipy.stats import shapiro
        
        # Test normality of positive ratings (sample if too large)
        pos_sample = positive_ratings.sample(min(5000, len(positive_ratings)))
        neg_sample = negative_ratings.sample(min(5000, len(negative_ratings)))
        
        pos_shapiro_stat, pos_shapiro_p = shapiro(pos_sample)
        neg_shapiro_stat, neg_shapiro_p = shapiro(neg_sample)
        
        print(f"\nNormality tests (Shapiro-Wilk):")
        print(f"  Positive ratings: W = {pos_shapiro_stat:.4f}, p = {pos_shapiro_p:.6f}")
        print(f"  Negative ratings: W = {neg_shapiro_stat:.4f}, p = {neg_shapiro_p:.6f}")
        
        if pos_shapiro_p < 0.05 or neg_shapiro_p < 0.05:
            print("  ⚠️  Data may not be normally distributed - consider non-parametric tests")
        
        # Store additional results
        self.results['additional_analyses'] = {
            'anova_f_statistic': f_statistic,
            'anova_p_value': anova_p,
            'word_count_correlation': word_rating_corr if 'word_count' in self.data['correlation'].columns else None,
            'positive_normality_p': pos_shapiro_p,
            'negative_normality_p': neg_shapiro_p
        }
    
    def generate_statistical_summary(self):
        """
        Generate a comprehensive statistical summary
        """
        print("\n📋 STATISTICAL ANALYSIS SUMMARY")
        print("="*60)
        
        # Main hypothesis test results
        ht_results = self.results['hypothesis_test']
        print(f"HYPOTHESIS TEST RESULTS:")
        print(f"  Research Question: Do positive reviews have higher ratings?")
        print(f"  Sample sizes: {ht_results['positive_sample_size']} positive, {ht_results['negative_sample_size']} negative")
        print(f"  Mean ratings: Positive = {ht_results['positive_mean']:.3f}, Negative = {ht_results['negative_mean']:.3f}")
        print(f"  Difference: {ht_results['mean_difference']:.3f} stars")
        print(f"  Statistical significance: p = {ht_results['p_value']:.6f}")
        print(f"  Effect size (Cohen's d): {ht_results['cohens_d']:.4f}")
        
        # Correlation results
        corr_results = self.results['correlation_analysis']
        print(f"\nCORRELATION ANALYSIS:")
        print(f"  Review length vs Rating: r = {corr_results['pearson_correlation']:.4f}")
        print(f"  Statistical significance: p = {corr_results['pearson_p_value']:.6f}")
        
        # Business interpretation
        print(f"\n💼 BUSINESS IMPLICATIONS:")
        
        if ht_results['p_value'] < 0.05:
            print(f"  ✅ SIGNIFICANT FINDING: Positive sentiment reviews have higher ratings")
            print(f"     → Recommendation: Encourage satisfied users to leave detailed reviews")
            print(f"     → Expected impact: {ht_results['mean_difference']:.2f} star improvement")
        else:
            print(f"  ❌ No significant relationship found between sentiment and ratings")
            print(f"     → Recommendation: Focus on other factors affecting ratings")
        
        if abs(corr_results['pearson_correlation']) > 0.1 and corr_results['pearson_p_value'] < 0.05:
            direction = "longer" if corr_results['pearson_correlation'] > 0 else "shorter"
            print(f"  ✅ Review length matters: {direction} reviews tend to have different ratings")
            print(f"     → Recommendation: Optimize review length prompts")
        else:
            print(f"  ❌ Review length does not significantly impact ratings")
            print(f"     → Recommendation: Focus on review quality over length")
        
        # Statistical confidence
        print(f"\n🎯 STATISTICAL CONFIDENCE:")
        if ht_results['positive_sample_size'] >= 30 and ht_results['negative_sample_size'] >= 30:
            print(f"  ✅ Adequate sample sizes for reliable results")
        else:
            print(f"  ⚠️  Small sample sizes - results should be interpreted cautiously")
        
        print(f"  Confidence level: 95% (α = 0.05)")
        print(f"  Statistical power: {'High' if min(ht_results['positive_sample_size'], ht_results['negative_sample_size']) > 100 else 'Moderate'}")
    
    def save_results_to_bigquery(self):
        """
        Save statistical results back to BigQuery for dashboard use
        """
        print(f"\n💾 Saving results to BigQuery...")
        
        # Create results summary table
        results_data = []
        
        # Hypothesis test results
        ht = self.results['hypothesis_test']
        results_data.extend([
            {
                'analysis_type': 'Hypothesis Test',
                'metric_name': 'Positive Mean Rating',
                'metric_value': ht['positive_mean'],
                'description': f"Average rating for positive sentiment reviews (n={ht['positive_sample_size']})"
            },
            {
                'analysis_type': 'Hypothesis Test',
                'metric_name': 'Negative Mean Rating',
                'metric_value': ht['negative_mean'],
                'description': f"Average rating for negative sentiment reviews (n={ht['negative_sample_size']})"
            },
            {
                'analysis_type': 'Hypothesis Test',
                'metric_name': 'P-Value',
                'metric_value': ht['p_value'],
                'description': 'Statistical significance of difference between positive and negative ratings'
            },
            {
                'analysis_type': 'Hypothesis Test',
                'metric_name': 'Effect Size (Cohens D)',
                'metric_value': ht['cohens_d'],
                'description': 'Magnitude of difference between positive and negative ratings'
            }
        ])
        
        # Correlation results
        corr = self.results['correlation_analysis']
        results_data.extend([
            {
                'analysis_type': 'Correlation Analysis',
                'metric_name': 'Length-Rating Correlation',
                'metric_value': corr['pearson_correlation'],
                'description': 'Pearson correlation between review length and rating'
            },
            {
                'analysis_type': 'Correlation Analysis',
                'metric_name': 'Correlation P-Value',
                'metric_value': corr['pearson_p_value'],
                'description': 'Statistical significance of length-rating correlation'
            }
        ])
        
        # Convert to DataFrame
        results_df = pd.DataFrame(results_data)
        results_df['analysis_timestamp'] = pd.Timestamp.now()
        
        # Upload to BigQuery
        table_id = f"{self.project_id}.{self.dataset_name}.statistical_results"
        
        job_config = bigquery.LoadJobConfig(
            write_disposition="WRITE_TRUNCATE",
            schema=[
                bigquery.SchemaField("analysis_type", "STRING"),
                bigquery.SchemaField("metric_name", "STRING"),
                bigquery.SchemaField("metric_value", "FLOAT"),
                bigquery.SchemaField("description", "STRING"),
                bigquery.SchemaField("analysis_timestamp", "TIMESTAMP"),
            ]
        )
        
        job = self.client.load_table_from_dataframe(results_df, table_id, job_config=job_config)
        job.result()  # Wait for the job to complete
        
        print(f"✅ Statistical results saved to {table_id}")
    
    def run_complete_analysis(self):
        """
        Run the complete statistical analysis pipeline
        """
        print("🚀 Starting Complete Statistical Analysis")
        print("="*60)
        
        try:
            # Step 1: Load data
            self.load_data_for_analysis()
            
            # Step 2: Perform hypothesis test
            self.perform_hypothesis_test()
            
            # Step 3: Analyze correlations
            self.analyze_length_rating_correlation()
            
            # Step 4: Additional analyses
            self.perform_additional_analyses()
            
            # Step 5: Generate summary
            self.generate_statistical_summary()
            
            # Step 6: Save results
            self.save_results_to_bigquery()
            
            print(f"\n🎉 ANALYSIS COMPLETE!")
            print(f"Results are ready for dashboard visualization.")
            
            return self.results
            
        except Exception as e:
            print(f"❌ Analysis failed: {str(e)}")
            raise


def main():
    """
    Main function to run statistical analysis
    """
    # Configuration - UPDATE THESE VALUES
    PROJECT_ID = "your-gcp-project-id"  # Replace with your GCP project ID
    DATASET_NAME = "play_store_analysis"  # BigQuery dataset name
    
    try:
        # Initialize and run analysis
        analyzer = PlayStoreStatisticalAnalysis(PROJECT_ID, DATASET_NAME)
        results = analyzer.run_complete_analysis()
        
        print("\n" + "="*60)
        print("NEXT STEPS:")
        print("="*60)
        print("1. ✅ Statistical analysis completed")
        print("2. 📊 Connect Looker Studio to BigQuery for visualization")
        print("3. 📈 Build executive dashboard with key findings")
        print("4. 📋 Prepare business recommendations based on results")
        print("5. 🎤 Practice explaining findings for interviews")
        
        # Print key findings for quick reference
        ht_results = results['hypothesis_test']
        print(f"\n🔑 KEY FINDINGS FOR INTERVIEWS:")
        print(f"   • Analyzed {ht_results['positive_sample_size'] + ht_results['negative_sample_size']} reviews")
        print(f"   • Statistical significance: p = {ht_results['p_value']:.4f}")
        print(f"   • Effect size: {ht_results['cohens_d']:.3f}")
        if ht_results['p_value'] < 0.05:
            print(f"   • Business impact: {ht_results['mean_difference']:.2f} star rating difference")
            print(f"   • Recommendation: Encourage positive detailed reviews")
        
    except Exception as e:
        print(f"❌ Statistical analysis failed: {str(e)}")
        print("Please check your BigQuery setup and data availability")


if __name__ == "__main__":
    main()
