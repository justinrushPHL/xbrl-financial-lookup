"""
XBRL Financial Statement Lookup App
===================================
A comprehensive web application for searching financial statement line items
and their corresponding XBRL tags across SEC filings.

File Structure:
├── main_app.py (this file)
├── requirements.txt
├── database/
│   ├── db_manager.py
│   └── financial_data.db
├── parsers/
│   └── edgar_parser.py
└── utils/
    ├── search_engine.py
    └── chart_generator.py
"""

import streamlit as st
import sqlite3
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import requests
import json
import re
from datetime import datetime, timedelta
import numpy as np
from typing import List, Dict, Tuple, Optional

# Configure Streamlit page
st.set_page_config(
    page_title="XBRL Financial Lookup",
    page_icon="🏦",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS for professional styling
st.markdown("""
<style>
    .main-header {
        font-size: 3rem;
        color: #1f77b4;
        text-align: center;
        margin-bottom: 2rem;
        font-weight: bold;
    }
    .search-container {
        background-color: #f8f9fa;
        padding: 2rem;
        border-radius: 15px;
        margin: 1rem 0;
        border: 2px solid #e9ecef;
    }
    .metric-card {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        color: white;
        padding: 1.5rem;
        border-radius: 12px;
        margin: 0.5rem 0;
        box-shadow: 0 4px 6px rgba(0, 0, 0, 0.1);
    }
    .result-card {
        background-color: white;
        padding: 1rem;
        border-radius: 8px;
        border-left: 4px solid #1f77b4;
        margin: 0.5rem 0;
        box-shadow: 0 2px 4px rgba(0, 0, 0, 0.1);
    }
    .tag-badge {
        background-color: #17a2b8;
        color: white;
        padding: 0.3rem 0.8rem;
        border-radius: 20px;
        font-size: 0.8rem;
        font-weight: bold;
    }
</style>
""", unsafe_allow_html=True)

class DatabaseManager:
    """Handles all database operations for financial data storage and retrieval"""
    
    def __init__(self, db_path: str = "financial_data.db"):
        self.db_path = db_path
        self.init_database()
    
    def init_database(self):
        """Initialize the database with required tables"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Financial line items table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS financial_line_items (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                company_name TEXT NOT NULL,
                ticker_symbol TEXT,
                cik TEXT,
                line_item_label TEXT NOT NULL,
                xbrl_tag TEXT NOT NULL,
                value REAL,
                filing_date DATE,
                period_end_date DATE,
                form_type TEXT,
                accession_number TEXT,
                sec_url TEXT,
                filing_year INTEGER,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # XBRL taxonomy mapping table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS xbrl_taxonomy (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                xbrl_tag TEXT UNIQUE NOT NULL,
                standard_label TEXT,
                documentation TEXT,
                data_type TEXT,
                period_type TEXT,
                balance_type TEXT,
                common_aliases TEXT
            )
        """)
        
        # Company information table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS companies (
                cik TEXT PRIMARY KEY,
                company_name TEXT NOT NULL,
                ticker_symbol TEXT,
                sic_code TEXT,
                industry TEXT,
                latest_filing_date DATE,
                total_filings INTEGER DEFAULT 0
            )
        """)
        
        conn.commit()
        conn.close()
        
        # Populate with sample data if empty
        self.populate_sample_data()
    
    def populate_sample_data(self):
        """Add sample financial data for demonstration"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Check if data already exists
        cursor.execute("SELECT COUNT(*) FROM financial_line_items")
        count = cursor.fetchone()[0]
        
        if count == 0:
            # Sample financial data
            sample_data = [
                ('Apple Inc.', 'AAPL', '0000320193', 'Net Sales', 'Revenues', 394328000000, '2023-11-02', '2023-09-30', '10-K', '0000320193-23-000106', 'https://www.sec.gov/ix?doc=/Archives/edgar/data/320193/000032019323000106/aapl-20230930.htm', 2023),
                ('Apple Inc.', 'AAPL', '0000320193', 'Cost of Sales', 'CostOfGoodsAndServicesSold', 223520000000, '2023-11-02', '2023-09-30', '10-K', '0000320193-23-000106', 'https://www.sec.gov/ix?doc=/Archives/edgar/data/320193/000032019323000106/aapl-20230930.htm', 2023),
                ('Apple Inc.', 'AAPL', '0000320193', 'Total Assets', 'Assets', 352755000000, '2023-11-02', '2023-09-30', '10-K', '0000320193-23-000106', 'https://www.sec.gov/ix?doc=/Archives/edgar/data/320193/000032019323000106/aapl-20230930.htm', 2023),
                ('Microsoft Corporation', 'MSFT', '0000789019', 'Revenue', 'Revenues', 211915000000, '2023-07-27', '2023-06-30', '10-K', '0000789019-23-000076', 'https://www.sec.gov/ix?doc=/Archives/edgar/data/789019/000078901923000076/msft-20230630.htm', 2023),
                ('Microsoft Corporation', 'MSFT', '0000789019', 'Cost of Revenue', 'CostOfRevenue', 65525000000, '2023-07-27', '2023-06-30', '10-K', '0000789019-23-000076', 'https://www.sec.gov/ix?doc=/Archives/edgar/data/789019/000078901923000076/msft-20230630.htm', 2023),
                ('Microsoft Corporation', 'MSFT', '0000789019', 'Total Assets', 'Assets', 411976000000, '2023-07-27', '2023-06-30', '10-K', '0000789019-23-000076', 'https://www.sec.gov/ix?doc=/Archives/edgar/data/789019/000078901923000076/msft-20230630.htm', 2023),
                ('Tesla Inc.', 'TSLA', '0001318605', 'Total Revenues', 'RevenueFromContractWithCustomerExcludingAssessedTax', 96773000000, '2024-01-29', '2023-12-31', '10-K', '0001318605-24-000006', 'https://www.sec.gov/ix?doc=/Archives/edgar/data/1318605/000131860524000006/tsla-20231231.htm', 2023),
                ('Tesla Inc.', 'TSLA', '0001318605', 'Cost of Revenues', 'CostOfGoodsAndServicesSold', 79113000000, '2024-01-29', '2023-12-31', '10-K', '0001318605-24-000006', 'https://www.sec.gov/ix?doc=/Archives/edgar/data/1318605/000131860524000006/tsla-20231231.htm', 2023),
                ('Tesla Inc.', 'TSLA', '0001318605', 'Total Assets', 'Assets', 106618000000, '2024-01-29', '2023-12-31', '10-K', '0001318605-24-000006', 'https://www.sec.gov/ix?doc=/Archives/edgar/data/1318605/000131860524000006/tsla-20231231.htm', 2023),
                # Add historical data for trend analysis
                ('Apple Inc.', 'AAPL', '0000320193', 'Net Sales', 'Revenues', 365817000000, '2022-10-28', '2022-09-30', '10-K', '0000320193-22-000108', 'https://www.sec.gov/ix?doc=/Archives/edgar/data/320193/000032019322000108/aapl-20220930.htm', 2022),
                ('Apple Inc.', 'AAPL', '0000320193', 'Total Assets', 'Assets', 352583000000, '2022-10-28', '2022-09-30', '10-K', '0000320193-22-000108', 'https://www.sec.gov/ix?doc=/Archives/edgar/data/320193/000032019322000108/aapl-20220930.htm', 2022),
                ('Microsoft Corporation', 'MSFT', '0000789019', 'Revenue', 'Revenues', 198270000000, '2022-07-28', '2022-06-30', '10-K', '0000789019-22-000070', 'https://www.sec.gov/ix?doc=/Archives/edgar/data/789019/000078901922000070/msft-20220630.htm', 2022),
                ('Microsoft Corporation', 'MSFT', '0000789019', 'Total Assets', 'Assets', 364840000000, '2022-07-28', '2022-06-30', '10-K', '0000789019-22-000070', 'https://www.sec.gov/ix?doc=/Archives/edgar/data/789019/000078901922000070/msft-20220630.htm', 2022),
            ]
            
            cursor.executemany("""
                INSERT INTO financial_line_items 
                (company_name, ticker_symbol, cik, line_item_label, xbrl_tag, value, filing_date, period_end_date, form_type, accession_number, sec_url, filing_year)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, sample_data)
            
            # Sample XBRL taxonomy data
            taxonomy_data = [
                ('Revenues', 'Revenue', 'Total revenue from all sources', 'monetary', 'duration', 'credit', 'Net Sales,Total Revenue,Sales'),
                ('Assets', 'Assets', 'Total assets of the entity', 'monetary', 'instant', 'debit', 'Total Assets'),
                ('CostOfGoodsAndServicesSold', 'Cost of Goods and Services Sold', 'Direct costs attributable to production', 'monetary', 'duration', 'debit', 'Cost of Sales,COGS,Cost of Revenue'),
                ('NetIncomeLoss', 'Net Income (Loss)', 'Net income or loss for the period', 'monetary', 'duration', 'credit', 'Net Income,Net Earnings,Profit'),
                ('CashAndCashEquivalentsAtCarryingValue', 'Cash and Cash Equivalents', 'Cash and short-term investments', 'monetary', 'instant', 'debit', 'Cash,Cash Equivalents')
            ]
            
            cursor.executemany("""
                INSERT OR IGNORE INTO xbrl_taxonomy
                (xbrl_tag, standard_label, documentation, data_type, period_type, balance_type, common_aliases)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, taxonomy_data)
            
            conn.commit()
        
        conn.close()
    
    def search_line_items(self, search_term: str, limit: int = 50) -> pd.DataFrame:
        """Search for financial line items based on search term"""
        conn = sqlite3.connect(self.db_path)
        
        query = """
            SELECT DISTINCT
                f.company_name,
                f.ticker_symbol,
                f.line_item_label,
                f.xbrl_tag,
                f.value,
                f.filing_date,
                f.period_end_date,
                f.form_type,
                f.sec_url,
                f.filing_year,
                t.standard_label,
                t.documentation
            FROM financial_line_items f
            LEFT JOIN xbrl_taxonomy t ON f.xbrl_tag = t.xbrl_tag
            WHERE 
                f.line_item_label LIKE ? OR 
                f.xbrl_tag LIKE ? OR
                t.standard_label LIKE ? OR
                t.common_aliases LIKE ?
            ORDER BY f.filing_year DESC, f.company_name
            LIMIT ?
        """
        
        search_pattern = f"%{search_term}%"
        df = pd.read_sql_query(query, conn, params=[search_pattern, search_pattern, search_pattern, search_pattern, limit])
        conn.close()
        
        return df
    
    def get_trend_data(self, xbrl_tag: str, companies: List[str] = None) -> pd.DataFrame:
        """Get trend data for a specific XBRL tag across companies and years"""
        conn = sqlite3.connect(self.db_path)
        
        base_query = """
            SELECT 
                company_name,
                ticker_symbol,
                filing_year,
                AVG(value) as avg_value,
                line_item_label,
                xbrl_tag
            FROM financial_line_items
            WHERE xbrl_tag = ?
        """
        
        params = [xbrl_tag]
        
        if companies:
            placeholders = ','.join(['?' for _ in companies])
            base_query += f" AND ticker_symbol IN ({placeholders})"
            params.extend(companies)
        
        base_query += " GROUP BY company_name, ticker_symbol, filing_year ORDER BY filing_year, company_name"
        
        df = pd.read_sql_query(base_query, conn, params=params)
        conn.close()
        
        return df
    
    def get_company_metrics(self) -> Dict:
        """Get summary metrics about the database"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        metrics = {}
        
        # Total companies
        cursor.execute("SELECT COUNT(DISTINCT company_name) FROM financial_line_items")
        metrics['total_companies'] = cursor.fetchone()[0]
        
        # Total line items
        cursor.execute("SELECT COUNT(*) FROM financial_line_items")
        metrics['total_line_items'] = cursor.fetchone()[0]
        
        # Unique XBRL tags
        cursor.execute("SELECT COUNT(DISTINCT xbrl_tag) FROM financial_line_items")
        metrics['unique_xbrl_tags'] = cursor.fetchone()[0]
        
        # Date range
        cursor.execute("SELECT MIN(filing_year), MAX(filing_year) FROM financial_line_items")
        min_year, max_year = cursor.fetchone()
        metrics['year_range'] = f"{min_year}-{max_year}"
        
        conn.close()
        return metrics

class SearchEngine:
    """Advanced search functionality with fuzzy matching and ranking"""
    
    def __init__(self, db_manager: DatabaseManager):
        self.db = db_manager
    
    def smart_search(self, query: str, max_results: int = 20) -> pd.DataFrame:
        """Perform intelligent search with ranking and suggestions"""
        # First try exact search
        results = self.db.search_line_items(query, max_results)
        
        if len(results) == 0:
            # Try individual words
            words = query.split()
            for word in words:
                if len(word) > 2:  # Skip very short words
                    word_results = self.db.search_line_items(word, max_results // len(words))
                    results = pd.concat([results, word_results], ignore_index=True)
        
        # Remove duplicates and rank by relevance
        if len(results) > 0:
            results = results.drop_duplicates(subset=['company_name', 'xbrl_tag', 'filing_year'])
            results = self._rank_results(results, query)
        
        return results.head(max_results)
    
    def _rank_results(self, df: pd.DataFrame, query: str) -> pd.DataFrame:
        """Rank search results by relevance"""
        if len(df) == 0:
            return df
        
        query_lower = query.lower()
        
        def calculate_score(row):
            score = 0
            label_lower = str(row['line_item_label']).lower()
            tag_lower = str(row['xbrl_tag']).lower()
            
            # Exact match gets highest score
            if query_lower == label_lower:
                score += 100
            elif query_lower in label_lower:
                score += 50
            
            # Tag match
            if query_lower in tag_lower:
                score += 30
            
            # Recent filings get bonus
            if row['filing_year'] >= 2023:
                score += 10
            elif row['filing_year'] >= 2022:
                score += 5
            
            return score
        
        df['relevance_score'] = df.apply(calculate_score, axis=1)
        return df.sort_values('relevance_score', ascending=False).drop('relevance_score', axis=1)

class ChartGenerator:
    """Generate interactive charts for financial data visualization"""
    
    @staticmethod
    def create_trend_chart(df: pd.DataFrame, metric_name: str) -> go.Figure:
        """Create a trend chart showing metric evolution over time"""
        if len(df) == 0:
            fig = go.Figure()
            fig.add_annotation(text="No data available for this metric", 
                             xref="paper", yref="paper", x=0.5, y=0.5, showarrow=False)
            return fig
        
        fig = go.Figure()
        
        # Color palette for different companies
        colors = ['#1f77b4', '#ff7f0e', '#2ca02c', '#d62728', '#9467bd', '#8c564b']
        
        for i, company in enumerate(df['company_name'].unique()):
            company_data = df[df['company_name'] == company]
            
            fig.add_trace(go.Scatter(
                x=company_data['filing_year'],
                y=company_data['avg_value'],
                mode='lines+markers',
                name=f"{company} ({company_data['ticker_symbol'].iloc[0]})",
                line=dict(color=colors[i % len(colors)], width=3),
                marker=dict(size=8),
                hovertemplate='<b>%{fullData.name}</b><br>' +
                            'Year: %{x}<br>' +
                            'Value: $%{y:,.0f}<br>' +
                            '<extra></extra>'
            ))
        
        fig.update_layout(
            title=f"{metric_name} Trend Analysis",
            xaxis_title="Filing Year",
            yaxis_title="Value (USD)",
            hovermode='closest',
            template='plotly_white',
            height=500,
            legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1)
        )
        
        # Format y-axis for large numbers
        fig.update_yaxes(tickformat='$,.0s')
        
        return fig
    
    @staticmethod
    def create_comparison_chart(df: pd.DataFrame, metric_name: str) -> go.Figure:
        """Create a comparison chart for the latest year"""
        if len(df) == 0:
            fig = go.Figure()
            fig.add_annotation(text="No data available", xref="paper", yref="paper", x=0.5, y=0.5, showarrow=False)
            return fig
        
        # Get latest year data
        latest_year = df['filing_year'].max()
        latest_data = df[df['filing_year'] == latest_year]
        
        fig = go.Figure(data=[
            go.Bar(
                x=[f"{row['company_name']}<br>({row['ticker_symbol']})" for _, row in latest_data.iterrows()],
                y=latest_data['avg_value'],
                marker_color=['#1f77b4', '#ff7f0e', '#2ca02c', '#d62728', '#9467bd'][:len(latest_data)],
                hovertemplate='<b>%{x}</b><br>Value: $%{y:,.0f}<extra></extra>'
            )
        ])
        
        fig.update_layout(
            title=f"{metric_name} Comparison ({latest_year})",
            xaxis_title="Company",
            yaxis_title="Value (USD)",
            template='plotly_white',
            height=400
        )
        
        fig.update_yaxes(tickformat='$,.0s')
        
        return fig

def main():
    """Main Streamlit application"""
    
    # Initialize components
    if 'db_manager' not in st.session_state:
        st.session_state.db_manager = DatabaseManager()
        st.session_state.search_engine = SearchEngine(st.session_state.db_manager)
        st.session_state.chart_generator = ChartGenerator()
    
    # App header
    st.markdown('<h1 class="main-header">🏦 XBRL Financial Statement Lookup</h1>', unsafe_allow_html=True)
    
    # Sidebar with metrics
    with st.sidebar:
        st.markdown("### 📊 Database Metrics")
        metrics = st.session_state.db_manager.get_company_metrics()
        
        col1, col2 = st.columns(2)
        with col1:
            st.metric("Companies", metrics['total_companies'])
            st.metric("XBRL Tags", metrics['unique_xbrl_tags'])
        with col2:
            st.metric("Line Items", metrics['total_line_items'])
            st.metric("Years", metrics['year_range'])
        
        st.markdown("---")
        st.markdown("### 🔍 Quick Searches")
        quick_searches = ["Revenue", "Assets", "Net Income", "Cash", "Cost of Sales"]
        for search_term in quick_searches:
            if st.button(search_term, key=f"quick_{search_term}"):
                st.session_state.search_query = search_term
    
    # Main tabs
    tab1, tab2, tab3 = st.tabs(["🔍 Search & Results", "📈 Trend Analysis", "🗃️ Data Explorer"])
    
    with tab1:
        st.markdown("### Search Financial Line Items")
        
        # Search interface
        col1, col2 = st.columns([3, 1])
        with col1:
            search_query = st.text_input(
                "Enter a financial line item or XBRL tag:",
                placeholder="e.g., 'Net Sales', 'Total Revenue', 'Assets'",
                value=st.session_state.get('search_query', ''),
                key="main_search"
            )
        with col2:
            search_button = st.button("🔍 Search", type="primary", use_container_width=True)
        
        # Perform search
        if search_button and search_query:
            with st.spinner("Searching financial data..."):
                results = st.session_state.search_engine.smart_search(search_query)
                
                if len(results) > 0:
                    st.success(f"Found {len(results)} matching line items!")
                    
                    # Display results
                    st.markdown("### 📋 Search Results")
                    
                    for idx, row in results.iterrows():
                        with st.container():
                            col1, col2, col3 = st.columns([2, 1, 1])
                            
                            with col1:
                                st.markdown(f"""
                                <div class="result-card">
                                    <h4 style="margin-top: 0; color: #1f77b4;">{row['company_name']} ({row['ticker_symbol']})</h4>
                                    <p><strong>Line Item:</strong> {row['line_item_label']}</p>
                                    <p><strong>Value:</strong> ${row['value']:,.0f} ({row['filing_year']})</p>
                                </div>
                                """, unsafe_allow_html=True)
                            
                            with col2:
                                st.markdown(f'<span class="tag-badge">{row["xbrl_tag"]}</span>', unsafe_allow_html=True)
                                st.caption(f"Filed: {row['filing_date']}")
                            
                            with col3:
                                if pd.notna(row['sec_url']):
                                    st.link_button("📄 View Filing", row['sec_url'])
                                st.caption(f"Form: {row['form_type']}")
                else:
                    st.warning("No matching line items found. Try different search terms.")
    
    with tab2:
        st.markdown("### 📈 Financial Trend Analysis")
        
        # Get available XBRL tags for trends
        conn = sqlite3.connect(st.session_state.db_manager.db_path)
        available_tags = pd.read_sql_query(
            "SELECT DISTINCT xbrl_tag, line_item_label FROM financial_line_items ORDER BY xbrl_tag", 
            conn
        )
        conn.close()
        
        col1, col2 = st.columns(2)
        with col1:
            selected_tag = st.selectbox(
                "Select XBRL Tag for Trend Analysis:",
                options=available_tags['xbrl_tag'].tolist(),
                format_func=lambda x: f"{x} ({available_tags[available_tags['xbrl_tag']==x]['line_item_label'].iloc[0]})"
            )
        
        with col2:
            available_companies = pd.read_sql_query(
                "SELECT DISTINCT ticker_symbol, company_name FROM financial_line_items ORDER BY company_name",
                sqlite3.connect(st.session_state.db_manager.db_path)
            )
            selected_companies = st.multiselect(
                "Select Companies (leave empty for all):",
                options=available_companies['ticker_symbol'].tolist(),
                format_func=lambda x: f"{x} ({available_companies[available_companies['ticker_symbol']==x]['company_name'].iloc[0]})"
            )
        
        if st.button("Generate Trend Chart", type="primary"):
            trend_data = st.session_state.db_manager.get_trend_data(
                selected_tag, 
                selected_companies if selected_companies else None
            )
            
            if len(trend_data) > 0:
                # Create trend chart
                metric_name = trend_data['line_item_label'].iloc[0]
                trend_fig = st.session_state.chart_generator.create_trend_chart(trend_data, metric_name)
                st.plotly_chart(trend_fig, use_container_width=True)
                
                # Create comparison chart
                comparison_fig = st.session_state.chart_generator.create_comparison_chart(trend_data, metric_name)
                st.plotly_chart(comparison_fig, use_container_width=True)
                
                # Show data table
                with st.expander("📊 View Raw Data"):
                    st.dataframe(trend_data, use_container_width=True)
            else:
                st.warning("No trend data available for the selected criteria.")
    
    with tab3:
        st.markdown("### 🗃️ Data Explorer")
        st.markdown("Execute custom SQL queries on the financial database.")
        
        # Predefined query examples
        st.markdown("#### Quick Query Examples:")
        col1, col2, col3 = st.columns(3)
        
        with col1:
            if st.button("Top Revenue Companies"):
                st.session_state.sql_query = """
                SELECT company_name, ticker_symbol, MAX(value) as revenue, filing_year
                FROM financial_line_items 
                WHERE xbrl_tag = 'Revenues' 
                GROUP BY company_name 
                ORDER BY revenue DESC
                """
        
        with col2:
            if st.button("Asset Growth Analysis"):
                st.session_state.sql_query = """
                SELECT company_name, filing_year, 
                       AVG(value) as avg_assets,
                       LAG(AVG(value)) OVER (PARTITION BY company_name ORDER BY filing_year) as prev_assets
                FROM financial_line_items 
                WHERE xbrl_tag = 'Assets'
                GROUP BY company_name, filing_year
                ORDER BY company_name, filing_year
                """
        
        with col3:
            if st.button("XBRL Tag Summary"):
                st.session_state.sql_query = """
                SELECT xbrl_tag, COUNT(*) as usage_count, 
                       COUNT(DISTINCT company_name) as companies_using,
                       AVG(value) as avg_value
                FROM financial_line_items 
                GROUP BY xbrl_tag 
                ORDER BY usage_count DESC
                """
        
        # Custom SQL query interface
        sql_query = st.text_area(
            "Custom SQL Query:",
            value=st.session_state.get('sql_query', ''),
            height=150,
            placeholder="SELECT * FROM financial_line_items LIMIT 10;"
        )
        
        if st.button("Execute Query", type="primary"):
            try:
                conn = sqlite3.connect(st.session_state.db_manager.db_path)
                result_df = pd.read_sql_query(sql_query, conn)
                conn.close()
                
                st.success(f"Query executed successfully! Returned {len(result_df)} rows.")
                st.dataframe(result_df, use_container_width=True)
                
                # Download option
                csv = result_df.to_csv(index=False)
                st.download_button(
                    label="📥 Download Results as CSV",
                    data=csv,
                    file_name=f"query_results_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                    mime="text/csv"
                )
                
            except Exception as e:
                st.error(f"Query execution error: {str(e)}")
        
        # Database schema information
        with st.expander("📋 Database Schema"):
            st.markdown("""
            **Available Tables:**
            
            1. **financial_line_items**
               - `company_name`, `ticker_symbol`, `cik`
               - `line_item_label`, `xbrl_tag`, `value`
               - `filing_date`, `period_end_date`, `filing_year`
               - `form_type`, `accession_number`, `sec_url`
            
            2. **xbrl_taxonomy**
               - `xbrl_tag`, `standard_label`, `documentation`
               - `data_type`, `period_type`, `balance_type`, `common_aliases`
            
            3. **companies**
               - `cik`, `company_name`, `ticker_symbol`
               - `sic_code`, `industry`, `latest_filing_date`
            """)

if __name__ == "__main__":
    main()