"""
database/db_manager.py
Enhanced database manager with real SEC data support
"""

import sqlite3
import pandas as pd
import logging
from typing import List, Dict, Optional, Tuple
from datetime import datetime
import os

# Import our custom types
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from parsers.edgar_parser import CompanyFact

logger = logging.getLogger(__name__)

class ProductionDatabaseManager:
    """Enhanced database manager for production SEC data"""
    
    def __init__(self, db_path: str = "production_financial_data.db"):
        self.db_path = db_path
        self.init_production_schema()
    
    def init_production_schema(self):
        """Initialize production database with proper indexing and constraints"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        try:
            # Companies table with comprehensive info
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS companies (
                    cik TEXT PRIMARY KEY,
                    name TEXT NOT NULL,
                    ticker TEXT UNIQUE,
                    sic_code TEXT,
                    industry TEXT,
                    market_cap BIGINT,
                    fiscal_year_end TEXT,
                    state_of_incorporation TEXT,
                    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    data_source TEXT DEFAULT 'SEC_EDGAR'
                )
            """)
            
            # Financial facts table (replaces old financial_line_items)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS financial_facts (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    cik TEXT NOT NULL,
                    tag TEXT NOT NULL,
                    label TEXT NOT NULL,
                    description TEXT,
                    value REAL,
                    unit TEXT,
                    end_date DATE,
                    start_date DATE,
                    form_type TEXT NOT NULL,
                    filed_date DATE,
                    fiscal_year INTEGER,
                    fiscal_period TEXT,
                    accession_number TEXT,
                    sec_url TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (cik) REFERENCES companies (cik),
                    UNIQUE(cik, tag, end_date, form_type, accession_number)
                )
            """)
            
            # Filing metadata table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS filings (
                    accession_number TEXT PRIMARY KEY,
                    cik TEXT NOT NULL,
                    form_type TEXT NOT NULL,
                    filing_date DATE NOT NULL,
                    period_end_date DATE,
                    fiscal_year INTEGER,
                    fiscal_period TEXT,
                    document_url TEXT,
                    processed_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    fact_count INTEGER DEFAULT 0,
                    FOREIGN KEY (cik) REFERENCES companies (cik)
                )
            """)
            
            # Data quality tracking
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS data_quality_log (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    cik TEXT,
                    check_type TEXT,
                    status TEXT,
                    message TEXT,
                    checked_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # Create performance indexes
            self._create_indexes(cursor)
            
            # Create analytical views
            self._create_views(cursor)
            
            conn.commit()
            logger.info("Production database schema initialized successfully")
            
        except Exception as e:
            conn.rollback()
            logger.error(f"Failed to initialize database schema: {e}")
            raise
        finally:
            conn.close()
    
    def _create_indexes(self, cursor):
        """Create database indexes for optimal query performance"""
        indexes = [
            "CREATE INDEX IF NOT EXISTS idx_financial_facts_cik_tag ON financial_facts(cik, tag)",
            "CREATE INDEX IF NOT EXISTS idx_financial_facts_end_date ON financial_facts(end_date DESC)",
            "CREATE INDEX IF NOT EXISTS idx_financial_facts_form_type ON financial_facts(form_type)",
            "CREATE INDEX IF NOT EXISTS idx_financial_facts_fiscal_year ON financial_facts(fiscal_year DESC)",
            "CREATE INDEX IF NOT EXISTS idx_financial_facts_tag ON financial_facts(tag)",
            "CREATE INDEX IF NOT EXISTS idx_companies_ticker ON companies(ticker)",
            "CREATE INDEX IF NOT EXISTS idx_filings_cik_date ON filings(cik, filing_date DESC)",
            "CREATE INDEX IF NOT EXISTS idx_filings_form_type ON filings(form_type)",
        ]
        
        for index_sql in indexes:
            try:
                cursor.execute(index_sql)
            except Exception as e:
                logger.warning(f"Index creation warning: {e}")
    
    def _create_views(self, cursor):
        """Create analytical views for common queries"""
        
        # Latest annual metrics view
        cursor.execute("""
            CREATE VIEW IF NOT EXISTS latest_annual_metrics AS
            SELECT DISTINCT
                f.cik,
                c.name as company_name,
                c.ticker,
                f.tag,
                f.label,
                f.value,
                f.unit,
                f.end_date,
                f.fiscal_year,
                f.sec_url
            FROM financial_facts f
            JOIN companies c ON f.cik = c.cik
            WHERE f.form_type = '10-K'
            AND f.end_date = (
                SELECT MAX(end_date) 
                FROM financial_facts f2 
                WHERE f2.cik = f.cik 
                AND f2.tag = f.tag 
                AND f2.form_type = '10-K'
            )
        """)
        
        # Quarterly trends view
        cursor.execute("""
            CREATE VIEW IF NOT EXISTS quarterly_trends AS
            SELECT 
                f.cik,
                c.name as company_name,
                c.ticker,
                f.tag,
                f.label,
                f.value,
                f.unit,
                f.end_date,
                f.fiscal_year,
                f.fiscal_period,
                f.sec_url,
                ROW_NUMBER() OVER (
                    PARTITION BY f.cik, f.tag 
                    ORDER BY f.end_date DESC
                ) as period_rank
            FROM financial_facts f
            JOIN companies c ON f.cik = c.cik
            WHERE f.form_type = '10-Q'
        """)
        
        # Company overview with key metrics
        cursor.execute("""
            CREATE VIEW IF NOT EXISTS company_overview AS
            SELECT 
                c.cik,
                c.name,
                c.ticker,
                c.industry,
                COUNT(DISTINCT f.accession_number) as total_filings,
                MAX(f.filed_date) as latest_filing_date,
                COUNT(DISTINCT f.tag) as unique_metrics,
                MAX(f.fiscal_year) as latest_fiscal_year
            FROM companies c
            LEFT JOIN financial_facts f ON c.cik = f.cik
            GROUP BY c.cik, c.name, c.ticker, c.industry
        """)
    
    def store_company_facts(self, facts: List[CompanyFact], update_company_info: bool = True):
        """
        Store financial facts in the database with conflict resolution
        
        Args:
            facts: List of CompanyFact objects to store
            update_company_info: Whether to update company metadata
        """
        if not facts:
            logger.warning("No facts provided to store")
            return
        
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        try:
            # Group facts by company
            companies = {}
            for fact in facts:
                if fact.cik not in companies:
                    companies[fact.cik] = {
                        'name': fact.company_name,
                        'facts': []
                    }
                companies[fact.cik]['facts'].append(fact)
            
            # Process each company
            for cik, company_data in companies.items():
                company_name = company_data['name']
                company_facts = company_data['facts']
                
                # Update/insert company info
                if update_company_info:
                    cursor.execute("""
                        INSERT OR REPLACE INTO companies (cik, name, last_updated)
                        VALUES (?, ?, CURRENT_TIMESTAMP)
                    """, (cik, company_name))
                
                # Insert facts (with conflict resolution)
                for fact in company_facts:
                    # Extract fiscal year from end date
                    fiscal_year = None
                    if fact.end_date:
                        try:
                            fiscal_year = int(fact.end_date[:4])
                        except (ValueError, IndexError):
                            pass
                    
                    cursor.execute("""
                        INSERT OR REPLACE INTO financial_facts (
                            cik, tag, label, description, value, unit, 
                            end_date, form_type, filed_date, fiscal_year,
                            accession_number, sec_url, updated_at
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                    """, (
                        fact.cik, fact.tag, fact.label, fact.description,
                        fact.value, fact.unit, fact.end_date, fact.form_type,
                        fact.filed_date, fiscal_year, fact.accession_number, fact.sec_url
                    ))
                
                logger.info(f"Stored {len(company_facts)} facts for {company_name}")
            
            conn.commit()
            logger.info(f"Successfully stored facts for {len(companies)} companies")
            
        except Exception as e:
            conn.rollback()
            logger.error(f"Database storage error: {e}")
            raise
        finally:
            conn.close()
    
    def smart_search(self, query: str, limit: int = 50) -> pd.DataFrame:
        """
        Enhanced search with ranking and relevance scoring
        
        Args:
            query: Search term for financial metrics
            limit: Maximum number of results
            
        Returns:
            DataFrame with search results ranked by relevance
        """
        conn = sqlite3.connect(self.db_path)
        
        # Enhanced search with scoring
        search_query = """
            SELECT DISTINCT
                f.cik,
                f.tag,
                c.name as company_name,
                c.ticker,
                f.label,
                f.value,
                f.unit,
                f.end_date,
                f.form_type,
                f.fiscal_year,
                f.sec_url,
                f.description,
                -- Relevance scoring
                CASE 
                    WHEN LOWER(f.label) = LOWER(?) THEN 100
                    WHEN LOWER(f.label) LIKE LOWER(?) THEN 80
                    WHEN LOWER(f.tag) LIKE LOWER(?) THEN 60
                    WHEN LOWER(f.description) LIKE LOWER(?) THEN 40
                    ELSE 20
                END as relevance_score
            FROM financial_facts f
            JOIN companies c ON f.cik = c.cik
            WHERE (
                LOWER(f.label) LIKE LOWER(?) OR
                LOWER(f.tag) LIKE LOWER(?) OR
                LOWER(f.description) LIKE LOWER(?) OR
                LOWER(c.name) LIKE LOWER(?)
            )
            ORDER BY relevance_score DESC, f.fiscal_year DESC, c.name
            LIMIT ?
        """
        
        search_pattern = f"%{query}%"
        params = [
            query,  # Exact match score
            search_pattern, search_pattern, search_pattern,  # Partial match scores
            search_pattern, search_pattern, search_pattern, search_pattern,  # WHERE clause
            limit
        ]
        
        df = pd.read_sql_query(search_query, conn, params=params)
        conn.close()
        
        return df
    
    def get_trend_data(self, tag: str, companies: List[str] = None, 
                      form_types: List[str] = ['10-K'], periods: int = 10) -> pd.DataFrame:
        """
        Get trend data for financial metrics with enhanced filtering
        
        Args:
            tag: XBRL tag to analyze
            companies: List of tickers to include (None for all)
            form_types: Types of filings to include ('10-K', '10-Q')
            periods: Number of periods to return
            
        Returns:
            DataFrame with trend data
        """
        conn = sqlite3.connect(self.db_path)
        
        base_query = """
            SELECT 
                f.cik,
                c.name as company_name,
                c.ticker,
                f.tag,
                f.label,
                f.value,
                f.unit,
                f.end_date,
                f.fiscal_year,
                f.fiscal_period,
                f.form_type,
                f.sec_url
            FROM financial_facts f
            JOIN companies c ON f.cik = c.cik
            WHERE f.tag = ?
        """
        
        params = [tag]
        
        # Add form type filter
        if form_types:
            placeholders = ','.join(['?' for _ in form_types])
            base_query += f" AND f.form_type IN ({placeholders})"
            params.extend(form_types)
        
        # Add company filter
        if companies:
            placeholders = ','.join(['?' for _ in companies])
            base_query += f" AND c.ticker IN ({placeholders})"
            params.extend(companies)
        
        base_query += " ORDER BY f.end_date DESC, c.name"
        
        if periods:
            base_query += " LIMIT ?"
            params.append(periods * len(companies) if companies else periods * 10)
        
        df = pd.read_sql_query(base_query, conn, params=params)
        conn.close()
        
        return df
    
    def get_company_metrics_summary(self) -> Dict:
        """Get comprehensive database metrics and statistics"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        metrics = {}
        
        # Basic counts
        cursor.execute("SELECT COUNT(*) FROM companies")
        metrics['total_companies'] = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM financial_facts")
        metrics['total_facts'] = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(DISTINCT tag) FROM financial_facts")
        metrics['unique_tags'] = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(DISTINCT accession_number) FROM financial_facts")
        metrics['total_filings'] = cursor.fetchone()[0]
        
        # Date ranges
        cursor.execute("SELECT MIN(fiscal_year), MAX(fiscal_year) FROM financial_facts WHERE fiscal_year IS NOT NULL")
        min_year, max_year = cursor.fetchone()
        metrics['year_range'] = f"{min_year}-{max_year}" if min_year and max_year else "N/A"
        
        # Form type breakdown
        cursor.execute("""
            SELECT form_type, COUNT(*) as count 
            FROM financial_facts 
            GROUP BY form_type 
            ORDER BY count DESC
        """)
        metrics['form_types'] = dict(cursor.fetchall())
        
        # Most recent update
        cursor.execute("SELECT MAX(updated_at) FROM financial_facts")
        metrics['last_updated'] = cursor.fetchone()[0]
        
        # Top companies by fact count
        cursor.execute("""
            SELECT c.name, c.ticker, COUNT(*) as fact_count
            FROM financial_facts f
            JOIN companies c ON f.cik = c.cik
            GROUP BY f.cik, c.name, c.ticker
            ORDER BY fact_count DESC
            LIMIT 5
        """)
        metrics['top_companies'] = cursor.fetchall()
        
        conn.close()
        return metrics
    
    def get_available_companies(self) -> pd.DataFrame:
        """Get list of all companies in the database"""
        conn = sqlite3.connect(self.db_path)
        df = pd.read_sql_query("SELECT * FROM company_overview ORDER BY name", conn)
        conn.close()
        return df
    
    def execute_custom_query(self, query: str, params: List = None) -> pd.DataFrame:
        """
        Execute custom SQL query safely
        
        Args:
            query: SQL query string
            params: Query parameters for safe parameterized queries
            
        Returns:
            DataFrame with query results
        """
        conn = sqlite3.connect(self.db_path)
        try:
            if params:
                df = pd.read_sql_query(query, conn, params=params)
            else:
                df = pd.read_sql_query(query, conn)
            return df
        except Exception as e:
            logger.error(f"Custom query failed: {e}")
            raise
        finally:
            conn.close()
    
    def get_financial_ratios(self, cik: str, fiscal_year: int = None) -> Dict:
        """
        Calculate common financial ratios for a company
        
        Args:
            cik: Company CIK
            fiscal_year: Specific fiscal year (None for latest)
            
        Returns:
            Dictionary of calculated financial ratios
        """
        conn = sqlite3.connect(self.db_path)
        
        # Get latest year if not specified
        if not fiscal_year:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT MAX(fiscal_year) 
                FROM financial_facts 
                WHERE cik = ? AND form_type = '10-K'
            """, (cik,))
            fiscal_year = cursor.fetchone()[0]
            
            if not fiscal_year:
                conn.close()
                return {}
        
        # Get key financial metrics for ratio calculations
        query = """
            SELECT tag, value
            FROM financial_facts
            WHERE cik = ? AND fiscal_year = ? AND form_type = '10-K'
            AND tag IN (
                'Revenues', 'NetIncomeLoss', 'Assets', 'StockholdersEquity',
                'Liabilities', 'CostOfGoodsAndServicesSold', 'OperatingIncomeLoss',
                'CashAndCashEquivalentsAtCarryingValue', 'LiabilitiesCurrent'
            )
        """
        
        df = pd.read_sql_query(query, conn, params=[cik, fiscal_year])
        conn.close()
        
        # Convert to dictionary for easier access
        metrics = dict(zip(df['tag'], df['value']))
        
        ratios = {}
        
        try:
            # Profitability ratios
            if 'NetIncomeLoss' in metrics and 'Revenues' in metrics:
                ratios['net_profit_margin'] = (metrics['NetIncomeLoss'] / metrics['Revenues']) * 100
            
            if 'NetIncomeLoss' in metrics and 'Assets' in metrics:
                ratios['return_on_assets'] = (metrics['NetIncomeLoss'] / metrics['Assets']) * 100
            
            if 'NetIncomeLoss' in metrics and 'StockholdersEquity' in metrics:
                ratios['return_on_equity'] = (metrics['NetIncomeLoss'] / metrics['StockholdersEquity']) * 100
            
            # Liquidity ratios
            if 'CashAndCashEquivalentsAtCarryingValue' in metrics and 'LiabilitiesCurrent' in metrics:
                ratios['cash_ratio'] = metrics['CashAndCashEquivalentsAtCarryingValue'] / metrics['LiabilitiesCurrent']
            
            # Leverage ratios
            if 'Liabilities' in metrics and 'Assets' in metrics:
                ratios['debt_to_assets'] = (metrics['Liabilities'] / metrics['Assets']) * 100
            
            if 'Liabilities' in metrics and 'StockholdersEquity' in metrics:
                ratios['debt_to_equity'] = metrics['Liabilities'] / metrics['StockholdersEquity']
            
            # Efficiency ratios
            if 'Revenues' in metrics and 'Assets' in metrics:
                ratios['asset_turnover'] = metrics['Revenues'] / metrics['Assets']
                
        except (ZeroDivisionError, TypeError) as e:
            logger.warning(f"Ratio calculation error for CIK {cik}: {e}")
        
        return ratios
    
    def cleanup_old_data(self, days_old: int = 30) -> int:
        """
        Clean up old temporary data (optional maintenance function)
        
        Args:
            days_old: Remove data older than this many days
            
        Returns:
            Number of records cleaned up
        """
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        try:
            # Clean old data quality logs
            cursor.execute("""
                DELETE FROM data_quality_log 
                WHERE checked_at < datetime('now', '-{} days')
            """.format(days_old))
            
            cleaned_records = cursor.rowcount
            conn.commit()
            
            logger.info(f"Cleaned up {cleaned_records} old records")
            return cleaned_records
            
        except Exception as e:
            conn.rollback()
            logger.error(f"Cleanup failed: {e}")
            raise
        finally:
            conn.close()

# Legacy compatibility class for existing code
class DatabaseManager(ProductionDatabaseManager):
    """
    Legacy wrapper to maintain compatibility with existing main_app.py
    Extends ProductionDatabaseManager with the old interface
    """
    
    def __init__(self, db_path: str = "financial_data.db"):
        # Initialize with legacy database name but production schema
        super().__init__(db_path)
        # Add sample data if database is empty (for demo purposes)
        self._ensure_demo_data()
    
    def _ensure_demo_data(self):
        """Ensure some demo data exists for development"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Check if we have any companies
        cursor.execute("SELECT COUNT(*) FROM companies")
        count = cursor.fetchone()[0]
        
        if count == 0:
            # Add some basic demo companies
            demo_companies = [
                ('0000320193', 'Apple Inc.', 'AAPL'),
                ('0000789019', 'Microsoft Corporation', 'MSFT'),
                ('0001318605', 'Tesla Inc.', 'TSLA'),
            ]
            
            cursor.executemany("""
                INSERT OR REPLACE INTO companies (cik, name, ticker)
                VALUES (?, ?, ?)
            """, demo_companies)
            
            conn.commit()
            logger.info("Added demo companies to database")
        
        conn.close()
    
    def search_line_items(self, search_term: str, limit: int = 50) -> pd.DataFrame:
        """Legacy method name - calls smart_search"""
        return self.smart_search(search_term, limit)
    
    def get_company_metrics(self) -> Dict:
        """Legacy method name - calls get_company_metrics_summary"""
        return self.get_company_metrics_summary()
    
    def populate_sample_data(self):
        """Legacy method - now handled automatically"""
        logger.info("Sample data population handled automatically")
        pass

if __name__ == "__main__":
    # Test the database manager
    db = ProductionDatabaseManager()
    print("Production database initialized successfully!")
    
    # Show metrics
    metrics = db.get_company_metrics_summary()
    print(f"Database contains {metrics['total_companies']} companies")
    print(f"Total facts: {metrics['total_facts']}")
    print(f"Year range: {metrics['year_range']}")