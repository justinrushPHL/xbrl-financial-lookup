#!/usr/bin/env python3
"""
real_data_integration.py

Script to integrate real SEC EDGAR data into your existing XBRL Financial Lookup app.
This script fetches real data and updates your database with production-quality financial facts.

Usage:
    python real_data_integration.py --company AAPL
    python real_data_integration.py --all-major
    python real_data_integration.py --company AAPL --update-app
"""

import argparse
import logging
import sys
import os
from typing import List

# Add project root to path so we can import our modules
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from parsers.edgar_parser import EdgarAPIClient, XBRLProcessor, CompanyLookup
from database.db_manager import ProductionDatabaseManager

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class RealDataIntegration:
    """Orchestrates the integration of real SEC data into your app"""
    
    def __init__(self, use_production_db: bool = True):
        self.edgar_client = EdgarAPIClient()
        self.processor = XBRLProcessor()
        
        # Choose database based on flag
        if use_production_db:
            self.db = ProductionDatabaseManager("production_financial_data.db")
        else:
            # Update existing demo database with real data
            from database.db_manager import DatabaseManager
            self.db = DatabaseManager("financial_data.db")
    
    def fetch_and_store_company(self, ticker: str) -> bool:
        """
        Fetch real SEC data for a company and store in database
        
        Args:
            ticker: Stock ticker symbol (e.g., 'AAPL')
            
        Returns:
            True if successful, False otherwise
        """
        logger.info(f"🚀 Starting real data integration for {ticker}")
        
        # Get company info
        company_info = CompanyLookup.get_company_info(ticker)
        if not company_info:
            logger.error(f"❌ Unknown ticker: {ticker}")
            return False
        
        cik = company_info['cik']
        company_name = company_info['name']
        
        logger.info(f"📡 Fetching SEC data for {company_name} (CIK: {cik})")
        
        # Fetch real SEC data
        company_data = self.edgar_client.get_company_facts(cik)
        if not company_data:
            logger.error(f"❌ Failed to fetch SEC data for {ticker}")
            return False
        
        # Process the data
        logger.info("🔄 Processing financial facts...")
        facts = self.processor.extract_financial_facts(company_data)
        
        if not facts:
            logger.error(f"❌ No financial facts extracted for {ticker}")
            return False
        
        # Store in database
        logger.info(f"💾 Storing {len(facts)} facts in database...")
        self.db.store_company_facts(facts)
        
        logger.info(f"✅ Successfully integrated {len(facts)} facts for {company_name}")
        return True
    
    def integrate_major_companies(self, limit: int = None) -> Dict[str, bool]:
        """
        Integrate data for all major companies
        
        Args:
            limit: Maximum number of companies to process (None for all)
            
        Returns:
            Dictionary of ticker -> success status
        """
        companies = CompanyLookup.get_all_major_companies()
        
        if limit:
            companies = dict(list(companies.items())[:limit])
        
        logger.info(f"🏢 Integrating data for {len(companies)} major companies...")
        
        results = {}
        for ticker, info in companies.items():
            logger.info(f"\n📈 Processing {ticker} ({info['name']})...")
            try:
                success = self.fetch_and_store_company(ticker)
                results[ticker] = success
                
                if success:
                    logger.info(f"✅ {ticker} completed successfully")
                else:
                    logger.warning(f"⚠️  {ticker} failed")
                    
            except Exception as e:
                logger.error(f"❌ {ticker} failed with error: {e}")
                results[ticker] = False
        
        # Summary
        successful = sum(1 for success in results.values() if success)
        total = len(results)
        
        logger.info(f"\n🎯 Integration Summary:")
        logger.info(f"   Successful: {successful}/{total}")
        logger.info(f"   Failed: {total - successful}/{total}")
        
        return results
    
    def show_database_stats(self):
        """Display current database statistics"""
        logger.info("📊 Current Database Statistics:")
        logger.info("=" * 50)
        
        metrics = self.db.get_company_metrics_summary()
        
        logger.info(f"Companies: {metrics['total_companies']}")
        logger.info(f"Financial Facts: {metrics['total_facts']:,}")
        logger.info(f"Unique Metrics: {metrics['unique_tags']}")
        logger.info(f"Total Filings: {metrics['total_filings']}")
        logger.info(f"Year Range: {metrics['year_range']}")
        logger.info(f"Last Updated: {metrics.get('last_updated', 'N/A')}")
        
        if 'top_companies' in metrics:
            logger.info(f"\nTop Companies by Data Volume:")
            for name, ticker, count in metrics['top_companies']:
                logger.info(f"  {name} ({ticker}): {count:,} facts")
    
    def update_streamlit_app(self):
        """Update the main Streamlit app to use the new production database"""
        logger.info("🔄 Updating Streamlit app configuration...")
        
        # Update main_app.py to use production database
        try:
            main_app_path = "main_app.py"
            
            # Read current app
            with open(main_app_path, 'r') as f:
                content = f.read()
            
            # Update database manager import and usage
            updated_content = content.replace(
                'st.session_state.db_manager = DatabaseManager()',
                'st.session_state.db_manager = ProductionDatabaseManager("production_financial_data.db")'
            )
            
            # Add import if not present
            if 'from database.db_manager import ProductionDatabaseManager' not in updated_content:
                import_line = 'from database.db_manager import DatabaseManager'
                new_import = 'from database.db_manager import DatabaseManager, ProductionDatabaseManager'
                updated_content = updated_content.replace(import_line, new_import)
            
            # Write back
            with open(main_app_path, 'w') as f:
                f.write(updated_content)
            
            logger.info("✅ Streamlit app updated to use production database")
            
        except Exception as e:
            logger.error(f"❌ Failed to update Streamlit app: {e}")
            logger.info("💡 Manual update required - see integration guide")

def main():
    """Main CLI interface"""
    parser = argparse.ArgumentParser(description='Integrate real SEC EDGAR data into XBRL Financial Lookup app')
    
    parser.add_argument('--company', '-c', type=str, help='Stock ticker to integrate (e.g., AAPL)')
    parser.add_argument('--all-major', action='store_true', help='Integrate all major companies')
    parser.add_argument('--limit', type=int, help='Limit number of companies (use with --all-major)')
    parser.add_argument('--update-app', action='store_true', help='Update Streamlit app to use production database')
    parser.add_argument('--stats', action='store_true', help='Show current database statistics')
    parser.add_argument('--demo-db', action='store_true', help='Update existing demo database instead of creating new one')
    
    args = parser.parse_args()
    
    if not any([args.company, args.all_major, args.stats, args.update_app]):
        parser.print_help()
        return
    
    # Initialize integration
    integrator = RealDataIntegration(use_production_db=not args.demo_db)
    
    try:
        # Show current stats
        if args.stats:
            integrator.show_database_stats()
            return
        
        # Single company integration
        if args.company:
            success = integrator.fetch_and_store_company(args.company.upper())
            if success:
                logger.info(f"🎉 {args.company.upper()} integration completed!")
            else:
                logger.error(f"💥 {args.company.upper()} integration failed!")
                sys.exit(1)
        
        # Multiple companies
        elif args.all_major:
            results = integrator.integrate_major_companies(args.limit)
            successful = sum(1 for success in results.values() if success)
            
            if successful > 0:
                logger.info(f"🎉 Integration completed! {successful} companies successful.")
            else:
                logger.error("💥 All integrations failed!")
                sys.exit(1)
        
        # Update Streamlit app
        if args.update_app:
            integrator.update_streamlit_app()
        
        # Show final stats
        print("\n" + "="*60)
        integrator.show_database_stats()
        
        # Instructions
        print("\n🚀 Next Steps:")
        print("1. Run 'streamlit run main_app.py' to test with real data")
        print("2. Search for 'Revenue' to see real SEC data")
        print("3. Check the Trend Analysis with multiple companies")
        print("4. Use Data Explorer to run SQL queries on real data")
        
    except KeyboardInterrupt:
        logger.info("\n⏹️  Integration stopped by user")
    except Exception as e:
        logger.error(f"💥 Integration failed: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()