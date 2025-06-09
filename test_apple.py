import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from parsers.edgar_parser import EdgarAPIClient, XBRLProcessor, CompanyLookup
from database.db_manager import ProductionDatabaseManager

def test_apple():
    print("🍎 Testing Apple SEC data integration...")
    
    # Initialize components
    client = EdgarAPIClient()
    processor = XBRLProcessor()
    db = ProductionDatabaseManager("test_financial_data.db")
    
    # Get Apple's CIK
    apple_cik = CompanyLookup.get_cik_by_ticker('AAPL')
    print(f"Apple CIK: {apple_cik}")
    
    # Fetch real SEC data
    print("📡 Fetching real SEC data...")
    company_data = client.get_company_facts(apple_cik)
    
    if company_data:
        print(f"✅ Got data for: {company_data['entityName']}")
        
        # Process facts
        print("🔄 Processing financial facts...")
        facts = processor.extract_financial_facts(company_data)
        print(f"📊 Extracted {len(facts)} financial facts")
        
        # Store in database
        print("💾 Storing in database...")
        db.store_company_facts(facts)
        
        # Show some results
        print("\n🎯 Sample results:")
        for i, fact in enumerate(facts[:5]):
            print(f"  {fact.label}: ${fact.value:,.0f} ({fact.end_date})")
        
        print(f"\n✅ Success! Stored {len(facts)} real SEC facts for Apple")
        return True
    else:
        print("❌ Failed to fetch Apple data")
        return False

if __name__ == "__main__":
    test_apple()
