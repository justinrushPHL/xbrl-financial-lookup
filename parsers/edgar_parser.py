"""
parsers/edgar_parser.py
Real SEC EDGAR API integration for the XBRL Financial Lookup app
"""

import requests
import json
import time
import logging
from typing import Dict, List, Optional, Tuple
from datetime import datetime
from dataclasses import dataclass

# Set up logging
logger = logging.getLogger(__name__)

@dataclass
class CompanyFact:
    """Structured representation of a financial fact from SEC filing"""
    cik: str
    company_name: str
    tag: str
    label: str
    description: str
    value: float
    unit: str
    end_date: str
    form_type: str
    filed_date: str
    accession_number: str
    sec_url: str

class EdgarAPIClient:
    """Production SEC EDGAR API client with rate limiting and error handling"""
    
    def __init__(self, user_agent: str = "Financial Research Tool contact@example.com"):
        self.base_url = "https://data.sec.gov/api/xbrl"
        self.headers = {
            'User-Agent': user_agent,
            'Accept-Encoding': 'gzip, deflate',
            'Host': 'data.sec.gov'
        }
        self.last_request_time = 0
        self.min_request_interval = 0.1  # 100ms between requests (SEC requirement)
    
    def _rate_limit(self):
        """Ensure we don't exceed SEC rate limits (10 requests per second)"""
        elapsed = time.time() - self.last_request_time
        if elapsed < self.min_request_interval:
            time.sleep(self.min_request_interval - elapsed)
        self.last_request_time = time.time()
    
    def get_company_facts(self, cik: str) -> Optional[Dict]:
        """
        Fetch all financial facts for a company from SEC EDGAR API
        
        Args:
            cik: Company CIK identifier (e.g., '0000320193' for Apple)
            
        Returns:
            Dictionary containing company financial data or None if failed
        """
        self._rate_limit()
        
        # Ensure CIK is properly formatted (10 digits with leading zeros)
        formatted_cik = cik.zfill(10)
        url = f"{self.base_url}/companyfacts/CIK{formatted_cik}.json"
        
        try:
            logger.info(f"Fetching SEC data for CIK {formatted_cik}")
            response = requests.get(url, headers=self.headers, timeout=30)
            
            if response.status_code == 200:
                data = response.json()
                logger.info(f"Successfully retrieved data for {data.get('entityName', 'Unknown Company')}")
                return data
            elif response.status_code == 404:
                logger.warning(f"No SEC data found for CIK {formatted_cik}")
                return None
            elif response.status_code == 429:
                logger.warning("Rate limit exceeded, waiting...")
                time.sleep(1)
                return self.get_company_facts(cik)  # Retry once
            else:
                logger.error(f"SEC API request failed with status {response.status_code}")
                return None
                
        except requests.exceptions.Timeout:
            logger.error(f"Request timeout for CIK {formatted_cik}")
            return None
        except requests.exceptions.RequestException as e:
            logger.error(f"Request failed for CIK {formatted_cik}: {e}")
            return None
        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse SEC JSON response: {e}")
            return None
    
    def get_company_submissions(self, cik: str) -> Optional[Dict]:
        """Get company submission history (filings list)"""
        self._rate_limit()
        
        formatted_cik = cik.zfill(10)
        url = f"https://data.sec.gov/submissions/CIK{formatted_cik}.json"
        
        try:
            response = requests.get(url, headers=self.headers, timeout=30)
            if response.status_code == 200:
                return response.json()
            return None
        except Exception as e:
            logger.error(f"Failed to get submissions for CIK {formatted_cik}: {e}")
            return None

class XBRLProcessor:
    """Process and standardize XBRL financial data from SEC filings"""
    
    def __init__(self):
        # Map of important financial metrics we want to track
        self.key_financial_tags = {
            # Revenue metrics
            'Revenues': 'Total Revenue',
            'SalesRevenueNet': 'Net Sales',
            'RevenueFromContractWithCustomerExcludingAssessedTax': 'Revenue from Contracts',
            
            # Asset metrics  
            'Assets': 'Total Assets',
            'AssetsCurrent': 'Current Assets',
            'AssetsNoncurrent': 'Non-current Assets',
            'CashAndCashEquivalentsAtCarryingValue': 'Cash and Cash Equivalents',
            
            # Liability and Equity
            'Liabilities': 'Total Liabilities',
            'LiabilitiesCurrent': 'Current Liabilities',
            'StockholdersEquity': 'Stockholders Equity',
            
            # Income Statement
            'CostOfGoodsAndServicesSold': 'Cost of Goods Sold',
            'GrossProfit': 'Gross Profit',
            'OperatingIncomeLoss': 'Operating Income',
            'NetIncomeLoss': 'Net Income',
            'EarningsPerShareBasic': 'Basic EPS',
            'EarningsPerShareDiluted': 'Diluted EPS',
            
            # Cash Flow
            'NetCashProvidedByUsedInOperatingActivities': 'Operating Cash Flow',
            'NetCashProvidedByUsedInInvestingActivities': 'Investing Cash Flow',
            'NetCashProvidedByUsedInFinancingActivities': 'Financing Cash Flow',
        }
    
    def extract_financial_facts(self, company_data: Dict) -> List[CompanyFact]:
        """
        Extract and process key financial metrics from SEC company data
        
        Args:
            company_data: Raw SEC EDGAR API response
            
        Returns:
            List of structured CompanyFact objects
        """
        if not company_data or 'facts' not in company_data:
            logger.warning("No financial facts found in SEC data")
            return []
        
        company_name = company_data.get('entityName', 'Unknown Company')
        cik = str(company_data.get('cik', ''))
        
        facts = []
        
        # Process US-GAAP facts (standard financial reporting)
        us_gaap_facts = company_data['facts'].get('us-gaap', {})
        
        for tag, tag_data in us_gaap_facts.items():
            if tag in self.key_financial_tags:
                tag_facts = self._process_financial_tag(
                    cik, company_name, tag, tag_data
                )
                facts.extend(tag_facts)
        
        # Also process DEI (Document and Entity Information) facts
        dei_facts = company_data['facts'].get('dei', {})
        for tag, tag_data in dei_facts.items():
            if tag in ['EntityCommonStockSharesOutstanding', 'EntityPublicFloat']:
                tag_facts = self._process_financial_tag(
                    cik, company_name, tag, tag_data
                )
                facts.extend(tag_facts)
        
        logger.info(f"Extracted {len(facts)} financial facts for {company_name}")
        return facts
    
    def _process_financial_tag(self, cik: str, company_name: str, tag: str, tag_data: Dict) -> List[CompanyFact]:
        """Process individual financial tag data into structured facts"""
        facts = []
        
        label = tag_data.get('label', self.key_financial_tags.get(tag, tag))
        description = tag_data.get('description', '')
        
        # Process different units (USD, shares, etc.)
        units_data = tag_data.get('units', {})
        
        for unit, entries in units_data.items():
            for entry in entries:
                # Only process 10-K (annual) and 10-Q (quarterly) filings
                form_type = entry.get('form', '')
                if form_type not in ['10-K', '10-Q']:
                    continue
                
                try:
                    # Create SEC filing URL
                    accession = entry.get('accn', '')
                    sec_url = self._build_sec_url(cik, accession) if accession else ''
                    
                    fact = CompanyFact(
                        cik=cik,
                        company_name=company_name,
                        tag=tag,
                        label=label,
                        description=description,
                        value=float(entry.get('val', 0)),
                        unit=unit,
                        end_date=entry.get('end', ''),
                        form_type=form_type,
                        filed_date=entry.get('filed', ''),
                        accession_number=accession,
                        sec_url=sec_url
                    )
                    facts.append(fact)
                    
                except (ValueError, TypeError) as e:
                    logger.debug(f"Skipped invalid entry for {tag}: {e}")
                    continue
        
        return facts
    
    def _build_sec_url(self, cik: str, accession_number: str) -> str:
        """Build SEC EDGAR filing URL from CIK and accession number"""
        if not cik or not accession_number:
            return ''
        
        # Format: https://www.sec.gov/ix?doc=/Archives/edgar/data/{cik}/{accession}/{filename}
        clean_cik = cik.lstrip('0')  # Remove leading zeros for URL
        clean_accession = accession_number.replace('-', '')
        
        # Most recent filings use .htm extension
        filename = f"{accession_number}.htm"
        
        return f"https://www.sec.gov/ix?doc=/Archives/edgar/data/{clean_cik}/{clean_accession}/{filename}"
    
    def get_latest_annual_data(self, facts: List[CompanyFact]) -> Dict[str, CompanyFact]:
        """Get the most recent annual (10-K) data for each financial metric"""
        latest_annual = {}
        
        for fact in facts:
            if fact.form_type == '10-K':
                key = fact.tag
                if key not in latest_annual or fact.end_date > latest_annual[key].end_date:
                    latest_annual[key] = fact
        
        return latest_annual
    
    def get_quarterly_trends(self, facts: List[CompanyFact], tag: str, periods: int = 8) -> List[CompanyFact]:
        """Get recent quarterly trends for a specific financial metric"""
        quarterly_facts = [f for f in facts if f.tag == tag and f.form_type == '10-Q']
        
        # Sort by end date (most recent first)
        quarterly_facts.sort(key=lambda x: x.end_date, reverse=True)
        
        return quarterly_facts[:periods]

class CompanyLookup:
    """Helper class for company CIK lookup and validation"""
    
    # Major companies for testing/demo
    MAJOR_COMPANIES = {
        'AAPL': {'cik': '0000320193', 'name': 'Apple Inc.'},
        'MSFT': {'cik': '0000789019', 'name': 'Microsoft Corporation'},
        'GOOGL': {'cik': '0001652044', 'name': 'Alphabet Inc.'},
        'AMZN': {'cik': '0001018724', 'name': 'Amazon.com Inc.'},
        'TSLA': {'cik': '0001318605', 'name': 'Tesla Inc.'},
        'META': {'cik': '0001326801', 'name': 'Meta Platforms Inc.'},
        'NVDA': {'cik': '0001045810', 'name': 'NVIDIA Corporation'},
        'NFLX': {'cik': '0001065280', 'name': 'Netflix Inc.'},
    }
    
    @classmethod
    def get_cik_by_ticker(cls, ticker: str) -> Optional[str]:
        """Get CIK for a ticker symbol"""
        return cls.MAJOR_COMPANIES.get(ticker.upper(), {}).get('cik')
    
    @classmethod
    def get_company_info(cls, ticker: str) -> Optional[Dict[str, str]]:
        """Get company information by ticker"""
        return cls.MAJOR_COMPANIES.get(ticker.upper())
    
    @classmethod
    def get_all_major_companies(cls) -> Dict[str, Dict[str, str]]:
        """Get all major companies for bulk processing"""
        return cls.MAJOR_COMPANIES.copy()

# Example usage and testing
def test_apple_parsing():
    """Test the EDGAR parser with Apple Inc."""
    client = EdgarAPIClient()
    processor = XBRLProcessor()
    
    apple_cik = CompanyLookup.get_cik_by_ticker('AAPL')
    print(f"Testing with Apple Inc. (CIK: {apple_cik})")
    
    # Fetch data
    company_data = client.get_company_facts(apple_cik)
    if not company_data:
        print("Failed to fetch Apple data")
        return
    
    # Process facts
    facts = processor.extract_financial_facts(company_data)
    print(f"Extracted {len(facts)} financial facts")
    
    # Show latest annual metrics
    latest_annual = processor.get_latest_annual_data(facts)
    print("\nLatest Annual Metrics:")
    for tag, fact in latest_annual.items():
        print(f"{fact.label}: ${fact.value:,.0f} ({fact.end_date})")
    
    return facts

if __name__ == "__main__":
    test_apple_parsing()