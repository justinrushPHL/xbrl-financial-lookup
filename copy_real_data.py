import sqlite3

conn = sqlite3.connect('financial_data.db')
cursor = conn.cursor()

print('Copying 2,770 real SEC records to your app...')

# Clear old sample data
cursor.execute('DELETE FROM financial_line_items')

# Copy real SEC data to the table your app uses
cursor.execute('''
INSERT INTO financial_line_items 
(company_name, ticker_symbol, cik, line_item_label, xbrl_tag, value, 
 filing_date, period_end_date, form_type, accession_number, sec_url, filing_year)
SELECT 
    company_name, 'AAPL', cik, label, tag, value,
    filed_date, end_date, form_type, accession_number, sec_url, 
    CAST(substr(end_date, 1, 4) AS INTEGER)
FROM financial_facts
WHERE company_name = 'Apple Inc.'
''')

conn.commit()

# Check the result
cursor.execute('SELECT COUNT(*) FROM financial_line_items')
count = cursor.fetchone()[0]
print(f'✅ Success! Now you have {count} real Apple records!')

# Show a sample
cursor.execute('SELECT line_item_label, value, filing_year FROM financial_line_items LIMIT 5')
samples = cursor.fetchall()
print('Sample real data:')
for label, value, year in samples:
    print(f'  {label}: ${value:,.0f} ({year})')

conn.close()
