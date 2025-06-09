# Quick script to update main_app.py to use real data

import re

# Read the current app
with open('main_app.py', 'r') as f:
    content = f.read()

# Update the database initialization
old_line = "st.session_state.db_manager = DatabaseManager()"
new_line = "st.session_state.db_manager = ProductionDatabaseManager('test_financial_data.db')"

# Add the import
if 'ProductionDatabaseManager' not in content:
    content = content.replace(
        'from database.db_manager import DatabaseManager',
        'from database.db_manager import DatabaseManager, ProductionDatabaseManager'
    )

content = content.replace(old_line, new_line)

# Write back
with open('main_app_real_data.py', 'w') as f:
    f.write(content)

print("✅ Created main_app_real_data.py with real SEC data!")
