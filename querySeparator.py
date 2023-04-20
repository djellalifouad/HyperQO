from sqlalchemy import create_engine, MetaData
from ImportantConfig import Config
config = Config()
# Sample query string
with open(config.queries_file) as f:
    import json
    queries = json.load(f)
# Database connection string
import os

# Create a directory to save the file in if it doesn't exist
import uuid

# Generate a unique ID



connection_string = "postgresql://postgres:@localhost:5401/imdb"
engine = create_engine(connection_string)
metadata = MetaData()
metadata.reflect(bind=engine)
import  re
import sqlparse
for idx,x in enumerate(queries[:]):
    query = x[0]
    from_clause_pattern = r"\bFROM\s+((?:\w+\s+(?:AS\s+)?\w+,\s*)*\w+\s+(?:AS\s+)?\w+)"
    match = re.search(from_clause_pattern, query, re.IGNORECASE)
    table_list = match.group(1).replace(" ", "").split(",")
    num_tables = len(table_list)
    print(num_tables)
    # Count the number of tables
    if not os.path.exists('./dataset2/'+str(num_tables)+'/'):
        os.makedirs('./dataset2/'+str(num_tables)+'/')
    with open('./dataset2/'+str(num_tables)+'/'+str(idx)+'.txt', 'w') as f:
        # Write a string to the file
        f.write(x[0])
        f.close()

