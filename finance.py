import psycopg2
import csv

# Database connection information
host = 'localhost'
database = 'tumekie'
username = 'tumekie'
password = ''

# Connect to the PostgreSQL database
conn = psycopg2.connect(
    host=host,
    database=database,
    user=username,
    password=password
)

# Different sources of withdrawals 
sources = {
    'subs': ['spotify', 'amazon', 'paramnt'],
    'credit': ['capital one', 'credit one', 'ppcr', 'mercury', 'discover', 'affirm'],
    'shop': ['klarna', 'nike'],
    'bills': ['ATM', 'Lu Empire', 'Windstream'],
    'groceries': ['walmart'],
    'transfers': ['zelle'],
    'other': [],  # Default category for remaining withdrawals
}

# Create a cursor object to interact with the database
cur = conn.cursor()

# Create spread table if it doesn't exist
create_table_query = '''
    CREATE TABLE IF NOT EXISTS financeSpreads (
        Subs DECIMAL(10, 2),
        Credit DECIMAL(10, 2),
        Shop DECIMAL(10, 2),
        Bills DECIMAL(10, 2),
        Groceries DECIMAL(10, 2),
        Transfers DECIMAL(10, 2),
        Other DECIMAL(10, 2)
    )
'''
cur.execute(create_table_query)
conn.commit()

# Path to the CSV file
csv_file = './5:8-6:7_wells.csv'

# Open the CSV file and read its contents
with open(csv_file, 'r') as file:
    # Create a CSV reader object
    csv_reader = csv.reader(file)
    
    # Skip the header row if present
    next(csv_reader)
    
    # Iterate over each row in the CSV file
    for row in csv_reader:
        # Extract the data from the row
        Date = row[0]
        Description = row[1]
        Withdrawals = float(row[3]) if row[3] else 0.0  # Handle empty value

        # Determine the category based on the source of withdrawal
        category = None
        
        for source, keywords in sources.items():
            begin = False
            if any(keyword.lower() in Description.lower() for keyword in keywords):
                category = source
                break
        if category is None:
            category = 'other'

        # Prepare the INSERT statement
        insert_query = '''
            INSERT INTO financeSpreads (Subs, Credit, Shop, Bills, Groceries, transfers, Other)
            VALUES (
                CASE WHEN %s = 'subs' THEN %s ELSE 0.0 END,
                CASE WHEN %s = 'credit' THEN %s ELSE 0.0 END,
                CASE WHEN %s = 'shop' THEN %s ELSE 0.0 END,
                CASE WHEN %s = 'bills' THEN %s ELSE 0.0 END,
                CASE WHEN %s = 'groceries' THEN %s ELSE 0.0 END,
                CASE WHEN %s = 'transfers' THEN %s ELSE 0.0 END,
                CASE WHEN %s = 'other' THEN %s ELSE 0.0 END
            )
        '''

        # Execute the INSERT statement with the data from the row
        cur.execute(insert_query, (category, Withdrawals, category, Withdrawals, category, Withdrawals, category, Withdrawals, category, Withdrawals, category, Withdrawals, category, Withdrawals))
        
        
# Commit the changes to the database
conn.commit()

# Close the cursor and connection
cur.close()
conn.close()
