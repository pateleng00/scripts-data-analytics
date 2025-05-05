import pandas as pd
import psycopg2


conn = psycopg2.connect(database="post",
                        user="postgres",
                        host='yourdbhost',
                        password="pass",
                        port=5432)
cursor = conn.cursor()

query = """
WITH ranked_data AS (
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY vehicle_no ORDER BY beacon_date_time DESC) AS rn,
        ROW_NUMBER() OVER (PARTITION BY vehicle_no ORDER BY beacon_date_time ASC) AS rrn
    FROM
        can_data_2024_08_03
)
SELECT
    vehicle_no, soc, beacon_date_time
FROM
    ranked_data
WHERE
    rn = 1
union all
SELECT
    vehicle_no, soc, beacon_date_time
FROM
    ranked_data
WHERE
    rrn = 1;
"""

# Execute the query
cursor.execute(query)
columns_ = ["vehicle_no", "soc", " beacon_date_time"]
# Fetch the results
results = cursor.fetchall()
# Convert the results to a DataFrame
df = pd.DataFrame(results, columns=columns_)

# Write the DataFrame to an Excel file
df.to_excel('output.xlsx', index=False)

print("Data written to 'output.xlsx'")
# Print the results
for row in results:
    print(row)
