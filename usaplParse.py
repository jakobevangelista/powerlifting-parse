import pandas as pd

# Initialize an empty list to hold the filtered chunks
filtered_chunks = []

# Define the chunk size
chunk_size = 100000  # Start with 500,000 rows per chunk

# Read the CSV file in chunks
for chunk in pd.read_csv('filtered_data.csv', chunksize=chunk_size, low_memory=False):
    # Filter the chunk for rows where the "Federation" column is "USAPL", "IPF", or "AMP"
    filtered_chunk = chunk[chunk['Federation'] =='USAPL']
    # Append the filtered chunk to the list
    filtered_chunks.append(filtered_chunk)

# Concatenate all the filtered chunks into a single DataFrame
filtered_df = pd.concat(filtered_chunks)

hannahNguyen_df = filtered_df[filtered_df['Name'] == 'Hannah Nguyen']

if not hannahNguyen_df.empty:
    print(hannahNguyen_df)
else:
    print("No rows with 'Hannah Nguyen' found.")

# Check if there is at least one row with 'USAPL'
# if not filtered_df.empty:
#     # Get the first row with 'USAPL'
#     first_usapl_row = filtered_df.iloc[0]
#     # Print the first row
#     print(first_usapl_row)
# else:
#     print("No rows with 'USAPL' found.")

# Optionally, save the filtered DataFrame to a new CSV file
# filtered_df.to_csv('filtered_data.csv', index=False)