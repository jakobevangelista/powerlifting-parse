import pandas as pd

# Initialize an empty list to hold the filtered chunks
filtered_chunks = []

# Define the chunk size
chunk_size = 500000  # Start with 500,000 rows per chunk

# Read the CSV file in chunks
for chunk in pd.read_csv('powerliftingData.csv', chunksize=chunk_size, low_memory=False):
    # Filter the chunk for rows where the "Federation" column is "USAPL", "IPF", or "AMP"
    filtered_chunk = chunk[chunk['Federation'].isin(['USAPL', 'IPF', 'AMP'])]
    # Append the filtered chunk to the list
    filtered_chunks.append(filtered_chunk)

# Concatenate all the filtered chunks into a single DataFrame
filtered_df = pd.concat(filtered_chunks)

# Display the filtered DataFrame
print(filtered_df[filtered_df["Name"] == "Jakob Evangelista"].to_numpy())

# Optionally, save the filtered DataFrame to a new CSV file
# filtered_df.to_csv('filtered_data.csv', index=False)