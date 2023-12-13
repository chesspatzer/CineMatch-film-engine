import pandas as pd
from multiprocessing import Pool, cpu_count
import time
import json
import os
from nltk.corpus import stopwords
import string

# Improved tokenize function
def tokenize(title):
    return title.str.split().explode()

# Function to write an inverted index to a file in the specified JSON format
def write_inverted_index_to_json(inverted_index, filename):
    with open(filename, 'w') as file:
        json.dump(inverted_index, file)

def process_chunk(chunk, chunk_id):
    start_time = time.time()
    print(f"Processing chunk {chunk_id} in process {os.getpid()}...")

    # Tokenize and filter
    tokens = tokenize(chunk['primaryTitle'].str.lower())
    tokens = tokens[~tokens.isin(stopwords.words('english') + list(string.punctuation))]

    # Create inverted index
    inverted_index = tokens.groupby(tokens).apply(set).to_dict()

    # Measure the time taken to write the inverted index to a file
    write_start_time = time.time()
    write_inverted_index_to_json(inverted_index, f'intermediate_v1/inverted_index_chunk_{chunk_id}.json')
    write_end_time = time.time()
    print(f"Time taken to write inverted index of chunk {chunk_id} to file: {write_end_time - write_start_time} seconds.")

    end_time = time.time()
    print(f"Finished processing chunk {chunk_id} in process {os.getpid()}. Total time taken: {end_time - start_time} seconds.")

if __name__ == "__main__":

    # Read the TSV file and divide it into chunks
    chunk_size = 100000  # Define your chunk size
    pool = Pool(cpu_count())  # Create a multiprocessing Pool

    results = []
    chunk_id = 0

    # Read the file in chunks
    for chunk in pd.read_csv('Original_datasets/title.basics.tsv', chunksize=chunk_size, delimiter='\t', encoding='utf-8'):
        if chunk_id == 3: break
        # Process each chunk in parallel
        result = pool.apply_async(process_chunk, args=(chunk, chunk_id))
        results.append(result)
        chunk_id += 1

    # Wait for all tasks to complete
    for result in results:
        result.get()  # This will re-raise any exceptions that occurred

    pool.close()
    pool.join()

    print("Processing complete.")
