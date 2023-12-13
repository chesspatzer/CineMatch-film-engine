import json
import os
import threading
from collections import defaultdict

class DocumentEntry:
    def __init__(self, term, documents):
        self.term = term
        self.documents = documents

    def to_dict(self):
        return {
            'term': self.term,
            'documents': self.documents,
            'document_count': len(self.documents)
        }

def read_file_and_update_map(filename, doc_map, lock):
    with open(filename, 'r') as file:
        lines = file.readlines()
        for line in lines:
            if not line.strip():
                continue
            entry = json.loads(line)
            term = entry['term']

            with lock:  # Synchronize access to the shared resource
                documents = set(doc_map.get(term, set()))
                documents.update(entry['documents'])
                doc_map[term] = documents

def write_json(data, filename):
    with open(filename, 'w') as file:
        for entry in data:
            json_data = json.dumps(entry.to_dict())
            file.write(json_data + '\n')

def main():
    start_time = os.times()

    intermediate_path = "intermediate"
    file_paths = [os.path.join(intermediate_path, file) for file in os.listdir(intermediate_path) if file.endswith('.json')]

    doc_map = {}
    lock = threading.Lock()
    threads = []
    for file in file_paths:
        thread = threading.Thread(target=read_file_and_update_map, args=(file, doc_map, lock))
        thread.start()
        threads.append(thread)

    for thread in threads:
        thread.join()

    final_data = [DocumentEntry(term, list(documents)) for term, documents in doc_map.items()]
    final_data.sort(key=lambda x: x.term)

    write_json(final_data, "final.json")

    end_time = os.times()
    print("Execution time:", end_time.elapsed - start_time.elapsed)

if __name__ == "__main__":
    main()
