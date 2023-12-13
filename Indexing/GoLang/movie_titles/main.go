// This program does the distirbuted indexing using a map reduce technique
package main

import (
    "bufio"
    "fmt"
    "strings"
    "sync"
	"encoding/json"
    // "github.com/jdkato/prose/v2"
	"os"
	"path"
	"time"
)
// InvertedIndex is a type alias for the map type used for the inverted index
type InvertedIndex map[string]map[string]struct{}
const IntermediateOutputDir = "./intermediate_output/"
const finalOutputDir = "../../Indexes/"


// Function to process a chunk of lines and return an intermediate index
func processChunk(chunk []string, chunkID int, wg *sync.WaitGroup){
	fmt.Printf("Processing chunk %d\n", chunkID)
	start := time.Now()
	defer wg.Done()
	ii := make(InvertedIndex)

	for _, line := range chunk {
		fields := strings.Split(line, "\t")
		if len(fields) > 3 {
			tconst := fields[0]
			primaryTitle := fields[2]

			tokens := strings.Fields(primaryTitle)
			for _, token := range tokens {
				lowerToken := strings.ToLower(token)
				if _, exists := ii[lowerToken]; !exists {
					ii[lowerToken] = make(map[string]struct{})
				}
				ii[lowerToken][tconst] = struct{}{}
			}

		
			// // Tokenizing the title using prose
			// doc, _ := prose.NewDocument(primaryTitle)
			// for _, token := range doc.Tokens() {
			// 	lowerToken := strings.ToLower(token.Text)
			// 	if _, exists := ii[lowerToken]; !exists {
			// 		ii[lowerToken] = make(map[string]struct{})
			// 	}
			// 	ii[lowerToken][tconst] = struct{}{}
			// }
		}
	}
    
    saveIntermediateIndex(ii, fmt.Sprintf("intermediate_index_%d.json", chunkID))

	end := time.Now()
    duration := end.Sub(start)
    fmt.Printf("Processing chunk %d took %v\n", chunkID, duration)
}

// Function to save intermediate index
func saveIntermediateIndex(ii InvertedIndex, filename string) {
    // Create the intermediate output directory if it doesn't exist
    if _, err := os.Stat(IntermediateOutputDir); os.IsNotExist(err) {
        os.Mkdir(IntermediateOutputDir, 0755)
    }

    // Join the directory path and the filename
    filepath := path.Join(IntermediateOutputDir, filename)

    data, err := json.Marshal(ii)
    if err != nil {
        panic(err)
    }
    if err := os.WriteFile(filepath, data, 0644); err != nil {
        panic(err)
    }
}

func deleteIntermediateFiles() {
    dir, err := os.ReadDir(IntermediateOutputDir)
    if err != nil {
        panic(err)
    }
    for _, d := range dir {
        os.Remove(path.Join(IntermediateOutputDir, d.Name()))
    }
}

// Function to read file in chunks and process
func processFile(filePath string, chunkSize int){
	start := time.Now()

	deleteIntermediateFiles()
    file, err := os.Open(filePath)
    if err != nil {
        panic(err)
    }
    defer file.Close()

    scanner := bufio.NewScanner(file)
    var wg sync.WaitGroup

    chunk := make([]string, 0, chunkSize)
    chunkID := 0

    for scanner.Scan() {
        chunk = append(chunk, scanner.Text())
        if len(chunk) == chunkSize {
            wg.Add(1)
            go func(c []string, id int) {
                processChunk(c, id,&wg)
            }(chunk, chunkID)
            chunkID++
            chunk = make([]string, 0, chunkSize)
        }
    }
    // Process remaining lines
    if len(chunk) > 0 {
        wg.Add(1)
        go processChunk(chunk, chunkID,&wg)
    }

    wg.Wait()

	end := time.Now()
    duration := end.Sub(start)
    fmt.Printf("MAP TASK COMPLETE, Processing took %v\n", duration)
    
}

type syncData struct{
	mu *sync.Mutex
	documents map[string]struct{}
}

// Function to merge intermediate indices into the final index
func mergeIndices() {
    // Read the intermediate output directory
	files, err := os.ReadDir(IntermediateOutputDir)
	if err != nil {
		panic(err)
	}
	// Create the final index
	finalIndex := &sync.Map{}

	// Create a WaitGroup to wait for all goroutines to finish
	var wg sync.WaitGroup

	// Iterate over the files
	for _, file := range files {
		wg.Add(1)
		go func(file os.DirEntry) {
			defer wg.Done()

			// Read the file
			data, err := os.ReadFile(path.Join(IntermediateOutputDir, file.Name()))
			if err != nil {
				panic(err)
			}

			// Unmarshal the data into an intermediate index
			var ii InvertedIndex
			if err := json.Unmarshal(data, &ii); err != nil {
				panic(err)
			}

			// Merge the intermediate index into the final index
			for k, v := range ii {
				actual, _ := finalIndex.LoadOrStore(k, &syncData{
					mu:    &sync.Mutex{},
					documents: make(map[string]struct{}),
				})
				data := actual.(*syncData)
				data.mu.Lock()
				for k := range v {
					data.documents[k] = struct{}{}
				}
				data.mu.Unlock()
			}
		}(file)
	}

	// Wait for all goroutines to finish
	wg.Wait()

	// Save the final index
	saveFinalInvertedIndex(finalIndex, finalOutputDir + "movie-titles.json")

}

func saveFinalInvertedIndex(ii *sync.Map, fileName string) {
	file, err := os.Create(fileName)
	if err != nil {
		panic(err) // Consider more graceful error handling
	}
	defer file.Close()

	jsonEncoder := json.NewEncoder(file)
	ii.Range(func(term, data interface{}) bool {
		syncData := data.(*syncData)
		documents := keys(syncData.documents)

		dataToEncode := map[string]interface{}{
			"term":     term,
			"documents":  documents,
			"document_count": len(documents),
		}

		if err := jsonEncoder.Encode(dataToEncode); err != nil {
			panic(err) // Consider more graceful error handling
		}
		return true
	})
}


func keys(set map[string]struct{}) []string {
	keys := make([]string, 0, len(set))
	for k := range set {
		keys = append(keys, k)
	}
	return keys
}


func main() {
    // Call processFile with the appropriate file path and chunk size
    // processFile("../../Original_datasets/title.basics.tsv", 10000)
    processFile("../../Original_datasets/title.basics.tsv", 2000000)
    // processFile("../../compressed/title.basics.tsv", 5)
    mergeIndices()
}
