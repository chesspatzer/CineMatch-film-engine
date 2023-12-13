package main

import (
	"bufio"
	"encoding/json"
	"os"
	"strings"
	"sync"
	"log"
	"time"
)

// TSVLine represents a line in the TSV file
type TSVLine struct {
	Tconst     string
	Ordering   string
	Nconst     string
	Category   string
	Job        string
	Characters string
}

// InvertedIndex represents the inverted index data structure
type InvertedIndex map[string]map[string]struct{}

const SavePath = "../../Indexes/index_names_to_titles/"
// const ReadPath = "../../compressed/"
const ReadPath = "../../Original_datasets/"

func main() {
	categoriesToFilter := []string{"actor","actress","director","producer","writer"}
	var wg sync.WaitGroup
	nameMap := buildNameMap(ReadPath + "name.basics.tsv")
	lines := readLines(ReadPath + "title.principals.tsv")
	for _, category := range categoriesToFilter {
		wg.Add(1)
		go func(category string) {
			defer wg.Done()
			start := time.Now()
			invertedIndex := buildInvertedIndex(lines, category,nameMap)
			saveInvertedIndex(invertedIndex, SavePath + category + "_to_movies.json")
			duration := time.Since(start)
			log.Printf("Time taken for category %s: %v\n", category, duration)
		}(category)
	}

	wg.Wait()
}

func readLines(filePath string) []TSVLine {
    file, err := os.Open(filePath)
    if err != nil {
        panic(err)
    }
    defer file.Close()

    scanner := bufio.NewScanner(bufio.NewReader(file))
    var lines []TSVLine
    for scanner.Scan() {
        line := parseLine(scanner.Text())
        lines = append(lines, line)
    }

    if err := scanner.Err(); err != nil {
        panic(err)
    }

    return lines
}

func buildNameMap(filePath string) map[string]string {
	file, err := os.Open(filePath)
	if err != nil {
		panic(err)
	}
	defer file.Close()

	scanner := bufio.NewScanner(bufio.NewReader(file))
	nameMap := make(map[string]string)

	for scanner.Scan() {
		line := parseNameLine(scanner.Text())
		nameMap[line.Nconst] = line.PrimaryName
	}

	if err := scanner.Err(); err != nil {
		panic(err)
	}

	return nameMap
}

// NameBasic represents a line in the name.basics.tsv file
type NameBasic struct {
	Nconst      string
	PrimaryName string
}

func parseNameLine(line string) NameBasic {
	fields := strings.Split(line, "\t")
	return NameBasic{
		Nconst:      fields[0],
		PrimaryName: fields[1],
	}
}

func buildInvertedIndex(lines []TSVLine, category string, nameMap map[string]string) InvertedIndex {
	
	index := make(InvertedIndex)
	for _, line := range lines {
	if line.Category == category {
		name := nameMap[line.Nconst]
		for _, word := range strings.Fields(name) { // Split the name into words
			if _, exists := index[word]; !exists {
				index[word] = make(map[string]struct{})
			}
			index[word][line.Tconst] = struct{}{}

	}
}
	}

	return index
}

func parseLine(line string) TSVLine {
	fields := strings.Split(line, "\t")
	return TSVLine{
		Tconst:     fields[0],
		Ordering:   fields[1],
		Nconst:     fields[2],
		Category:   fields[3],
		Job:        fields[4],
		Characters: fields[5],
	}
}

func saveInvertedIndex(index InvertedIndex, fileName string) {
	file, err := os.Create(fileName)
	if err != nil {
		panic(err) // Consider more graceful error handling
	}
	defer file.Close()

	jsonEncoder := json.NewEncoder(file)
	for term, titles := range index {
		data := map[string]interface{}{
			"term":     term,
			"documents":  keys(titles),
			"document_count": len(titles),
		}

		if err := jsonEncoder.Encode(data); err != nil {
			panic(err) // Consider more graceful error handling
		}
	}
}

func keys(set map[string]struct{}) []string {
	keys := make([]string, 0, len(set))
	for k := range set {
		keys = append(keys, k)
	}
	return keys
}
