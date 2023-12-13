package main

import ("os"
		"bufio"
		"strings"
		"encoding/json"
		"log"
		"time"
	)


type MovieMetaData struct {
		Tconst     string   `json:"tconst"`
		Actors     []string `json:"actors"`
		Actresses  []string `json:"actresses"`
		Directors  []string `json:"directors"`
		Producers  []string `json:"producers"`
		Writers    []string `json:"writers"`
}

// const filePath = "../../compressed/title.principals.tsv"
const filePath = "../../Original_datasets/title.principals.tsv"
const SavePath = "../../Indexes/movie-metadata.json"

func main() {
	start := time.Now()
	file, err := os.Open(filePath)
	if err != nil {
		panic(err)
	}
	defer file.Close()

	scanner := bufio.NewScanner(bufio.NewReader(file))
	if !scanner.Scan() {
		log.Fatal("Failed to read the header line")
	}
	movies := make(map[string]*MovieMetaData)
	for scanner.Scan() {
		line := parseLine(scanner.Text())
		if _, ok := movies[line.Tconst]; !ok {
			movies[line.Tconst] = &MovieMetaData{
				Tconst:    line.Tconst,
				Actors:     []string{},
				Actresses:  []string{},
				Directors:  []string{},
				Producers:  []string{},
				Writers:    []string{},
			}
		}
		if line.Category == "actor" {
			movies[line.Tconst].Actors = append(movies[line.Tconst].Actors, line.Nconst)
		} else if line.Category == "actress" {
			movies[line.Tconst].Actresses = append(movies[line.Tconst].Actresses, line.Nconst)
		} else if line.Category == "director" {
			movies[line.Tconst].Directors = append(movies[line.Tconst].Directors, line.Nconst)
		} else if line.Category == "producer" {
			movies[line.Tconst].Producers = append(movies[line.Tconst].Producers, line.Nconst)
		} else if line.Category == "writer" {
			movies[line.Tconst].Writers = append(movies[line.Tconst].Writers, line.Nconst)
		}
	}

	if err := scanner.Err(); err != nil {
		panic(err)
	}

	//saving the metadata
	file, err = os.Create(SavePath)
	if err != nil {
		panic(err)
	}
	defer file.Close()
	jsonEncoder := json.NewEncoder(file)
	for _, movie := range movies {
		err := jsonEncoder.Encode(movie)
		if err != nil {
			panic(err)
		}
	}
	log.Printf("Total time taken: %v\n", time.Since(start))


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
type TSVLine struct {
	Tconst     string
	Ordering   string
	Nconst     string
	Category   string
	Job        string
	Characters string
}
