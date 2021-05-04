package main

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type sentenceRecords struct {
	ObjId       primitive.ObjectID `bson:"_id,omitempty"`
	Audio       string             `bson:"audio"`
	AudioSize   int32              `bson:"audio_size"`
	Description string             `bson:"description"`
	Result      Result             `bson:"result"`
	Sentence    string             `bson:"sentence"`
	Time_ai     float64            `bson:"time_ai"`
	Time_api    float64            `bson:"time_api"`
	User_email  string             `bson:"user_email"`
	Created_at  time.Time          `bson:"created_at"`
}

type Result struct {
	Total_score string   `bson:"total_score"`
	Text        string   `bson:"text"`
	Audio_url   string   `bson:"audio_url"`
	Result      []bson.D `bson:"result"`
	Msg         string   `bson:"msg"`
	Success     string   `bson:"success"`
}

var Country Countries

type Email struct {
	Email []string `json:"email"`
}

type Countries map[string]Email

func ReadFile(file string) string {
	dat, _ := ioutil.ReadFile(file)
	return string(dat)

}

func UseMarshal(file string) Countries {
	data := ReadFile(file)
	err := json.Unmarshal([]byte(data), &Country)
	if err != nil {
		log.Fatal(err)
	}
	return Country
}

func getEmail(fileName string) Countries {
	data := UseMarshal(fileName)
	return data

}

func connectMongo(db string, collection string, url string) (*mongo.Collection, context.Context) {
	client, err := mongo.NewClient(options.Client().ApplyURI(url))
	if err != nil {
		log.Fatal(err)
	}
	ctx, _ := context.WithTimeout(context.Background(), 10*time.Minute)
	err = client.Connect(ctx)
	if err != nil {
		log.Fatal(err)
	}
	quickstartDatabase := client.Database(db)
	podcastsCollection := quickstartDatabase.Collection(collection)

	return podcastsCollection, ctx
}

func getResult(userEmail string, podcastsCollection *mongo.Collection, ctx context.Context) []sentenceRecords {
	filter := bson.D{{Key: "user_email", Value: userEmail}}
	cursor, err := podcastsCollection.Find(ctx, filter)
	if err != nil {
		log.Fatal(err)
	}
	var episodes []sentenceRecords
	if err = cursor.All(ctx, &episodes); err != nil {
		log.Fatal(err)
	}
	return episodes
}

func main() {
	db := "learn"
	collection := "sentence_records"
	url := "mongodb://XXXXXX:XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX@XXX.XXX.XXX.XXX:27017/?authSource=learn"
	podcastsCollection, ctx := connectMongo(db, collection, url)
	getResult("", podcastsCollection, ctx)
	data := getEmail("email.json")
	for country, v := range data {
		queryResults := [][]string{}
		f, err := os.Create(country + ".csv")
		defer f.Close()
		if err != nil {
			log.Fatalln("failed to open file", err)
		}
		w := csv.NewWriter(f)
		defer w.Flush()
		for _, email := range v.Email {
			query := getResult(email, podcastsCollection, ctx)
			if len(query) > 0 {
				for _, v := range query {
					resultResult := fmt.Sprintf("%s", v.Result.Result)
					timeAI := fmt.Sprintf("%f", v.Time_ai)
					timeAPI := fmt.Sprintf("%f", v.Time_api)
					ObjId := fmt.Sprintf("%s", v.ObjId)
					AudioSize := fmt.Sprintf("%d", v.AudioSize)
					queryResult := []string{ObjId, v.Audio, AudioSize, v.Description, v.Result.Audio_url, v.Result.Msg, resultResult, v.Result.Success, v.Result.Text, v.Result.Total_score, v.Sentence, timeAI, timeAPI, v.User_email}
					queryResults = append(queryResults, queryResult)
				}
			}
		}
		for _, record := range queryResults {
			if err := w.Write(record); err != nil {
				log.Fatalln("error writing record to file", err)
			}
		}
	}

}
