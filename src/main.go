package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type Kline struct {
	OpenTime                 float64
	OpenPrice                string
	HighPrice                string
	LowPrice                 string
	ClosePrice               string
	Volume                   string
	CloseTime                float64
	QuoteAssetVolume         string
	NumberOfTrades           float64
	TakerBuyBaseAssetVolume  string
	TakerBuyQuoteAssetVolume string
}

func main() {
	startTime := time.Date(2018, 1, 1, 0, 0, 0, 0, time.UTC)
	now := time.Now().UTC()
	endTime := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, time.UTC)
	totalMin := endTime.Sub(startTime).Minutes()
	numIntervals := totalMin / 500

	var totalLoopTime time.Duration
	for i := 0; i < int(numIntervals); i++ {
		intervalStart := startTime.Add(time.Duration(i*500) * time.Minute)
		intervalEnd := intervalStart.Add(500 * time.Minute)

		start := time.Now()
		klines, err := getKlines("BTCUSDT", "1m", intervalStart.Unix(), intervalEnd.Unix(), 500)
		if err != nil {
			fmt.Println(err)
			continue
		}

		err = saveKlines(klines)
		if err != nil {
			fmt.Println(err)
		}

		loopTime := time.Since(start)
		totalLoopTime += loopTime

		percentComplete := float64(i+1) / float64(numIntervals) * 100

		fmt.Printf("Time Left: %0.2f%% complete\n", percentComplete)
	}
}

func saveKlines(klines []Kline) error {
	if len(klines) == 0 {
		return nil
	}

	// Set client options
	clientOptions := options.Client().ApplyURI("mongodb://localhost:27017")

	// Connect to MongoDB
	client, err := mongo.Connect(context.Background(), clientOptions)
	if err != nil {
		return err
	}

	// Check the connection
	err = client.Ping(context.Background(), nil)
	if err != nil {
		return err
	}

	// Access the klines collection
	klinesCollection := client.Database("mydb").Collection("klines")

	// Create the time series collection if it doesn't exist
	cmd := bson.D{
		{Key: "create", Value: "klines"},
		{Key: "timeseries", Value: bson.D{
			{Key: "timeField", Value: "OpenTime"},
			{Key: "granularity", Value: "minutes"},
		}},
	}
	_, err = klinesCollection.Database().RunCommand(context.Background(), cmd).DecodeBytes()
	if err != nil && !strings.Contains(err.Error(), "already exists") {
		return err
	}

	// Build the measurement documents to insert
	var documents []interface{}
	for _, kline := range klines {

		doc := bson.D{
			{Key: "OpenTime", Value: time.Unix(int64(kline.OpenTime/1000), 0)},
			{Key: "CloseTime", Value: time.Unix(int64(kline.CloseTime/1000), 0)},
			{Key: "Open", Value: kline.OpenPrice},
			{Key: "High", Value: kline.HighPrice},
			{Key: "Low", Value: kline.LowPrice},
			{Key: "Close", Value: kline.ClosePrice},
			{Key: "Volume", Value: kline.Volume},
			{Key: "QuoteAssetVolume", Value: kline.QuoteAssetVolume},
			{Key: "NumberOfTrades", Value: kline.NumberOfTrades},
			{Key: "TakerBuyBaseAssetVolume", Value: kline.TakerBuyBaseAssetVolume},
			{Key: "TakerBuyQuoteAssetVolume", Value: kline.TakerBuyQuoteAssetVolume},
		}

		documents = append(documents, doc)
	}

	// Insert the measurement documents into the time series collection
	_, err = klinesCollection.InsertMany(context.Background(), documents)
	if err != nil {
		return err
	}

	// Close the MongoDB connection
	err = client.Disconnect(context.Background())
	if err != nil {
		return err
	}

	return nil
}

func getKlines(symbol string, interval string, startTime int64, endTime int64, limit int) ([]Kline, error) {

	url := fmt.Sprintf("https://api.binance.com/api/v3/klines?symbol=%s&interval=%s&startTime=%d&endTime=%d&limit=%d", symbol, interval, startTime*1000, endTime*1000, limit)
	resp, err := http.Get(url)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode == 429 {
		retryAfter := resp.Header.Get("Retry-After")
		if retryAfter != "" {
			waitTime, err := strconv.Atoi(retryAfter)
			if err != nil {
				return nil, err
			}
			time.Sleep(time.Duration(waitTime) * time.Second)
			return getKlines(symbol, interval, startTime, endTime, limit)
		}
	}

	if resp.StatusCode != 200 {
		return nil, fmt.Errorf("API request failed with status code %d: %s", resp.StatusCode, resp.Status)
	}

	var klines [][]interface{}
	err = json.NewDecoder(resp.Body).Decode(&klines)
	if err != nil {
		return nil, err
	}

	result := make([]Kline, len(klines))
	for i, kline := range klines {
		result[i].OpenTime = kline[0].(float64)
		result[i].OpenPrice = kline[1].(string)
		result[i].HighPrice = kline[2].(string)
		result[i].LowPrice = kline[3].(string)
		result[i].ClosePrice = kline[4].(string)
		result[i].Volume = kline[5].(string)
		result[i].CloseTime = kline[6].(float64)
		result[i].QuoteAssetVolume = kline[7].(string)
		result[i].NumberOfTrades = kline[8].(float64)
		result[i].TakerBuyBaseAssetVolume = kline[9].(string)
		result[i].TakerBuyQuoteAssetVolume = kline[10].(string)
	}

	return result, nil
}
