package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
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

type KlineDB struct {
	ID                       primitive.ObjectID `bson:"_id,omitempty"`
	OpenTime                 time.Time          `bson:"OpenTime"`
	CloseTime                time.Time          `bson:"CloseTime"`
	Open                     float64            `bson:"Open"`
	High                     float64            `bson:"High"`
	Low                      float64            `bson:"Low"`
	Close                    float64            `bson:"Close"`
	Volume                   float64            `bson:"Volume"`
	QuoteAssetVolume         float64            `bson:"QuoteAssetVolume"`
	NumberOfTrades           int64              `bson:"NumberOfTrades"`
	TakerBuyBaseAssetVolume  float64            `bson:"TakerBuyBaseAssetVolume"`
	TakerBuyQuoteAssetVolume float64            `bson:"TakerBuyQuoteAssetVolume"`
}

func main() {
	fmt.Println("What would you like to do?")
	fmt.Println("1. Fetch data")
	fmt.Println("2. Find gaps")
	fmt.Println("3. Fill gaps")

	var choice int
	fmt.Print("Enter your choice: ")
	_, err := fmt.Scanln(&choice)
	if err != nil {
		fmt.Println("Invalid choice. Please enter 1,2 or 3.")
		return
	}

	if choice == 1 {
		fetch()
	} else if choice == 2 {
		gaps()
	} else if choice == 3 {
		fillGaps()
	} else {
		fmt.Println("Invalid choice. Please enter 1,2 or 3.")
	}
}

func fillGaps() error {
	// Connect to MongoDB
	client, err := connectToDatabase()
	if err != nil {
		return err
	}
	defer client.Disconnect(context.Background())

	// Access the klines collection
	klinesCollection := client.Database("mydb").Collection("klines")

	// Find all the klines sorted by OpenTime in ascending order
	cursor, err := klinesCollection.Find(context.Background(), bson.M{}, options.Find().SetSort(bson.M{"OpenTime": 1}))
	if err != nil {
		return err
	}
	defer cursor.Close(context.Background())

	var prevKline *KlineDB
	var lastFilledKline *KlineDB
	for cursor.Next(context.Background()) {
		var kline KlineDB
		if err := cursor.Decode(&kline); err != nil {
			return err
		}
		if prevKline != nil && kline.OpenTime.Sub(prevKline.CloseTime) > time.Minute {
			// Calculate the number of 1-minute intervals in the gap
			numIntervals := int(kline.OpenTime.Sub(prevKline.CloseTime).Minutes())

			// Calculate the difference between the close prices of the two klines
			priceDiff := kline.Close - prevKline.Close

			// Calculate the price increment per interval
			priceInc := priceDiff / float64(numIntervals+1)

			// Fill the gap with linearly interpolated values
			for i := 1; i <= numIntervals; i++ {
				// Create a new KlineDB object with linearly interpolated values
				filledKline := KlineDB{
					OpenTime:  prevKline.CloseTime.Add(time.Minute * time.Duration(i)),
					CloseTime: prevKline.CloseTime.Add(time.Minute * time.Duration(i+1)),
					Open:      lastFilledKline.Close + priceInc,
					High:      lastFilledKline.Close + priceInc,
					Low:       lastFilledKline.Close + priceInc,
					Close:     lastFilledKline.Close + priceInc,
					Volume:    0,
					// Set the other fields to 0 or empty, as appropriate
					QuoteAssetVolume:         0,
					NumberOfTrades:           0,
					TakerBuyBaseAssetVolume:  0,
					TakerBuyQuoteAssetVolume: 0,
				}

				// Insert the filled kline into the collection
				_, err := klinesCollection.InsertOne(context.Background(), filledKline)
				if err != nil {
					return err
				}

				lastFilledKline = &filledKline
			}
		}

		// Insert the current kline into the collection
		_, err := klinesCollection.InsertOne(context.Background(), kline)
		if err != nil {
			return err
		}

		prevKline = &kline
		lastFilledKline = &kline
	}
	if err := cursor.Err(); err != nil {
		return err
	}

	return nil
}

func gaps() error {
	// Connect to MongoDB
	client, err := connectToDatabase()
	if err != nil {
		return err
	}
	defer client.Disconnect(context.Background())

	// Access the klines collection
	klinesCollection := client.Database("mydb").Collection("klines")

	// Find all the klines sorted by OpenTime in ascending order
	cursor, err := klinesCollection.Find(context.Background(), bson.M{}, options.Find().SetSort(bson.M{"OpenTime": 1}))
	if err != nil {
		return err
	}
	defer cursor.Close(context.Background())

	var prevKline *KlineDB
	for cursor.Next(context.Background()) {
		var kline KlineDB
		if err := cursor.Decode(&kline); err != nil {
			return err
		}
		if prevKline != nil && kline.OpenTime.Sub(prevKline.OpenTime) > time.Minute {
			fmt.Printf("Gap detected between %v and %v. Gap is %.0f minutes\n", prevKline.OpenTime, kline.OpenTime, kline.OpenTime.Sub(prevKline.OpenTime).Minutes())
		}
		prevKline = &kline
	}
	if err := cursor.Err(); err != nil {
		return err
	}

	return nil
}

func printKlineData(collection *mongo.Collection, start time.Time, end time.Time) error {
	// Query the collection for klines within the time range
	cursor, err := collection.Find(context.Background(), bson.M{"OpenTime": bson.M{"$gte": start, "$lte": end}}, options.Find().SetSort(bson.M{"OpenTime": 1}))
	if err != nil {
		return fmt.Errorf("error querying collection: %v", err)
	}
	defer cursor.Close(context.Background())

	// Iterate over the klines and print the Close value
	for cursor.Next(context.Background()) {
		var kline KlineDB
		if err := cursor.Decode(&kline); err != nil {
			return fmt.Errorf("error decoding kline: %v", err)
		}
		fmt.Println(kline)
	}
	if err := cursor.Err(); err != nil {
		return fmt.Errorf("cursor error: %v", err)
	}

	return nil
}

func fetch() {
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

		fmt.Printf("%0.2f%% complete\n", percentComplete)
	}

	// Check for missing klines
	intervalStart := startTime
	var missingKlines []Kline
	for i := 0; i < int(totalMin); i++ {
		intervalEnd := intervalStart.Add(1 * time.Minute)
		kline := Kline{OpenTime: float64(intervalStart.Unix() * 1000)}
		exists, err := klineExists(kline)
		if err != nil {
			fmt.Println(err)
			continue
		}
		if !exists {
			missingKlines = append(missingKlines, kline)
		}
		intervalStart = intervalEnd

		// Log percentage complete
		percentComplete := float64(i+1) / float64(totalMin) * 100
		fmt.Printf("%0.2f%% complete\n", percentComplete)
	}

	// Fetch missing klines with limit of 1
	for _, kline := range missingKlines {
		exists, err := klineExists(kline)
		if err != nil {
			fmt.Println(err)
			continue
		}
		if exists {
			continue
		}
		klines, err := getKlines("BTCUSDT", "1m", int64(kline.OpenTime/1000), 0, 1)
		if err != nil {
			fmt.Printf("Error fetching kline from API: %s\n", err)
			fmt.Printf("API URL: https://api.binance.com/api/v3/klines?symbol=BTCUSDT&interval=1m&startTime=%d&endTime=0&limit=1\n", int64(kline.OpenTime/1000))
			continue
		}
		if len(klines) == 0 {
			fmt.Printf("Empty response from API while fetching kline.\n")
			fmt.Printf("API URL: https://api.binance.com/api/v3/klines?symbol=BTCUSDT&interval=1m&startTime=%d&endTime=0&limit=1\n", int64(kline.OpenTime/1000))
			continue
		}
		err = saveKlines(klines)
		if err != nil {
			fmt.Println(err)
		}
	}
}

func connectToDatabase() (*mongo.Client, error) {
	// Set client options
	clientOptions := options.Client().ApplyURI("mongodb://localhost:27017")

	// Connect to MongoDB
	client, err := mongo.Connect(context.Background(), clientOptions)
	if err != nil {
		return nil, err
	}

	// Check the connection
	err = client.Ping(context.Background(), nil)
	if err != nil {
		return nil, err
	}

	return client, nil
}

func klineExists(kline Kline) (bool, error) {
	// Connect to MongoDB
	client, err := connectToDatabase()
	if err != nil {
		return false, err
	}
	defer client.Disconnect(context.Background())

	// Access the klines collection
	klinesCollection := client.Database("mydb").Collection("klines")

	// Check if the kline already exists in the database
	filter := bson.M{"OpenTime": time.Unix(int64(kline.OpenTime/1000), 0)}
	count, err := klinesCollection.CountDocuments(context.Background(), filter)
	if err != nil {
		return false, err
	}

	return count > 0, nil
}

type gap struct {
	startTime time.Time
	endTime   time.Time
	duration  time.Duration
}

type gapInfo struct {
	numGaps int
	gaps    []gap
}

func findTimeSeriesGaps(startTime time.Time, endTime time.Time, interval time.Duration) (*gapInfo, error) {
	clientOptions := options.Client().ApplyURI("mongodb://localhost:27017")
	client, err := mongo.Connect(context.Background(), clientOptions)
	if err != nil {
		return nil, err
	}
	defer client.Disconnect(context.Background())

	klinesCollection := client.Database("mydb").Collection("klines")
	cursor, err := klinesCollection.Find(context.Background(), bson.M{"OpenTime": bson.M{"$gte": startTime, "$lte": endTime}}, options.Find().SetSort(bson.M{"OpenTime": 1}))
	if err != nil {
		return nil, err
	}
	defer cursor.Close(context.Background())

	gaps := []gap{}
	var numGaps int
	var lastEndTime time.Time

	for cursor.Next(context.Background()) {
		kline := Kline{}
		err := cursor.Decode(&kline)
		if err != nil {
			return nil, err
		}

		klineTime := time.Unix(int64(kline.OpenTime/1000), 0)

		if !lastEndTime.IsZero() && klineTime.Sub(lastEndTime) > interval {
			gaps = append(gaps, gap{
				startTime: lastEndTime,
				endTime:   klineTime,
				duration:  klineTime.Sub(lastEndTime),
			})
			numGaps++
		}

		lastEndTime = time.Unix(int64(kline.CloseTime/1000), 0)
	}

	if err := cursor.Err(); err != nil {
		return nil, err
	}

	if len(gaps) == 0 {
		return &gapInfo{numGaps: 0, gaps: gaps}, nil
	}

	return &gapInfo{numGaps: numGaps, gaps: gaps}, nil
}

var mu sync.Mutex

func saveKlines(klines []Kline) error {
	if len(klines) == 0 {
		return nil
	}

	// Acquire the lock before accessing the database
	mu.Lock()
	defer mu.Unlock()

	// Connect to MongoDB
	client, err := connectToDatabase()
	if err != nil {
		return err
	}
	defer client.Disconnect(context.Background())

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

		// Check if the kline already exists in the database
		existingKline := Kline{}
		err := klinesCollection.FindOne(context.Background(), bson.M{"OpenTime": time.Unix(int64(kline.OpenTime/1000), 0)}).Decode(&existingKline)
		if err == nil {
			continue // Kline already exists, skip inserting it
		}

		// Ensure that the OpenTime is a whole minute without any seconds
		if int64(kline.OpenTime)%60000 != 0 {
			return fmt.Errorf("OpenTime must be a whole minute without any seconds")
		}

		// Convert fields to float values
		openPrice, err := strconv.ParseFloat(kline.OpenPrice, 64)
		if err != nil {
			return err
		}
		highPrice, err := strconv.ParseFloat(kline.HighPrice, 64)
		if err != nil {
			return err
		}
		lowPrice, err := strconv.ParseFloat(kline.LowPrice, 64)
		if err != nil {
			return err
		}
		closePrice, err := strconv.ParseFloat(kline.ClosePrice, 64)
		if err != nil {
			return err
		}
		volume, err := strconv.ParseFloat(kline.Volume, 64)
		if err != nil {
			return err
		}
		quoteAssetVolume, err := strconv.ParseFloat(kline.QuoteAssetVolume, 64)
		if err != nil {
			return err
		}
		takerBuyBaseAssetVolume, err := strconv.ParseFloat(kline.TakerBuyBaseAssetVolume, 64)
		if err != nil {
			return err
		}
		takerBuyQuoteAssetVolume, err := strconv.ParseFloat(kline.TakerBuyQuoteAssetVolume, 64)
		if err != nil {
			return err
		}

		doc := bson.D{
			{Key: "OpenTime", Value: time.Unix(int64(kline.OpenTime/1000), 0)},
			{Key: "CloseTime", Value: time.Unix(int64(kline.CloseTime/1000), 0)},
			{Key: "Open", Value: openPrice},
			{Key: "High", Value: highPrice},
			{Key: "Low", Value: lowPrice},
			{Key: "Close", Value: closePrice},
			{Key: "Volume", Value: volume},
			{Key: "QuoteAssetVolume", Value: quoteAssetVolume},
			{Key: "NumberOfTrades", Value: kline.NumberOfTrades},
			{Key: "TakerBuyBaseAssetVolume", Value: takerBuyBaseAssetVolume},
			{Key: "TakerBuyQuoteAssetVolume", Value: takerBuyQuoteAssetVolume},
		}

		documents = append(documents, doc)
	}

	// If there are no new documents to insert, return nil
	if len(documents) == 0 {
		return nil
	}

	// Insert the measurement documents into the time series collection
	opts := options.InsertMany().SetOrdered(false) // Set ordered to false to allow parallel inserts
	_, err = klinesCollection.InsertMany(context.Background(), documents, opts)
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
