# ddb-scanner

Package for scanning DynamoDb tables in parallel. This is an updated version of https://github.com/clearbit/go-ddb

## install

```shell
go get github.com/stroeer/ddb-scanner
```

## use

```go
const (
	region = "eu-west-1"
)

type item struct {
	Pk string
}

func main() {
	cfg, err := config.LoadDefaultConfig(context.TODO(), config.WithRegion(region))

	if err != nil {
		log.Fatal(err)
	}

	sc := scanner.New(scanner.Config{
		Svc:                  dynamodb.NewFromConfig(cfg),
		ProjectionExpression: aws.String("pk"),
		Region:               aws.String(region),
		TableName:            aws.String("test"),
		TotalSegments:        aws.Int32(20),
	})

	start := time.Now()
	sc.Start(scanner.HandlerFunc(func(ddbItems []map[string]types.AttributeValue) {
		var items []item
		err := attributevalue.UnmarshalListOfMaps(ddbItems, &items)
		if err != nil {
			log.Fatalf("failed to unmarshal ddbItems, %v", err)
		}
		for _, i := range items {
			log.Printf("pk: %s\n", i.Pk)
		}
	}))
	log.Printf("table scan finished - count: %d, segments: %d, duration: %s\n", sc.CompletedItems.Value(), sc.CompletedSegments.Value(), time.Since(start))
}

```
