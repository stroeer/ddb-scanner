package scanner

import (
	"log"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
)

const (
	defaultLimit         = 1000
	defaultRegion        = "eu-west-1"
	defaultTotalSegments = 40
)

// Config of a Scanner
type Config struct {
	// DynamoDb client
	Svc *dynamodb.Client

	// Region of the table to scan. Defaults to eu-west-1
	Region string

	// The name of the table containing the requested items.
	TableName string

	// SegmentOffset determines where to start indexing from
	SegmentOffset int

	// SegmentCount determines the size of a segment
	SegmentCount int

	// For a parallel Scan request, TotalSegments represents the total number of
	// segments into which the Scan operation will be divided. The value of
	// TotalSegments corresponds to the number of application workers that will perform
	// the parallel scan
	TotalSegments int

	// The maximum number of items to evaluate (not necessarily the number of matching
	// items).
	Limit int32

	// A string that identifies one or more attributes to retrieve from the specified
	// table or index. These attributes can include scalars, sets, or elements of a
	// JSON document. The attributes in the expression must be separated by commas.
	ProjectionExpression string

	// One or more substitution tokens for attribute names in an expression.
	ExpressionAttributeNames map[string]string
}

func (c *Config) defaults() {
	if c.TableName == "" {
		log.Fatal("TableName is required.")
	}

	if c.Svc == nil {
		log.Fatal("DynamoDb client is required")
	}

	if c.Region == "" {
		c.Region = defaultRegion
	}

	if c.TotalSegments == 0 {
		c.TotalSegments = defaultTotalSegments
	}

	if c.SegmentCount == 0 {
		c.SegmentCount = c.TotalSegments
	}

	if c.Limit == 0 {
		c.Limit = defaultLimit
	}
}
