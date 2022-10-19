package scanner

import (
	"log"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/smithy-go/ptr"
)

const (
	defaultLimit         = 1000
	defaultRegion        = "eu-west-1"
	defaultTotalSegments = 40
)

// Config of a Scanner
type Config struct {
	// One or more substitution tokens for attribute names in an expression. The
	// following are some use cases for using ExpressionAttributeNames:
	//
	// * To access an
	// attribute whose name conflicts with a DynamoDB reserved word.
	//
	// * To create a
	// placeholder for repeating occurrences of an attribute name in an expression.
	//
	// *
	// To prevent special characters in an attribute name from being misinterpreted in
	// an expression.
	//
	// Use the # character in an expression to dereference an attribute
	// name. For example, consider the following attribute name:
	//
	// * Percentile
	//
	// The
	// name of this attribute conflicts with a reserved word, so it cannot be used
	// directly in an expression. (For the complete list of reserved words, see
	// Reserved Words
	// (https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/ReservedWords.html)
	// in the Amazon DynamoDB Developer Guide). To work around this, you could specify
	// the following for ExpressionAttributeNames:
	//
	// * {"#P":"Percentile"}
	//
	// You could
	// then use this substitution in an expression, as in this example:
	//
	// * #P =
	// :val
	//
	// Tokens that begin with the : character are expression attribute values,
	// which are placeholders for the actual value at runtime. For more information on
	// expression attribute names, see Specifying Item Attributes
	// (https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Expressions.AccessingItemAttributes.html)
	// in the Amazon DynamoDB Developer Guide.
	ExpressionAttributeNames map[string]string

	// One or more values that can be substituted in an expression. Use the : (colon)
	// character in an expression to dereference an attribute value. For example,
	// suppose that you wanted to check whether the value of the ProductStatus
	// attribute was one of the following: Available | Backordered | Discontinued You
	// would first need to specify ExpressionAttributeValues as follows: {
	// ":avail":{"S":"Available"}, ":back":{"S":"Backordered"},
	// ":disc":{"S":"Discontinued"} } You could then use these values in an expression,
	// such as this: ProductStatus IN (:avail, :back, :disc) For more information on
	// expression attribute values, see Condition Expressions
	// (https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Expressions.SpecifyingConditions.html)
	// in the Amazon DynamoDB Developer Guide.
	ExpressionAttributeValues map[string]types.AttributeValue

	// A string that contains conditions that DynamoDB applies after the Scan
	// operation, but before the data is returned to you. Items that do not satisfy the
	// FilterExpression criteria are not returned. A FilterExpression is applied after
	// the items have already been read; the process of filtering does not consume any
	// additional read capacity units. For more information, see Filter Expressions
	// (https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/QueryAndScan.html#Query.FilterExpression)
	// in the Amazon DynamoDB Developer Guide.
	FilterExpression *string

	// The maximum number of items to evaluate (not necessarily the number of matching
	// items).
	Limit *int32

	// A string that identifies one or more attributes to retrieve from the specified
	// table or index. These attributes can include scalars, sets, or elements of a
	// JSON document. The attributes in the expression must be separated by commas. If
	// no attribute names are specified, then all attributes will be returned. If any
	// of the requested attributes are not found, they will not appear in the result.
	// For more information, see Specifying Item Attributes
	// (https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Expressions.AccessingItemAttributes.html)
	// in the Amazon DynamoDB Developer Guide.
	ProjectionExpression *string

	// Region of the table to scan. Defaults to eu-west-1
	Region string

	// SegmentOffset determines where to start indexing from
	SegmentOffset int

	// SegmentCount determines the size of a segment
	SegmentCount int

	// DynamoDb client
	Svc *dynamodb.Client

	// The name of the table containing the requested items.
	TableName *string

	// For a parallel Scan request, TotalSegments represents the total number of
	// segments into which the Scan operation will be divided. The value of
	// TotalSegments corresponds to the number of application workers that will perform
	// the parallel scan
	TotalSegments *int32
}

func (c *Config) defaults() {
	if c.TableName == nil {
		log.Fatal("TableName is required.")
	}

	if c.Svc == nil {
		log.Fatal("DynamoDb client is required")
	}

	if c.Region == "" {
		c.Region = defaultRegion
	}

	if c.TotalSegments == nil {
		c.TotalSegments = ptr.Int32(defaultTotalSegments)
	}

	if c.SegmentCount == 0 {
		c.SegmentCount = int(*c.TotalSegments)
	}

	if c.Limit == nil {
		c.Limit = ptr.Int32(defaultLimit)
	}
}
