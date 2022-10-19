package scanner

import (
	"context"
	"expvar"
	"log"
	"sync"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/smithy-go/ptr"
)

func New(cfg Config) *Scanner {
	cfg.defaults()

	return &Scanner{
		waitGroup:         &sync.WaitGroup{},
		Config:            cfg,
		CompletedSegments: expvar.NewInt("scanner.CompletedSegments"),
		CompletedItems:    expvar.NewInt("scanner.CompletedItems"),
	}
}

type Scanner struct {
	Config
	waitGroup         *sync.WaitGroup
	CompletedSegments *expvar.Int
	CompletedItems    *expvar.Int
}

// Start starts the parallel full table scan and blocks until all segments have been processed.
func (s *Scanner) Start(handler Handler) {
	for i := 0; i < s.SegmentCount; i++ {
		s.waitGroup.Add(1)
		segment := (s.SegmentCount * s.SegmentOffset) + i
		go s.scan(ptr.Int32(int32(segment)), handler)
	}
	s.waitGroup.Wait()
}

func (s *Scanner) scan(segment *int32, handler Handler) {
	defer s.waitGroup.Done()

	var lastEvaluatedKey map[string]types.AttributeValue
	for {
		resp, err := s.Svc.Scan(context.TODO(), &dynamodb.ScanInput{
			ExclusiveStartKey:         lastEvaluatedKey,
			ExpressionAttributeNames:  s.ExpressionAttributeNames,
			ExpressionAttributeValues: s.ExpressionAttributeValues,
			FilterExpression:          s.FilterExpression,
			Limit:                     s.Limit,
			ProjectionExpression:      s.ProjectionExpression,
			Segment:                   segment,
			TableName:                 s.TableName,
			TotalSegments:             s.TotalSegments,
		})

		if err != nil {
			log.Fatalf("scan operation failed: %v", err)
		}

		handler.Handle(resp.Items)

		lastEvaluatedKey = resp.LastEvaluatedKey
		s.CompletedItems.Add(int64(resp.Count))

		if resp.LastEvaluatedKey == nil {
			s.CompletedSegments.Add(1)
			break
		}
	}
}
