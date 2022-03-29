package scanner

import (
	"context"
	"expvar"
	"log"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/jpillora/backoff"
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
		go s.scan(segment, handler)
	}
	s.waitGroup.Wait()
}

func (s *Scanner) scan(segment int, handler Handler) {
	defer s.waitGroup.Done()

	bk := &backoff.Backoff{
		Max:    5 * time.Minute,
		Jitter: true,
	}

	var lastEvaluatedKey map[string]types.AttributeValue
	for {
		resp, err := s.Svc.Scan(context.TODO(), &dynamodb.ScanInput{
			ExclusiveStartKey:        lastEvaluatedKey,
			ExpressionAttributeNames: s.ExpressionAttributeNames,
			Limit:                    aws.Int32(s.Limit),
			ProjectionExpression:     aws.String(s.ProjectionExpression),
			Segment:                  aws.Int32(int32(segment)),
			TableName:                aws.String(s.TableName),
			TotalSegments:            aws.Int32(int32(s.TotalSegments)),
		})

		if err != nil {
			log.Printf("scan operation failed- backing off. %v\n", err)
			time.Sleep(bk.Duration())
			continue
		}
		bk.Reset()

		handler.Handle(resp.Items)

		lastEvaluatedKey = resp.LastEvaluatedKey
		s.CompletedItems.Add(int64(resp.Count))

		if resp.LastEvaluatedKey == nil {
			s.CompletedSegments.Add(1)
			break
		}
	}
}
