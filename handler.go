package scanner

import (
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

type Handler interface {
	Handle(items []map[string]types.AttributeValue)
}

type HandlerFunc func(items []map[string]types.AttributeValue)

func (h HandlerFunc) Handle(items []map[string]types.AttributeValue) {
	h(items)
}
