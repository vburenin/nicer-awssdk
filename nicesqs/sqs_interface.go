package nicesqs

import (
	"sync"

	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/vburenin/nsync"
)

// DefaultGoroutineLimit is a default limit for a number of
// concurrent goroutines used for batched operations.
const DefaultParallelRequestsLimit = 50

type SQSInterface interface {
	GetMessagesLimit(limit int64) ([]*SimpleMessage, error)
	GetMessagesLimitWait(limit, waitTimeout int64) ([]*SimpleMessage, error)
	GetMessages(limit, visibilityTimeout, waitTimeout int64) ([]*SimpleMessage, error)
	DeleteBatchByReceiptHandles(handles []string) (success []string, failed []*SendError)
	DeleteMessageBatch(msgs []*SimpleMessage) (success []string, failed []*SendError)
	DeleteMessage(msg *SimpleMessage) error
	ChangeVisibility(msg *SimpleMessage, visibilityTimeout int64) error
	ChangeVisibilityBatch(msgs []*SimpleMessage, visibilityTimeout int64) (success []string, failed []*SendError)
	ReleaseBatchReceiptHandlers(receipts []string) (success []string, failed []*SendError)
	SendMessage(body string) (*SimpleMessage, error)
	SendMessageBatch(bodies []string) (success []string, failed []*SendError)
	GetAttributes(attrs []string) (map[string]string, error)
}

// Interface check.
var _ SQSInterface = &NiceSQS{}

// Connect connects to SQS discovering SQS URL for a given queue name.
func Connect(session *session.Session, queueName string) (SQSInterface, error) {
	s := sqs.New(session)

	gq := &sqs.GetQueueUrlInput{
		QueueName: &queueName,
	}
	resp, err := s.GetQueueUrl(gq)

	if err != nil {
		return nil, err
	}
	psqs := &NiceSQS{
		Sqs:      s,
		queueUrl: resp.QueueUrl,
		sema:     nsync.NewSemaphore(DefaultParallelRequestsLimit),
	}
	return psqs, nil
}

// CreateQueue creates a queue with the specific name and user defined options.
func CreateQueue(session *session.Session, queueName string, opts *SQSOptions) (SQSInterface, error) {
	s := sqs.New(session)
	cqi := &sqs.CreateQueueInput{
		QueueName: &queueName,
	}
	if opts != nil {
		cqi.Attributes = opts.toOptionsMap()
	}

	resp, err := s.CreateQueue(cqi)
	if err != nil {
		return nil, err
	}

	return &NiceSQS{
		Sqs:      s,
		queueUrl: resp.QueueUrl,
		sema:     nsync.NewSemaphore(DefaultParallelRequestsLimit),
	}, nil
}

var sqsInstance map[string]SQSInterface = make(map[string]SQSInterface)
var sqsInstMutex sync.Mutex
var nameMutex = nsync.NewNamedMutex()

func CachedConnectSQSQueue(s *session.Session, queueName string) (SQSInterface, error) {
	cacheName := *s.Config.Region + "%" + queueName
	sqsInstMutex.Lock()
	if n, ok := sqsInstance[cacheName]; ok {
		sqsInstMutex.Unlock()
		return n, nil
	}
	sqsInstMutex.Unlock()

	nameMutex.Lock(cacheName)

	sqsInstMutex.Lock()
	if n, ok := sqsInstance[cacheName]; ok {
		sqsInstMutex.Unlock()
		nameMutex.Unlock(cacheName)
		return n, nil
	}
	sqsInstMutex.Unlock()

	q, err := Connect(s, queueName)
	if err != nil {
		nameMutex.Unlock(cacheName)
		return nil, err
	}
	sqsInstMutex.Lock()
	sqsInstance[cacheName] = q
	sqsInstMutex.Unlock()
	nameMutex.Unlock(cacheName)
	return q, nil
}
