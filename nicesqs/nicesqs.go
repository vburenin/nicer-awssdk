package nicesqs

import (
	"strconv"

	"sync"

	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/vburenin/nicer-awssdk/nicetools"
)

// DefaultGoroutineLimit is a default limit for a number of
// concurrent goroutines used for batched operations.
const DefaultGoroutineLimit = 20

// NicerSQS is SQS service wrapper on top of AWS SDK
// Provides common operations with the queue.
type NicerSQS struct {
	// AWS SQS object. Can be used directly if necessary.
	Sqs *sqs.SQS
	// Discovered SQS queue URL.
	queueUrl *string
	// Number of goroutines that can run per batch operation.
	goroutineLimit int
}

func (this *NicerSQS) SetGoroutineLimit(limit int) *NicerSQS {
	this.goroutineLimit = limit
	return this
}

// SimpleMessage is a pointer-less SQS Message structure.
type SimpleMessage struct {
	Id            string
	ReceiptHandle string
	Body          string
	BodyMd5       string
	Attributes    map[string]string
	AttributesMd5 string
}

type SendError struct {
	// An error code representing why the action failed on this entry.
	Code string
	// The id of an entry in a batch request.
	Id string
	// A message explaining why the action failed on this entry.
	Message string
	// Whether the error happened due to the sender's fault.
	SenderFault bool
}

// Connect connects to SQS discovering SQS URL for a given queue name.
func Connect(session *session.Session, queueName string) (*NicerSQS, error) {
	s := sqs.New(session)

	gq := &sqs.GetQueueUrlInput{
		QueueName: &queueName,
	}
	resp, err := s.GetQueueUrl(gq)

	if err != nil {
		return nil, err
	}
	psqs := &NicerSQS{
		Sqs:            s,
		queueUrl:       resp.QueueUrl,
		goroutineLimit: DefaultGoroutineLimit,
	}
	return psqs, nil
}

// GetMessagesLimit receives up to limited number of messages.
func (this *NicerSQS) GetMessagesLimit(limit int64) ([]*SimpleMessage, error) {
	return this.GetMessages(limit, -1, -1)
}

// GetMessagesLimitWait receives up to limited number of messages waiting up to waitTimeout seconds.
func (this *NicerSQS) GetMessagesLimitWait(limit, waitTimeout int64) ([]*SimpleMessage, error) {
	return this.GetMessages(limit, -1, waitTimeout)
}

// GetMessages queries queue for messages.
// The parameters are ignored if their value is less than 0.
func (this *NicerSQS) GetMessages(limit, visibilityTimeout, waitTimeout int64) ([]*SimpleMessage, error) {
	var msgs []*SimpleMessage
	rmi := &sqs.ReceiveMessageInput{
		QueueUrl: this.queueUrl,
	}

	if limit >= 0 {
		rmi.MaxNumberOfMessages = aws.Int64(limit)
	}
	if visibilityTimeout >= 0 {
		rmi.VisibilityTimeout = aws.Int64(visibilityTimeout)
	}
	if waitTimeout >= 0 {
		rmi.WaitTimeSeconds = aws.Int64(waitTimeout)
	}
	if out, err := this.Sqs.ReceiveMessage(rmi); err != nil {
		return nil, err
	} else {
		for _, m := range out.Messages {
			msgs = append(msgs, &SimpleMessage{
				Id:            *m.MessageId,
				ReceiptHandle: *m.ReceiptHandle,
				Body:          *m.Body,
				BodyMd5:       *m.MD5OfBody,
				Attributes:    aws.StringValueMap(m.Attributes),
			})
		}
		return msgs, nil
	}
}

// DeleteMessageBatch deletes a batch of messages.
// All operations are batched in a way to match AWS limit.
// So more than 10 messages can be passed in.
func (this *NicerSQS) DeleteMessageBatch(msgs []*SimpleMessage) (success []string, failed []*SendError) {
	var lock sync.Mutex
	var callWaitGroup sync.WaitGroup
	var runCnt int

	f := func(messages []*SimpleMessage) {
		mbi := &sqs.DeleteMessageBatchInput{
			QueueUrl: this.queueUrl,
		}
		for _, m := range messages {
			dm := &sqs.DeleteMessageBatchRequestEntry{
				Id:            &m.Id,
				ReceiptHandle: &m.ReceiptHandle,
			}
			mbi.Entries = append(mbi.Entries, dm)
		}

		out, lerr := this.Sqs.DeleteMessageBatch(mbi)
		lock.Lock()
		runCnt--
		for _, e := range out.Failed {
			failed = append(failed, &SendError{
				Id:          *e.Id,
				Code:        *e.Code,
				Message:     *e.Message,
				SenderFault: *e.SenderFault,
			})
		}
		for _, s := range out.Successful {
			success = append(success, *s.Id)
		}
		if lerr != nil {
			for _, m := range messages {
				failed = append(failed, &SendError{
					Code:        "LocalError",
					Id:          m.Id,
					Message:     lerr.Error(),
					SenderFault: false,
				})
			}
		}
		lock.Unlock()
	}
	for {
		for runCnt >= this.goroutineLimit {
			time.Sleep(time.Millisecond)
		}
		if len(msgs) > 10 {
			callWaitGroup.Add(1)
			lock.Lock()
			runCnt++
			lock.Unlock()
			go f(msgs[:10])
			msgs = msgs[10:]
		} else {
			callWaitGroup.Add(1)
			lock.Lock()
			runCnt++
			lock.Unlock()
			go f(msgs)
			break
		}
	}
	callWaitGroup.Wait()
	return success, failed
}

// DeleteMessage deletes just one message.
func (this *NicerSQS) DeleteMessage(msg *SimpleMessage) error {
	_, err := this.Sqs.DeleteMessage(&sqs.DeleteMessageInput{
		QueueUrl:      this.queueUrl,
		ReceiptHandle: &msg.ReceiptHandle,
	})
	return err
}

// ChangeVisibilityTimeout changes message visibility timeout.
func (this *NicerSQS) ChangeVisibilityTimeout(msg *SimpleMessage, visibilityTimeout int64) error {
	ci := &sqs.ChangeMessageVisibilityInput{
		QueueUrl:          this.queueUrl,
		VisibilityTimeout: aws.Int64(visibilityTimeout),
		ReceiptHandle:     &msg.ReceiptHandle,
	}
	_, err := this.Sqs.ChangeMessageVisibility(ci)
	return err
}

// SendMessage simply sends message body as SQS message.
func (this *NicerSQS) SendMessage(body string) (*SimpleMessage, error) {
	smi := &sqs.SendMessageInput{
		QueueUrl:    this.queueUrl,
		MessageBody: &body,
	}
	out, err := this.Sqs.SendMessage(smi)
	if out != nil {
		return &SimpleMessage{
			Id:            *out.MessageId,
			BodyMd5:       *out.MD5OfMessageBody,
			AttributesMd5: *out.MD5OfMessageAttributes,
		}, nil
	}
	return nil, err
}

// SendMessageBatch sends a batch of messages provided as array of strings.
func (this *NicerSQS) SendMessageBatch(bodies []string) (success []string, failed []*SendError) {
	var lock sync.Mutex
	var callWaitGroup sync.WaitGroup
	var runCnt int
	var batchNum int

	f := func(bn int, b []string) {
		smi := &sqs.SendMessageBatchInput{
			QueueUrl: this.queueUrl,
		}
		for idx, b := range b {
			strIdx := strconv.Itoa(bn*10 + idx)
			re := &sqs.SendMessageBatchRequestEntry{
				Id:          aws.String(strIdx),
				MessageBody: aws.String(b),
			}
			smi.Entries = append(smi.Entries, re)
		}
		res, lerr := this.Sqs.SendMessageBatch(smi)
		callWaitGroup.Done()

		lock.Lock()
		runCnt--
		for _, s := range res.Successful {
			success = append(success, *s.MessageId)
		}
		for _, f := range res.Failed {
			failed = append(failed, &SendError{
				Id:          *f.Id,
				Code:        *f.Code,
				Message:     *f.Message,
				SenderFault: *f.SenderFault,
			})
		}
		if lerr != nil {
			for _, e := range smi.Entries {
				failed = append(failed, &SendError{
					Code:        "LocalError",
					Id:          *e.Id,
					Message:     lerr.Error(),
					SenderFault: false,
				})
			}
		}
		lock.Unlock()
	}

	for {
		for runCnt >= this.goroutineLimit {
			time.Sleep(time.Millisecond)
		}
		if len(bodies) > 10 {
			callWaitGroup.Add(1)
			lock.Lock()
			runCnt++
			lock.Unlock()
			go f(batchNum, bodies[:10])
			bodies = bodies[10:]
		} else {
			callWaitGroup.Add(1)
			lock.Lock()
			runCnt++
			lock.Unlock()
			go f(batchNum, bodies)
			break
		}
		batchNum++
	}
	callWaitGroup.Wait()
	return success, failed
}

// GetAttributes returns a list of queue attributes. All attributes are
// returned if not provided.
func (this *NicerSQS) GetAttributes(attrs []string) (map[string]string, error) {
	a := &sqs.GetQueueAttributesInput{
		QueueUrl: this.queueUrl,
	}
	if len(attrs) > 0 {
		a.AttributeNames = aws.StringSlice(attrs)
	} else {
		a.AttributeNames = []*string{aws.String("All")}
	}
	resp, err := this.Sqs.GetQueueAttributes(a)
	if err != nil {
		return nil, err
	}
	return aws.StringValueMap(resp.Attributes), nil
}

// CreateQueue creates a queue with the specific name and user defined options.
func CreateQueue(session *session.Session, queueName string, opts *SQSOptions) (*NicerSQS, error) {
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

	return &NicerSQS{
		Sqs:            s,
		queueUrl:       resp.QueueUrl,
		goroutineLimit: DefaultGoroutineLimit,
	}, nil
}

// SQSOptions options used to initialize/update SQS queue parameters.
type SQSOptions struct {
	DefaultVisibilityTimeout      int64
	DefaultMessageRetentionPeriod int64
	DefaultDelaySeconds           int64
}

func (this *SQSOptions) toOptionsMap() map[string]*string {
	optsMap := make(map[string]*string)
	if this.DefaultVisibilityTimeout > 0 {
		optsMap["VisibilityTimeout"] = nicetools.AwsIntString(this.DefaultVisibilityTimeout)
	}
	if this.DefaultDelaySeconds >= 0 {
		optsMap["DelaySeconds"] = nicetools.AwsIntString(this.DefaultDelaySeconds)
	}
	if this.DefaultMessageRetentionPeriod > 0 {
		optsMap["MessageRetentionPeriod"] = nicetools.AwsIntString(this.DefaultMessageRetentionPeriod)
	}
	return optsMap
}
