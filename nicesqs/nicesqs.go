package nicesqs

import (
	"strconv"
	"sync"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/vburenin/nicer-awssdk/nicetools"
	"github.com/vburenin/nsync"
)

// NiceSQS is SQS service wrapper on top of AWS SDK
// Provides common operations with the queue.
type NiceSQS struct {
	// AWS SQS object. Can be used directly if necessary.
	Sqs *sqs.SQS
	// Discovered SQS queue URL.
	queueUrl *string
	// Number of goroutines that can run in parallel.
	sema *nsync.Semaphore
}

// SetParallelLimit sets a goroutine limit that can be used to
// do concurrent SQS requests.
// If you change a limit, make sure there are no other goroutines
// running, otherwise it will cause panic.
func (this *NiceSQS) SetParallelLimit(limit int) *NiceSQS {
	this.sema = nsync.NewSemaphore(limit)
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

// SendError is simple error message that is returned to the caller.
type SendError struct {
	// An error code representing why the action failed on this entry.
	Code string
	// The id of an entry in a batch request.
	Id string
	// A message explaining why the action failed on this entry.
	ErrorDescription string
	// Whether the error happened due to the sender's fault.
	SenderFault bool
}

// GetMessagesLimit receives up to limited number of messages.
func (this *NiceSQS) GetMessagesLimit(limit int64) ([]*SimpleMessage, error) {
	return this.GetMessages(limit, -1, -1)
}

// GetMessagesLimitWait receives up to limited number of messages waiting up to waitTimeout seconds.
func (this *NiceSQS) GetMessagesLimitWait(limit, waitTimeout int64) ([]*SimpleMessage, error) {
	return this.GetMessages(limit, -1, waitTimeout)
}

// GetMessages queries queue for messages.
// The parameters are ignored if their value is less than 0.
func (this *NiceSQS) GetMessages(limit, visibilityTimeout, waitTimeout int64) ([]*SimpleMessage, error) {
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

func (this *NiceSQS) DeleteBatchByReceiptHandles(handles []string) (success []string, failed []*SendError) {
	var msgs []*SimpleMessage
	for idx, h := range handles {
		msgs = append(msgs, &SimpleMessage{
			Id:            strconv.Itoa(idx),
			ReceiptHandle: h,
		})
	}
	return this.DeleteMessageBatch(msgs)
}

// DeleteMessageBatch deletes a batch of messages.
// All operations are batched in a way to match AWS limit.
// So more than 10 messages can be passed in.
func (this *NiceSQS) DeleteMessageBatch(msgs []*SimpleMessage) (success []string, failed []*SendError) {
	var lock sync.Mutex
	var callWaitGroup sync.WaitGroup
	if len(msgs) == 0 {
		return success, failed
	}
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
		for _, e := range out.Failed {
			failed = append(failed, &SendError{
				Id:               *e.Id,
				Code:             *e.Code,
				ErrorDescription: *e.Message,
				SenderFault:      *e.SenderFault,
			})
		}
		for _, s := range out.Successful {
			success = append(success, *s.Id)
		}
		if lerr != nil {
			for _, m := range messages {
				failed = append(failed, &SendError{
					Code:             "LocalError",
					Id:               m.Id,
					ErrorDescription: lerr.Error(),
					SenderFault:      false,
				})
			}
		}
		this.sema.Release()
		lock.Unlock()
		callWaitGroup.Done()
	}
	for {
		if len(msgs) > 10 {
			this.sema.Acquire()
			callWaitGroup.Add(1)
			go f(msgs[:10])
			msgs = msgs[10:]
		} else {
			callWaitGroup.Add(1)
			this.sema.Acquire()
			go f(msgs)
			break
		}
	}
	callWaitGroup.Wait()
	return success, failed
}

// DeleteMessage deletes just one message.
func (this *NiceSQS) DeleteMessage(msg *SimpleMessage) error {
	_, err := this.Sqs.DeleteMessage(&sqs.DeleteMessageInput{
		QueueUrl:      this.queueUrl,
		ReceiptHandle: &msg.ReceiptHandle,
	})
	return err
}

// ReleaseBatchReceiptHandlers sets a visibility timeout to 0 for all provided receipt handlers.
func (this *NiceSQS) ReleaseBatchReceiptHandlers(receipts []string) (success []string, failed []*SendError) {
	msgs := make([]*SimpleMessage, 0)
	for i, r := range receipts {
		msgs = append(msgs, &SimpleMessage{
			Id:            strconv.Itoa(i),
			ReceiptHandle: r,
		})
	}
	return this.ChangeVisibilityBatch(msgs, 0)
}

// ChangeVisibilityTimeout changes message visibility timeout.
func (this *NiceSQS) ChangeVisibility(msg *SimpleMessage, visibilityTimeout int64) error {
	ci := &sqs.ChangeMessageVisibilityInput{
		QueueUrl:          this.queueUrl,
		VisibilityTimeout: aws.Int64(visibilityTimeout),
		ReceiptHandle:     &msg.ReceiptHandle,
	}
	_, err := this.Sqs.ChangeMessageVisibility(ci)
	return err
}

// ChangeVisibilityBatch changes a visibility for the bunch of messages.
func (this *NiceSQS) ChangeVisibilityBatch(
	msgs []*SimpleMessage, visibilityTimeout int64) (success []string, failed []*SendError) {
	var lock sync.Mutex
	var callWaitGroup sync.WaitGroup

	f := func(messages []*SimpleMessage) {
		batch := &sqs.ChangeMessageVisibilityBatchInput{
			QueueUrl: this.queueUrl,
		}
		for _, m := range messages {
			dm := &sqs.ChangeMessageVisibilityBatchRequestEntry{
				Id:                &m.Id,
				ReceiptHandle:     &m.ReceiptHandle,
				VisibilityTimeout: aws.Int64(visibilityTimeout),
			}
			batch.Entries = append(batch.Entries, dm)
		}

		out, lerr := this.Sqs.ChangeMessageVisibilityBatch(batch)
		lock.Lock()
		for _, e := range out.Failed {
			failed = append(failed, &SendError{
				Id:               *e.Id,
				Code:             *e.Code,
				ErrorDescription: *e.Message,
				SenderFault:      *e.SenderFault,
			})
		}
		for _, s := range out.Successful {
			success = append(success, *s.Id)
		}
		if lerr != nil {
			for _, m := range messages {
				failed = append(failed, &SendError{
					Code:             "LocalError",
					Id:               m.Id,
					ErrorDescription: lerr.Error(),
					SenderFault:      false,
				})
			}
		}
		this.sema.Release()
		lock.Unlock()
		callWaitGroup.Done()
	}
	for {
		if len(msgs) > 10 {
			callWaitGroup.Add(1)
			this.sema.Acquire()
			go f(msgs[:10])
			msgs = msgs[10:]
		} else {
			callWaitGroup.Add(1)
			this.sema.Acquire()
			go f(msgs)
			break
		}
	}
	callWaitGroup.Wait()
	return success, failed
}

// SendMessage simply sends message body as SQS message.
func (this *NiceSQS) SendMessage(body string) (*SimpleMessage, error) {
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
func (this *NiceSQS) SendMessageBatch(bodies []string) (success []string, failed []*SendError) {
	var lock sync.Mutex
	var callWaitGroup sync.WaitGroup
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

		lock.Lock()
		for _, s := range res.Successful {
			success = append(success, *s.MessageId)
		}
		for _, f := range res.Failed {
			failed = append(failed, &SendError{
				Id:               *f.Id,
				Code:             *f.Code,
				ErrorDescription: *f.Message,
				SenderFault:      *f.SenderFault,
			})
		}
		if lerr != nil {
			for _, e := range smi.Entries {
				failed = append(failed, &SendError{
					Code:             "LocalError",
					Id:               *e.Id,
					ErrorDescription: lerr.Error(),
					SenderFault:      false,
				})
			}
		}
		this.sema.Release()
		lock.Unlock()
		callWaitGroup.Done()
	}

	for {
		if len(bodies) > 10 {
			callWaitGroup.Add(1)
			this.sema.Acquire()
			go f(batchNum, bodies[:10])
			bodies = bodies[10:]
		} else {
			callWaitGroup.Add(1)
			this.sema.Acquire()
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
func (this *NiceSQS) GetAttributes(attrs []string) (map[string]string, error) {
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
