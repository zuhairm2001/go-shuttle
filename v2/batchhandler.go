package shuttle

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/messaging/azservicebus"
)

const (
	defaultMaxBatchSize  = 10
	defaultFlushInterval = time.Second
)

// BatchHandler handles an ordered batch of received messages. Messages must be
// settled individually; Azure Service Bus does not provide atomic settlement
// for a received batch.
type BatchHandler interface {
	Handle(context.Context, MessageSettler, []*azservicebus.ReceivedMessage)
}

// BatchHandlerFunc adapts a function to BatchHandler.
type BatchHandlerFunc func(context.Context, MessageSettler, []*azservicebus.ReceivedMessage)

func (f BatchHandlerFunc) Handle(
	ctx context.Context,
	settler MessageSettler,
	messages []*azservicebus.ReceivedMessage,
) {
	f(ctx, settler, messages)
}

// BatchHandlerOptions configures count-, time-, and key-based batching.
type BatchHandlerOptions struct {
	// MaxBatchSize flushes a group when it contains this many messages.
	// Values less than one use the default of 10.
	MaxBatchSize int
	// FlushInterval flushes a non-empty group this long after its first message
	// arrives. Values less than or equal to zero use the default of one second.
	FlushInterval time.Duration
	// GroupBy returns the batching key for a message. A nil function puts all
	// messages in one group.
	GroupBy func(*azservicebus.ReceivedMessage) string
}

// ValidateProcessorOptions verifies that a processor has enough concurrency
// for a count-triggered batch to fill. All buffered messages occupy processor
// concurrency slots until their batch finishes.
func (o *BatchHandlerOptions) ValidateProcessorOptions(processorOptions *ProcessorOptions) error {
	batchOptions := applyBatchHandlerOptions(o)
	options := applyProcessorOptions(processorOptions)
	if options.MaxConcurrency < batchOptions.MaxBatchSize {
		return fmt.Errorf(
			"processor MaxConcurrency (%d) must be at least batch MaxBatchSize (%d)",
			options.MaxConcurrency,
			batchOptions.MaxBatchSize,
		)
	}
	return nil
}

type batchItem struct {
	ctx     context.Context
	settler MessageSettler
	message *azservicebus.ReceivedMessage
}

type pendingBatch struct {
	items       []*batchItem
	ready       chan struct{}
	done        chan struct{}
	readyOnce   sync.Once
	executeOnce sync.Once
	timer       *time.Timer
}

type batchGroup struct {
	current   *pendingBatch
	execution chan struct{}
	refs      int
}

type batchingHandler struct {
	next    BatchHandler
	options BatchHandlerOptions

	mu     sync.Mutex
	groups map[string]*batchGroup
}

func applyBatchHandlerOptions(options *BatchHandlerOptions) BatchHandlerOptions {
	applied := BatchHandlerOptions{
		MaxBatchSize:  defaultMaxBatchSize,
		FlushInterval: defaultFlushInterval,
	}
	if options == nil {
		return applied
	}
	if options.MaxBatchSize > 0 {
		applied.MaxBatchSize = options.MaxBatchSize
	}
	if options.FlushInterval > 0 {
		applied.FlushInterval = options.FlushInterval
	}
	applied.GroupBy = options.GroupBy
	return applied
}

// NewBatchHandler creates middleware that groups messages and invokes handler
// when a group reaches MaxBatchSize or FlushInterval elapses.
//
// Place lock-renewal middleware outside this handler so locks remain renewed
// while messages wait for and execute as a batch.
func NewBatchHandler(options *BatchHandlerOptions, handler BatchHandler) HandlerFunc {
	b := &batchingHandler{
		next:    handler,
		options: applyBatchHandlerOptions(options),
		groups:  make(map[string]*batchGroup),
	}
	return b.Handle
}

func (h *batchingHandler) Handle(
	ctx context.Context,
	settler MessageSettler,
	message *azservicebus.ReceivedMessage,
) {
	if ctx.Err() != nil {
		return
	}

	key := ""
	if h.options.GroupBy != nil {
		key = h.options.GroupBy(message)
	}

	item := &batchItem{ctx: ctx, settler: settler, message: message}
	group, batch := h.enqueue(key, item)

	select {
	case <-ctx.Done():
		if h.removePending(key, group, batch, item) {
			return
		}
	case <-batch.ready:
	}

	defer h.release(key, group)
	h.execute(group, batch)

	select {
	case <-batch.done:
	case <-ctx.Done():
	}
}

func (h *batchingHandler) enqueue(key string, item *batchItem) (*batchGroup, *pendingBatch) {
	h.mu.Lock()
	defer h.mu.Unlock()

	group := h.groups[key]
	if group == nil {
		group = &batchGroup{execution: make(chan struct{}, 1)}
		group.execution <- struct{}{}
		h.groups[key] = group
	}

	batch := group.current
	if batch == nil {
		batch = &pendingBatch{
			ready: make(chan struct{}),
			done:  make(chan struct{}),
		}
		group.current = batch
		batch.timer = time.AfterFunc(h.options.FlushInterval, func() {
			h.markReady(group, batch)
		})
	}

	batch.items = append(batch.items, item)
	group.refs++
	if len(batch.items) >= h.options.MaxBatchSize {
		h.markReadyLocked(group, batch)
	}
	return group, batch
}

func (h *batchingHandler) markReady(group *batchGroup, batch *pendingBatch) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.markReadyLocked(group, batch)
}

func (h *batchingHandler) markReadyLocked(group *batchGroup, batch *pendingBatch) {
	if group.current != batch || len(batch.items) == 0 {
		return
	}
	group.current = nil
	if batch.timer != nil {
		batch.timer.Stop()
	}
	batch.readyOnce.Do(func() {
		close(batch.ready)
	})
}

func (h *batchingHandler) execute(group *batchGroup, batch *pendingBatch) {
	batch.executeOnce.Do(func() {
		defer close(batch.done)
		if len(batch.items) == 0 {
			return
		}

		var ctx context.Context
		for {
			item := liveBatchItem(batch.items)
			if item == nil {
				return
			}
			ctx = item.ctx
			select {
			case <-ctx.Done():
				continue
			case <-group.execution:
			}
			break
		}
		defer func() {
			group.execution <- struct{}{}
		}()

		liveItem := liveBatchItem(batch.items)
		if liveItem == nil {
			return
		}
		ctx = liveItem.ctx

		messages := make([]*azservicebus.ReceivedMessage, len(batch.items))
		for i, item := range batch.items {
			messages[i] = item.message
		}
		h.next.Handle(ctx, liveItem.settler, messages)
	})
}

func liveBatchItem(items []*batchItem) *batchItem {
	for _, item := range items {
		if item.ctx.Err() == nil {
			return item
		}
	}
	return nil
}

func (h *batchingHandler) removePending(
	key string,
	group *batchGroup,
	batch *pendingBatch,
	item *batchItem,
) bool {
	h.mu.Lock()
	defer h.mu.Unlock()

	if group.current != batch {
		return false
	}
	for i, candidate := range batch.items {
		if candidate != item {
			continue
		}
		batch.items = append(batch.items[:i], batch.items[i+1:]...)
		group.refs--
		if len(batch.items) == 0 {
			batch.timer.Stop()
			group.current = nil
			if group.refs == 0 {
				delete(h.groups, key)
			}
		}
		return true
	}
	return false
}

func (h *batchingHandler) release(key string, group *batchGroup) {
	h.mu.Lock()
	defer h.mu.Unlock()

	group.refs--
	if group.refs == 0 && group.current == nil {
		delete(h.groups, key)
	}
}

// BatchSettler handles a batch and returns one settlement for each input
// message, in matching order.
type BatchSettler interface {
	Handle(context.Context, []*azservicebus.ReceivedMessage) []Settlement
}

// BatchSettlerFunc adapts a function to BatchSettler.
type BatchSettlerFunc func(context.Context, []*azservicebus.ReceivedMessage) []Settlement

func (f BatchSettlerFunc) Handle(
	ctx context.Context,
	messages []*azservicebus.ReceivedMessage,
) []Settlement {
	return f(ctx, messages)
}

// BatchSettlementHandlerOptions configures invalid settlement handling.
type BatchSettlementHandlerOptions struct {
	// OnInvalidSettlements is called when the handler returns a different
	// number of settlements than messages, or returns a nil settlement.
	// The default behavior is to panic. A replacement must contain exactly one
	// non-nil settlement per message.
	OnInvalidSettlements func(
		context.Context,
		[]*azservicebus.ReceivedMessage,
		[]Settlement,
	) []Settlement
}

// NewBatchSettlementHandler applies one settlement to each batch message.
// Settlements are independent, so a batch may be only partially settled.
func NewBatchSettlementHandler(
	options *BatchSettlementHandlerOptions,
	handler BatchSettler,
) BatchHandlerFunc {
	onInvalid := func(
		_ context.Context,
		messages []*azservicebus.ReceivedMessage,
		settlements []Settlement,
	) []Settlement {
		if len(settlements) != len(messages) {
			panic(fmt.Sprintf(
				"batch handler returned %d settlements for %d messages",
				len(settlements),
				len(messages),
			))
		}
		panic("batch handler returned a nil settlement")
	}
	if options != nil && options.OnInvalidSettlements != nil {
		onInvalid = options.OnInvalidSettlements
	}

	return func(
		ctx context.Context,
		settler MessageSettler,
		messages []*azservicebus.ReceivedMessage,
	) {
		settlements := handler.Handle(ctx, messages)
		if !validBatchSettlements(messages, settlements) {
			settlements = onInvalid(ctx, messages, settlements)
		}
		if !validBatchSettlements(messages, settlements) {
			panic("OnInvalidSettlements must return one non-nil settlement per message")
		}
		for i, settlement := range settlements {
			settlement.Settle(ctx, settler, messages[i])
		}
	}
}

func validBatchSettlements(
	messages []*azservicebus.ReceivedMessage,
	settlements []Settlement,
) bool {
	if len(messages) != len(settlements) {
		return false
	}
	for _, settlement := range settlements {
		if settlement == nil {
			return false
		}
	}
	return true
}
