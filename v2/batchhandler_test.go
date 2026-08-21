package shuttle

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore/to"
	"github.com/Azure/azure-sdk-for-go/sdk/messaging/azservicebus"
	"github.com/stretchr/testify/require"
)

type batchTestSettler struct {
	completed atomic.Int32
	abandoned atomic.Int32
	renewed   atomic.Int32
}

type batchTestReceiver struct {
	*batchTestSettler
	mu       sync.Mutex
	messages []*azservicebus.ReceivedMessage
}

func (r *batchTestReceiver) ReceiveMessages(
	ctx context.Context,
	maxMessages int,
	_ *azservicebus.ReceiveMessagesOptions,
) ([]*azservicebus.ReceivedMessage, error) {
	r.mu.Lock()
	if len(r.messages) > 0 {
		count := min(maxMessages, len(r.messages))
		messages := append([]*azservicebus.ReceivedMessage(nil), r.messages[:count]...)
		r.messages = r.messages[count:]
		r.mu.Unlock()
		return messages, nil
	}
	r.mu.Unlock()
	<-ctx.Done()
	return nil, ctx.Err()
}

func (s *batchTestSettler) AbandonMessage(context.Context, *azservicebus.ReceivedMessage, *azservicebus.AbandonMessageOptions) error {
	s.abandoned.Add(1)
	return nil
}

func (s *batchTestSettler) CompleteMessage(context.Context, *azservicebus.ReceivedMessage, *azservicebus.CompleteMessageOptions) error {
	s.completed.Add(1)
	return nil
}

func (*batchTestSettler) DeadLetterMessage(context.Context, *azservicebus.ReceivedMessage, *azservicebus.DeadLetterOptions) error {
	return nil
}

func (*batchTestSettler) DeferMessage(context.Context, *azservicebus.ReceivedMessage, *azservicebus.DeferMessageOptions) error {
	return nil
}

func (s *batchTestSettler) RenewMessageLock(context.Context, *azservicebus.ReceivedMessage, *azservicebus.RenewMessageLockOptions) error {
	s.renewed.Add(1)
	return nil
}

func batchMessage(id string) *azservicebus.ReceivedMessage {
	return &azservicebus.ReceivedMessage{MessageID: id}
}

func runBatchMessages(
	ctx context.Context,
	handler Handler,
	settler MessageSettler,
	messages ...*azservicebus.ReceivedMessage,
) <-chan struct{} {
	done := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(len(messages))
	for _, message := range messages {
		go func(message *azservicebus.ReceivedMessage) {
			defer wg.Done()
			handler.Handle(ctx, settler, message)
		}(message)
	}
	go func() {
		wg.Wait()
		close(done)
	}()
	return done
}

func TestBatchHandlerFlushesByCountInReceiveOrder(t *testing.T) {
	batches := make(chan []string, 1)
	release := make(chan struct{})
	internal := &batchingHandler{
		options: applyBatchHandlerOptions(&BatchHandlerOptions{MaxBatchSize: 3, FlushInterval: time.Minute}),
		groups:  make(map[string]*batchGroup),
		next: BatchHandlerFunc(func(_ context.Context, _ MessageSettler, messages []*azservicebus.ReceivedMessage) {
			ids := make([]string, len(messages))
			for i, message := range messages {
				ids[i] = message.MessageID
			}
			batches <- ids
			<-release
		}),
	}

	messages := []*azservicebus.ReceivedMessage{batchMessage("1"), batchMessage("2"), batchMessage("3")}
	done := make([]<-chan struct{}, 0, len(messages))
	for i, message := range messages {
		done = append(done, runBatchMessages(context.Background(), internal, &batchTestSettler{}, message))
		require.Eventually(t, func() bool {
			internal.mu.Lock()
			defer internal.mu.Unlock()
			group := internal.groups[""]
			if i == len(messages)-1 {
				return group != nil && group.current == nil
			}
			return group != nil && group.current != nil && len(group.current.items) == i+1
		}, time.Second, time.Millisecond)
	}

	require.Equal(t, []string{"1", "2", "3"}, <-batches)
	close(release)
	for _, messageDone := range done {
		select {
		case <-messageDone:
		case <-time.After(time.Second):
			t.Fatal("message handler did not return")
		}
	}
}

func TestBatchHandlerFlushesPartialBatchByTime(t *testing.T) {
	batches := make(chan int, 1)
	handler := NewBatchHandler(
		&BatchHandlerOptions{MaxBatchSize: 3, FlushInterval: 20 * time.Millisecond},
		BatchHandlerFunc(func(_ context.Context, _ MessageSettler, messages []*azservicebus.ReceivedMessage) {
			batches <- len(messages)
		}),
	)

	done := runBatchMessages(context.Background(), handler, &batchTestSettler{}, batchMessage("1"), batchMessage("2"))
	require.Equal(t, 2, <-batches)
	require.Eventually(t, func() bool {
		select {
		case <-done:
			return true
		default:
			return false
		}
	}, time.Second, time.Millisecond)
}

func TestBatchHandlerSeparatesGroupsAndRunsThemConcurrently(t *testing.T) {
	started := make(chan string, 2)
	release := make(chan struct{})
	handler := NewBatchHandler(
		&BatchHandlerOptions{
			MaxBatchSize: 1,
			FlushInterval: time.Minute,
			GroupBy: func(message *azservicebus.ReceivedMessage) string {
				return *message.CorrelationID
			},
		},
		BatchHandlerFunc(func(_ context.Context, _ MessageSettler, messages []*azservicebus.ReceivedMessage) {
			started <- *messages[0].CorrelationID
			<-release
		}),
	)

	a := batchMessage("a")
	a.CorrelationID = to.Ptr("a")
	b := batchMessage("b")
	b.CorrelationID = to.Ptr("b")
	done := runBatchMessages(context.Background(), handler, &batchTestSettler{}, a, b)

	keys := map[string]bool{<-started: true, <-started: true}
	require.Equal(t, map[string]bool{"a": true, "b": true}, keys)
	close(release)
	<-done
}

func TestBatchHandlerSerializesBatchesForSameGroup(t *testing.T) {
	firstStarted := make(chan struct{})
	secondStarted := make(chan struct{})
	releaseFirst := make(chan struct{})
	var calls atomic.Int32
	handler := NewBatchHandler(
		&BatchHandlerOptions{MaxBatchSize: 1, FlushInterval: time.Minute},
		BatchHandlerFunc(func(_ context.Context, _ MessageSettler, _ []*azservicebus.ReceivedMessage) {
			if calls.Add(1) == 1 {
				close(firstStarted)
				<-releaseFirst
				return
			}
			close(secondStarted)
		}),
	)

	firstDone := runBatchMessages(context.Background(), handler, &batchTestSettler{}, batchMessage("1"))
	<-firstStarted
	secondDone := runBatchMessages(context.Background(), handler, &batchTestSettler{}, batchMessage("2"))
	select {
	case <-secondStarted:
		t.Fatal("second batch started before the first completed")
	case <-time.After(20 * time.Millisecond):
	}
	close(releaseFirst)
	<-firstDone
	<-secondStarted
	<-secondDone
}

func TestBatchHandlerCancellationReleasesPendingMessages(t *testing.T) {
	var calls atomic.Int32
	handler := NewBatchHandler(
		&BatchHandlerOptions{MaxBatchSize: 2, FlushInterval: time.Minute},
		BatchHandlerFunc(func(context.Context, MessageSettler, []*azservicebus.ReceivedMessage) {
			calls.Add(1)
		}),
	)
	ctx, cancel := context.WithCancel(context.Background())
	done := runBatchMessages(ctx, handler, &batchTestSettler{}, batchMessage("1"))
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("pending handler was not released on cancellation")
	}
	require.Zero(t, calls.Load())
}

func TestBatchHandlerCancellationDuringActiveBatch(t *testing.T) {
	started := make(chan struct{})
	handler := NewBatchHandler(
		&BatchHandlerOptions{MaxBatchSize: 1},
		BatchHandlerFunc(func(ctx context.Context, _ MessageSettler, _ []*azservicebus.ReceivedMessage) {
			close(started)
			<-ctx.Done()
		}),
	)
	ctx, cancel := context.WithCancel(context.Background())
	done := runBatchMessages(ctx, handler, &batchTestSettler{}, batchMessage("1"))
	<-started
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("active batch was not released on cancellation")
	}

	func TestBatchHandlerKeepsLocksRenewingWhileWaiting(t *testing.T) {
		settler := &batchTestSettler{}
		handler := NewRenewLockHandler(
			&LockRenewalOptions{Interval: to.Ptr(2 * time.Millisecond)},
			NewBatchHandler(
				&BatchHandlerOptions{MaxBatchSize: 2, FlushInterval: 30 * time.Millisecond},
				BatchHandlerFunc(func(context.Context, MessageSettler, []*azservicebus.ReceivedMessage) {}),
			),
		)

		<-runBatchMessages(context.Background(), handler, settler, batchMessage("1"))
		require.Positive(t, settler.renewed.Load())
	}
}

func TestBatchHandlerPanicReleasesAllMessages(t *testing.T) {
	panicSeen := make(chan struct{}, 1)
	handler := NewPanicHandler(
		&PanicHandlerOptions{OnPanicRecovered: func(context.Context, MessageSettler, *azservicebus.ReceivedMessage, any) {
			panicSeen <- struct{}{}
		}},
		NewBatchHandler(
			&BatchHandlerOptions{MaxBatchSize: 2, FlushInterval: time.Minute},
			BatchHandlerFunc(func(context.Context, MessageSettler, []*azservicebus.ReceivedMessage) {
				panic("batch failed")
			}),
		),
	)

	done := runBatchMessages(context.Background(), handler, &batchTestSettler{}, batchMessage("1"), batchMessage("2"))
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("panic left batch callers blocked")
	}
	require.Len(t, panicSeen, 1)
}

func TestBatchHandlerDeliversEachMessageExactlyOnce(t *testing.T) {
	const messageCount = 100
	var mu sync.Mutex
	seen := make(map[string]int, messageCount)
	handler := NewBatchHandler(
		&BatchHandlerOptions{MaxBatchSize: 7, FlushInterval: 2 * time.Millisecond},
		BatchHandlerFunc(func(_ context.Context, _ MessageSettler, messages []*azservicebus.ReceivedMessage) {
			mu.Lock()
			defer mu.Unlock()
			for _, message := range messages {
				seen[message.MessageID]++
			}

			func TestBatchHandlerFillsAcrossSmallerReceiveCalls(t *testing.T) {
				receiver := &batchTestReceiver{
					batchTestSettler: &batchTestSettler{},
					messages: []*azservicebus.ReceivedMessage{
						batchMessage("1"),
						batchMessage("2"),
						batchMessage("3"),
					},
				}
				received := make(chan int, 1)
				handler := NewBatchHandler(
					&BatchHandlerOptions{MaxBatchSize: 3, FlushInterval: time.Second},
					BatchHandlerFunc(func(_ context.Context, _ MessageSettler, messages []*azservicebus.ReceivedMessage) {
						received <- len(messages)
					}),
				)
				interval := time.Millisecond
				processor := NewProcessor(receiver, handler, &ProcessorOptions{
					MaxConcurrency:  3,
					MaxReceiveCount: 1,
					ReceiveInterval: &interval,
				})
				ctx, cancel := context.WithCancel(context.Background())
				finished := make(chan error, 1)
				go func() {
					finished <- processor.Start(ctx)
				}()

				require.Equal(t, 3, <-received)
				cancel()
				require.ErrorIs(t, <-finished, context.Canceled)
			}
		}),
	)
	messages := make([]*azservicebus.ReceivedMessage, messageCount)
	for i := range messages {
		messages[i] = batchMessage(fmt.Sprint(i))
	}

	<-runBatchMessages(context.Background(), handler, &batchTestSettler{}, messages...)
	require.Len(t, seen, messageCount)
	for _, count := range seen {
		require.Equal(t, 1, count)
	}
}

func TestBatchSettlementHandlerAppliesIndependentSettlements(t *testing.T) {
	settler := &batchTestSettler{}
	handler := NewBatchSettlementHandler(nil, BatchSettlerFunc(
		func(context.Context, []*azservicebus.ReceivedMessage) []Settlement {
			return []Settlement{&Complete{}, &Abandon{}}
		},
	))

	handler.Handle(context.Background(), settler, []*azservicebus.ReceivedMessage{
		batchMessage("1"),
		batchMessage("2"),
	})
	require.EqualValues(t, 1, settler.completed.Load())
	require.EqualValues(t, 1, settler.abandoned.Load())
}

func TestBatchSettlementHandlerRejectsInvalidResults(t *testing.T) {
	tests := map[string][]Settlement{
		"missing": {},
		"extra":   {&Complete{}, &Complete{}},
		"nil":     {nil},
	}
	for name, settlements := range tests {
		t.Run(name, func(t *testing.T) {
			handler := NewBatchSettlementHandler(nil, BatchSettlerFunc(
				func(context.Context, []*azservicebus.ReceivedMessage) []Settlement {
					return settlements
				},
			))
			require.Panics(t, func() {
				handler.Handle(context.Background(), &batchTestSettler{}, []*azservicebus.ReceivedMessage{batchMessage("1")})
			})
		})
	}
}

func TestBatchSettlementHandlerCanReplaceInvalidResults(t *testing.T) {
	settler := &batchTestSettler{}
	handler := NewBatchSettlementHandler(
		&BatchSettlementHandlerOptions{
			OnInvalidSettlements: func(
				context.Context,
				[]*azservicebus.ReceivedMessage,
				[]Settlement,
			) []Settlement {
				return []Settlement{&Complete{}}
			},
		},
		BatchSettlerFunc(func(context.Context, []*azservicebus.ReceivedMessage) []Settlement {
			return nil
		}),
	)

	handler.Handle(context.Background(), settler, []*azservicebus.ReceivedMessage{batchMessage("1")})
	require.EqualValues(t, 1, settler.completed.Load())
}

func TestBatchOptionsValidateProcessorConcurrency(t *testing.T) {
	require.NoError(t, (&BatchHandlerOptions{MaxBatchSize: 3}).ValidateProcessorOptions(
		&ProcessorOptions{MaxConcurrency: 3},
	))
	require.EqualError(t, (&BatchHandlerOptions{MaxBatchSize: 3}).ValidateProcessorOptions(
		&ProcessorOptions{MaxConcurrency: 2},
	), "processor MaxConcurrency (2) must be at least batch MaxBatchSize (3)")
	require.Error(t, (*BatchHandlerOptions)(nil).ValidateProcessorOptions(nil))
}
