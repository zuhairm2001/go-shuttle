package shuttle_test

import (
	"context"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/Azure/azure-sdk-for-go/sdk/messaging/azservicebus"

	"github.com/Azure/go-shuttle/v2"
)

func ExampleNewBatchHandler() {
	credential, err := azidentity.NewDefaultAzureCredential(nil)
	if err != nil {
		panic(err)
	}
	client, err := azservicebus.NewClient("myservicebus.servicebus.windows.net", credential, nil)
	if err != nil {
		panic(err)
	}
	// Configure this subscription in Azure Service Bus to filter the "type"
	// application property set by shuttle.Sender.
	receiver, err := client.NewReceiverForSubscription("topic-a", "batched-orders", nil)
	if err != nil {
		panic(err)
	}

	batchOptions := &shuttle.BatchHandlerOptions{
		MaxBatchSize:  10,
		FlushInterval: 5 * time.Second,
		GroupBy: func(message *azservicebus.ReceivedMessage) string {
			if message.CorrelationID == nil {
				return ""
			}
			return *message.CorrelationID
		},
	}
	processorOptions := &shuttle.ProcessorOptions{
		MaxConcurrency:  20,
		MaxReceiveCount: 10,
	}
	if err := batchOptions.ValidateProcessorOptions(processorOptions); err != nil {
		panic(err)
	}

	batch := shuttle.NewBatchHandler(batchOptions,
		shuttle.NewBatchSettlementHandler(nil, shuttle.BatchSettlerFunc(
			func(_ context.Context, messages []*azservicebus.ReceivedMessage) []shuttle.Settlement {
				settlements := make([]shuttle.Settlement, len(messages))
				for i := range messages {
					settlements[i] = &shuttle.Complete{}
				}
				return settlements
			},
		)))
	handler := shuttle.NewPanicHandler(nil,
		shuttle.NewRenewLockHandler(nil, batch))
	processor := shuttle.NewProcessor(receiver, handler, processorOptions)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	_ = processor.Start(ctx)
}
