package kafka

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"github.com/IBM/sarama"
)

var DefaultVersion = sarama.V3_6_0_0

func baseConfig(clientID string) *sarama.Config {
	cfg := sarama.NewConfig()
	cfg.Version = DefaultVersion
	cfg.ClientID = clientID
	cfg.Net.DialTimeout = 5 * time.Second
	cfg.Net.ReadTimeout = 5 * time.Second
	cfg.Net.WriteTimeout = 5 * time.Second
	cfg.Metadata.Full = true
	return cfg
}

func EnsureTopic(brokers []string, clientID, topic string, partitions int32) error {
	cfg := baseConfig(clientID)
	admin, err := sarama.NewClusterAdmin(brokers, cfg)
	if err != nil {
		return err
	}
	defer admin.Close()

	detail := &sarama.TopicDetail{
		NumPartitions:     partitions,
		ReplicationFactor: 1,
	}

	if err := admin.CreateTopic(topic, detail, false); err != nil && !errors.Is(err, sarama.ErrTopicAlreadyExists) {
		return err
	}

	return nil
}

func NewSyncProducer(brokers []string, clientID string) (sarama.SyncProducer, error) {
	cfg := baseConfig(clientID)
	cfg.Producer.RequiredAcks = sarama.WaitForAll
	cfg.Producer.Retry.Max = 5
	cfg.Producer.Return.Successes = true
	cfg.Producer.Partitioner = sarama.NewHashPartitioner
	return sarama.NewSyncProducer(brokers, cfg)
}

func NewConsumerGroup(brokers []string, groupID, clientID string) (sarama.ConsumerGroup, sarama.Client, error) {
	cfg := baseConfig(clientID)
	cfg.Consumer.Group.Rebalance.GroupStrategies = []sarama.BalanceStrategy{
		sarama.NewBalanceStrategySticky(),
	}
	cfg.Consumer.Offsets.Initial = sarama.OffsetOldest
	cfg.Consumer.Return.Errors = true
	cfg.Consumer.Group.Session.Timeout = 30 * time.Second
	cfg.Consumer.Group.Heartbeat.Interval = 3 * time.Second

	client, err := sarama.NewClient(brokers, cfg)
	if err != nil {
		return nil, nil, err
	}

	group, err := sarama.NewConsumerGroupFromClient(groupID, client)
	if err != nil {
		_ = client.Close()
		return nil, nil, err
	}

	return group, client, nil
}

func RunConsumerGroup(ctx context.Context, group sarama.ConsumerGroup, topics []string, logger *slog.Logger, handler sarama.ConsumerGroupHandler) error {
	errCh := group.Errors()
	if errCh != nil {
		go func() {
			for err := range errCh {
				logger.Error("kafka consumer error", "error", err)
			}
		}()
	}

	for {
		if err := group.Consume(ctx, topics, handler); err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			logger.Error("kafka consume cycle failed", "error", err)
			time.Sleep(2 * time.Second)
		}

		if ctx.Err() != nil {
			return ctx.Err()
		}
	}
}
