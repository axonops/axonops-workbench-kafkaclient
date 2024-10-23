package kafka

import (
	"context"
	"fmt"
	"math"
	"time"

	log "github.com/sirupsen/logrus"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
	"github.com/twmb/franz-go/pkg/kversion"
)

type ExponentialBackoff struct {
	BaseInterval time.Duration
	MaxInterval  time.Duration
	Multiplier   float64
}

func (e *ExponentialBackoff) Backoff(attempts int) time.Duration {
	multiplied := math.Pow(e.Multiplier, float64(attempts))
	wait := time.Duration(float64(e.BaseInterval) * multiplied)
	if wait > e.MaxInterval {
		wait = e.MaxInterval
	}
	return wait
}

// Config required for opening a connection to Kafka
type KafkaConfig struct {
	// General
	Brokers          []string     `yaml:"brokers"`
	ClientID         string       `yaml:"clientId"`
	ClusterVersion   string       `yaml:"clusterVersion"`
	RackID           string       `yaml:"rackId"`
	MetricsNamespace string       `yaml:"metricsNamespace"`
	Startup          KafkaStartup `yaml:"startup"`
}

type KafkaStartup struct {
	EstablishConnectionEagerly bool          `yaml:"establishConnectionEagerly"`
	MaxRetries                 int           `yaml:"maxRetries"`
	RetryInterval              time.Duration `yaml:"retryInterval"`
	MaxRetryInterval           time.Duration `yaml:"maxRetryInterval"`
	BackoffMultiplier          float64       `yaml:"backoffMultiplier"`
}

type Service struct {
	KafkaClient      *kgo.Client
	KafkaAdminClient *kadm.Client
	MetricsNamespace string
}

func NewService(cfg *KafkaConfig) *Service {
	kgoOpts, err := kgo.NewClient(cfg)
	if err != nil {
		log.Fatalf(fmt.Sprintf("failed to create a valid kafka client config: %v", err))
	}

	kafkaClient, err := kgo.NewClient(kgoOpts...)
	if err != nil {
		log.Fatalf(fmt.Sprintf("failed to create kafka client: %v", err))
	}
	eb := ExponentialBackoff{
		BaseInterval: cfg.Startup.RetryInterval,
		MaxInterval:  cfg.Startup.MaxRetryInterval,
		Multiplier:   cfg.Startup.BackoffMultiplier,
	}
	attempt := 0
	for attempt < cfg.Startup.MaxRetries && cfg.Startup.EstablishConnectionEagerly {
		err = testConnection(kafkaClient, time.Second*15)
		if err == nil {
			break
		}

		backoffDuration := eb.Backoff(attempt)
		fmt.Sprintf("Failed to test Kafka connection, going to retry in %vs ", backoffDuration.Seconds())
		fmt.Sprintf("remaining_retries: %v ", cfg.Startup.MaxRetries-attempt)
		attempt++
		time.Sleep(backoffDuration)
	}
	if err != nil {
		log.Fatalf(fmt.Sprintf("failed to test kafka connection: %v", err))
	}
	return &Service{
		KafkaClient:      kafkaClient,
		KafkaAdminClient: kadm.NewClient(kafkaClient),
		MetricsNamespace: cfg.MetricsNamespace,
	}
}

func NewKgoConfig(cfg KafkaConfig) ([]kgo.Opt, error) {
	opts := []kgo.Opt{
		kgo.SeedBrokers(cfg.Brokers...),
		kgo.MaxVersions(kversion.V2_6_0()),
		kgo.ClientID(cfg.ClientID),
		kgo.FetchMaxBytes(5 * 1000 * 1000), // 5MB
		kgo.MaxConcurrentFetches(12),
		kgo.KeepControlRecords(),
	}

	if cfg.RackID != "" {
		opts = append(opts, kgo.Rack(cfg.RackID))
	}
	return opts, nil
}

func testConnection(client *kgo.Client, timeout time.Duration) error {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	fmt.Println("connecting to Kafka seed brokers, trying to fetch cluster metadata")

	req := kmsg.NewMetadataRequest()
	res, err := req.RequestWith(ctx, client)
	if err != nil {
		return fmt.Errorf("failed to request metadata: %w", err)
	}

	// Request versions in order to guess Kafka Cluster version
	versionsReq := kmsg.NewApiVersionsRequest()
	versionsRes, err := versionsReq.RequestWith(ctx, client)
	if err != nil {
		return fmt.Errorf("failed to request api versions: %w", err)
	}
	err = kerr.ErrorForCode(versionsRes.ErrorCode)
	if err != nil {
		return fmt.Errorf("failed to request api versions. Inner kafka error: %w", err)
	}
	versions := kversion.FromApiVersionsResponse(versionsRes)

	fmt.Sprintf("advertised_broker_count: %v ", len(res.Brokers))
	fmt.Sprintf("topic_count: %v ", len(res.Topics))
	fmt.Sprintf("controller_id: %v ", int(res.ControllerID))
	fmt.Sprintf("kafka_version: %v ", versions.VersionGuess())
	return nil
}
