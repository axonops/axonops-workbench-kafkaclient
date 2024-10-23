package main

import (
	"fmt"
	"os"

	"github.com/axonops/axonops-workbench-kafkaclient/pkg/kafka"
	log "github.com/sirupsen/logrus"
)

func init() {
	// Log as JSON instead of the default ASCII formatter.
	log.SetFormatter(&log.JSONFormatter{})

	// Output to stdout instead of the default stderr
	// Can be any io.Writer, see below for File example
	log.SetOutput(os.Stdout)

	// Only log the warning severity or above.
	log.SetLevel(log.WarnLevel)
}

func main() {
	cfg := kafka.KafkaConfig{
		Brokers: []string{"localhost:9200"},
	}
	svc := kafka.NewService(&cfg)
	fmt.Println(svc)
}
