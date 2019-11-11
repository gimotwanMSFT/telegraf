package azuremdm

import (
	"encoding/json"
	"fmt"
	"log"
	"strings"

	"github.com/influxdata/telegraf"
	"github.com/influxdata/telegraf/plugins/outputs"

	statsd "gopkg.in/alexcesaro/statsd.v2"
)

// AzureMdm allows publishing of metrics to Geneva MDM service
type AzureMdm struct {
	Account   string
	Namespace string

	Client *statsd.Client
}

// Dimensions contains the tags associated with the metric
type Dimensions map[string]string

type azureMdmData struct {
	Account   string     `json:"Account"`
	Namespace string     `json:"Namespace"`
	Metric    string     `json:"Metric"`
	Dims      Dimensions `json:"Dims"`
}

type azureMdmMetric struct {
	Data  azureMdmData
	Value int64
}

// Description provides a short description of the azure mdm output plugin
func (mdm *AzureMdm) Description() string {
	return "Emit metrics to Geneva MDM."
}

// SampleConfig returns a description of the configuration knobs provided by the azure mdm plugin
func (mdm *AzureMdm) SampleConfig() string {
	return `                                  
## Default MDM Account                              
# account = "<default-metrics-account>"                       
																						
## Default MDM Namespace                            
# namespace = "<default-namespace>"                       
`
}

// Connect initializes the plugin and validates connectivity
func (mdm *AzureMdm) Connect() error {

	c, err := statsd.New()
	if err != nil {
		return err
	}
	mdm.Client = c
	return nil
}

// Write writes the given metrics to the destination.
// If an error is encountered, it is up to the caller to retry the same write again later.
// Not parallel safe.
func (mdm *AzureMdm) Write(metrics []telegraf.Metric) error {
	if mdm.Client == nil {
		// previous write failed with permanent error and socket was closed.
		if err := mdm.Connect(); err != nil {
			return err
		}
	}

	for _, m := range metrics {
		azureMetrics, err := mdm.translate(m)
		if err != nil {
			log.Printf("D! [outputs.azure_mdm] Could not serialize metric: %v", err)
			continue
		}

		for _, azm := range azureMetrics {
			// send the metric to mdm extension
			b, err := json.Marshal(azm.Data)
			if err != nil {
				log.Printf("Error while marshalling metric %#v", err)
				return fmt.Errorf("Error while marshalling metric %#v", err)
			}

			log.Printf("Sending metric %s value %v", string(b), azm.Value)

			mdm.Client.Gauge(string(b), azm.Value)
			mdm.Client.Flush()
		}
	}

	return nil
}

func (mdm *AzureMdm) translate(m telegraf.Metric) ([]azureMdmMetric, error) {

	var azureMetrics []azureMdmMetric
	dims := make(Dimensions)

	account := mdm.Account
	namespace := mdm.Namespace
	for _, tag := range m.TagList() {
		if strings.EqualFold(tag.Key, "account") {
			account = tag.Value
			continue
		}
		if strings.EqualFold(tag.Key, "namespace") {
			namespace = tag.Value
			continue
		}

		// Azure custom metrics service supports up to 10 dimensions
		if len(dims) > 10 {
			continue
		}
		if tag.Key == "" || tag.Value == "" {
			continue
		}
		dims[tag.Key] = tag.Value
	}

	// emit a metric for each field in the telegraf metric
	// we support only Integer metric values, ignore other types
	for _, field := range m.FieldList() {
		metricName := m.Name() + "." + field.Key

		if value, ok := field.Value.(int64); ok {

			azmData := azureMdmData{
				Account:   account,
				Namespace: namespace,
				Metric:    metricName,
				Dims:      dims,
			}

			azm := azureMdmMetric{
				Data:  azmData,
				Value: value,
			}

			// now append it to the list of metrics to be emitted
			azureMetrics = append(azureMetrics, azm)
		} else {
			log.Printf("failed to parse metric %s with type:value %T:%v\n", metricName, field.Value, field.Value)
		}
	}
	return azureMetrics, nil
}

// Close closes the connection. Noop if already closed.
func (mdm *AzureMdm) Close() error {
	if mdm.Client == nil {
		return nil
	}
	mdm.Client.Close()
	mdm.Client = nil
	return nil
}

func newAzureMdm() *AzureMdm {
	return &AzureMdm{}
}

func init() {
	outputs.Add("azure_mdm", func() telegraf.Output { return newAzureMdm() })
}
