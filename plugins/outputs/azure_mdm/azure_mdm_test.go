package azuremdm

import (
	"bytes"
	"encoding/json"
	"net"
	"testing"

	"github.com/influxdata/telegraf"
	"github.com/influxdata/telegraf/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAzureMdm_translate(t *testing.T) {
	mdm := newAzureMdm()
	mdm.Account = "testAccount"
	mdm.Namespace = "testNamespace"

	azureMetrics, err := mdm.translate(testutil.TestMetric(1, "test"))
	require.NoError(t, err)

	assert.Equal(t, len(azureMetrics), 1)
	b, err := json.Marshal(azureMetrics[0].azureMdmData)
	require.NoError(t, err)

	output := "{\"Account\":\"testAccount\",\"Namespace\":\"testNamespace\",\"Metric\":\"test.value\",\"Dims\":{\"tag1\":\"value1\"}}"
	assert.Equal(t, string(b), output)
}

func TestAzureMdm_Connect(t *testing.T) {

	addr := net.UDPAddr{
		Port: 8125,
		IP:   net.ParseIP("127.0.0.1"),
	}
	conn, err := net.ListenUDP("udp", &addr)
	require.NoError(t, err)

	defer conn.Close()
	mdm := newAzureMdm()
	mdm.Account = "testAccount"
	mdm.Namespace = "testNamespace"

	//c, err := statsd.New(statsd.Address("127.0.0.1:38125"))
	err = mdm.Connect()
	require.NoError(t, err)
	//mdm.Client = c

	metrics := []telegraf.Metric{}
	metrics = append(metrics, testutil.TestMetric(1, "test"))
	metrics = append(metrics, testutil.TestMetric(2, "test"))

	err = mdm.Write(metrics)
	require.NoError(t, err)

	buf := make([]byte, 256)
	var mstrins []string
	for len(mstrins) < 1 {
		n, _, err := conn.ReadFromUDP(buf)
		require.NoError(t, err)

		for _, bs := range bytes.Split(buf[:n], []byte{'\n'}) {
			if len(bs) == 0 {
				continue
			}
			mstrins = append(mstrins, string(bs))
		}
	}

	expected := "{\"Account\":\"testAccount\",\"Namespace\":\"testNamespace\",\"Metric\":\"test.value\",\"Dims\":{\"tag1\":\"value1\"}}:1|g"
	assert.Equal(t, mstrins[0], expected)
}
