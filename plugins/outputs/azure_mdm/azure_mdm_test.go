package azuremdm

import (
	"fmt"
	"net"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestAzureMdm_Connect(t *testing.T) {
	listener, err := net.ListenPacket("udp", "127.0.0.1:8125")
	require.NoError(t, err)

	fmt.Printf("Listener: %v listening on port 8125\n", listener)
	mdm := newAzureMdm()

	err = mdm.Connect()
	require.NoError(t, err)
}
