package protocol

import (
	"fmt"
	"time"
)

// ClusterRequest is sent over the ClusterChannel that is stored in the application context. It is to request
// data from the cluster. This is first introduced so that consumer module can communicate with the cluster it
// is configured with with specific data - timestamp in messages in given topic, partition, offset.
type ClusterRequest struct {

	// The name of the cluster to which this request is intended to
	Cluster string

	Topic string

	Partition int32

	Offset int64

	// output parameter
	Timestamp time.Time

	// The channel to send response on
	ResponseChannel chan *ClusterRequest

	Error error
}

func (c *ClusterRequest) String() string {
	return c.Cluster + ":" + c.Topic + ":" + fmt.Sprint(c.Partition) + ":" + fmt.Sprint(c.Offset) + ":" + c.Timestamp.String()
}
