package protocol

import (
	"fmt"
	"time"
)

// OffsetFetchRequest is sent over the ClusterChannel that is stored in the application context. It is to request
// data from the cluster. This is first introduced so that consumer module can communicate with the cluster it
// is configured with with specific data - timestamp in messages in given topic, partition, offset.
type OffsetFetchRequest struct {

	// The name of the cluster to which this request is intended to
	Cluster string

	// could serve as a consumer group / any bucket for offset categorization
	Category string

	Topic string

	Partition int32

	Offset int64
	// The channel to send response on
	ResponseChannel chan *OffsetFetchResponse
}

// OffsetFetchResponse response to ClusterRequest to get details for message at a given offset
// If Error is set, it means failed response.
type OffsetFetchResponse struct {
	Timestamp time.Time // Msg timestamp
	Error     error
}

func (c *OffsetFetchRequest) String() string {
	return c.Cluster + ":" + c.Category + ":" + c.Topic + ":" + fmt.Sprint(c.Partition) + ":" + fmt.Sprint(c.Offset)
}

func (r *OffsetFetchResponse) String() string {
	return fmt.Sprint(r.Timestamp.String())
}

// CommitMetadata Metadata associated with a commit like timestamp etc.
type CommitMetadata struct {
	// Create time of the message at the offset
	MsgTimestamp time.Time
	// last time when this entry was recorded (msgTimestamp updated)
	LastUpdated time.Time
}
