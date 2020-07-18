/* Copyright 2017 LinkedIn Corp. Licensed under the Apache License, Version
 * 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 */

package cluster

import (
	"fmt"
	"sync"
	"time"

	"github.com/Shopify/sarama"
	"github.com/spf13/viper"
	"go.uber.org/zap"

	"github.com/tysonheart/burrow/core/internal/helpers"
	"github.com/tysonheart/burrow/core/protocol"
)

// KafkaCluster is a cluster module which connects to a single Apache Kafka cluster and manages the broker topic and
// partition information. It periodically updates a list of all topics and partitions, and also fetches the broker
// end offset (latest) for each partition. This information is forwarded to the storage module for use in consumer
// evaluations.
type KafkaCluster struct {
	// App is a pointer to the application context. This stores the channel to the storage subsystem
	App *protocol.ApplicationContext

	// Log is a logger that has been configured for this module to use. Normally, this means it has been set up with
	// fields that are appropriate to identify this coordinator
	Log *zap.Logger

	name          string
	saramaConfig  *sarama.Config
	servers       []string
	offsetRefresh int
	topicRefresh  int

	offsetTicker   *time.Ticker
	metadataTicker *time.Ticker
	quitChannel    chan struct{}
	running        sync.WaitGroup

	fetchMetadata bool
	topicMap      map[string]int

	requestChannel chan *protocol.OffsetFetchRequest

	commitMetadataCache map[string]*protocol.CommitMetadata

	// This lock is used when modifying commit metadata cache
	commitMetadataCacheLock *sync.RWMutex

	// min time interval for time lookup in seconds
	minTimeLookupInterval int64
}

// GetCommunicationChannel return the communication channel for this cluster
func (module *KafkaCluster) GetCommunicationChannel() chan *protocol.OffsetFetchRequest {
	return module.requestChannel
}

// Configure validates the configuration for the cluster. At minimum, there must be a list of servers provided for the
// Kafka cluster, of the form host:port. Default values will be set for the intervals to use for refreshing offsets
// (10 seconds) and topics (60 seconds). A missing, or bad, list of servers will cause this func to panic.
func (module *KafkaCluster) Configure(name string, configRoot string) {
	module.Log.Info("configuring")

	module.name = name
	module.quitChannel = make(chan struct{})
	module.running = sync.WaitGroup{}

	profile := viper.GetString(configRoot + ".client-profile")
	module.saramaConfig = helpers.GetSaramaConfigFromClientProfile(profile)

	module.servers = viper.GetStringSlice(configRoot + ".servers")
	if len(module.servers) == 0 {
		panic("No Kafka brokers specified for cluster " + module.name)
	} else if !helpers.ValidateHostList(module.servers) {
		panic("Cluster '" + name + "' has one or more improperly formatted servers (must be host:port)")
	}

	viper.SetDefault(configRoot+".min-time-lookup", 900)
	module.minTimeLookupInterval = viper.GetInt64(configRoot + ".min-time-lookup")

	// Set defaults for configs if needed
	viper.SetDefault(configRoot+".offset-refresh", 10)
	viper.SetDefault(configRoot+".topic-refresh", 60)
	module.offsetRefresh = viper.GetInt(configRoot + ".offset-refresh")
	module.topicRefresh = viper.GetInt(configRoot + ".topic-refresh")
}

// Start connects to the Kafka cluster using the Shopify/sarama client. Any error connecting to the cluster is returned
// to the caller. Once the client is set up, tickers are started to periodically refresh topics and offsets.
func (module *KafkaCluster) Start() error {
	module.Log.Info("starting")

	// Connect Kafka client
	client, err := sarama.NewClient(module.servers, module.saramaConfig)
	if err != nil {
		module.Log.Error("failed to start client", zap.Error(err))
		return err
	}

	// Fire off the offset requests once, before we start the ticker, to make sure we start with good data for consumers
	helperClient := &helpers.BurrowSaramaClient{
		Client: client,
	}

	module.commitMetadataCache = make(map[string]*protocol.CommitMetadata, 512)

	module.commitMetadataCacheLock = &sync.RWMutex{}

	module.fetchMetadata = true
	module.getOffsets(helperClient)

	// Start main loop that has a timer for offset and topic fetches
	module.offsetTicker = time.NewTicker(time.Duration(module.offsetRefresh) * time.Second)
	module.metadataTicker = time.NewTicker(time.Duration(module.topicRefresh) * time.Second)

	module.requestChannel = make(chan *protocol.OffsetFetchRequest, 50)

	go module.mainLoop(helperClient)

	return nil
}

// Stop causes both the topic and offset refresh tickers to be stopped, and then it closes the Kafka client.
func (module *KafkaCluster) Stop() error {
	module.Log.Info("stopping")

	module.metadataTicker.Stop()
	module.offsetTicker.Stop()
	close(module.quitChannel)
	module.running.Wait()

	return nil
}

func (module *KafkaCluster) mainLoop(client helpers.SaramaClient) {
	module.running.Add(1)
	defer module.running.Done()

	for {
		select {
		case <-module.offsetTicker.C:
			module.getOffsets(client)
		case <-module.metadataTicker.C:
			// Update metadata on next offset fetch
			module.fetchMetadata = true
		case req := <-module.requestChannel:
			go module.processClusterRequest(client, req)
		case <-module.quitChannel:
			return
		}
	}
}

func (module *KafkaCluster) processClusterRequest(client helpers.SaramaClient, req *protocol.OffsetFetchRequest) error {
	response := &protocol.OffsetFetchResponse{}

	t, err := module.getOffsetTimestamp(client, req.Category, req.Topic, req.Partition, req.Offset)
	if err != nil {
		response.Error = err
	} else {
		response.Timestamp = t
		response.Error = nil
	}

	req.ResponseChannel <- response

	return nil
}

func (module *KafkaCluster) getOffsetTimestamp(client helpers.SaramaClient, category string, topic string, partition int32, offset int64) (time.Time, error) {

	key1 := fmt.Sprintf("%s:%s:%d", category, topic, partition)

	module.commitMetadataCacheLock.RLock()
	if val, ok := module.commitMetadataCache[key1]; !ok {

		module.commitMetadataCacheLock.RUnlock()
		module.Log.Debug("No cache entry for key for offset time lookup; first lookup",
			zap.String("key", key1))

		t, err := module.fetchMessageTimestamp(client, topic, partition, offset)
		if err != nil {
			return t, err
		}

		cmd := &protocol.CommitMetadata{
			MsgTimestamp: t,
			LastUpdated:  time.Now(),
		}

		module.commitMetadataCacheLock.Lock()
		module.commitMetadataCache[key1] = cmd
		module.commitMetadataCacheLock.Unlock()

		return cmd.MsgTimestamp, nil

	} else {
		module.commitMetadataCacheLock.RUnlock()

		if time.Now().Sub(val.LastUpdated).Seconds() > float64(module.minTimeLookupInterval) {
			module.Log.Info("Offset time lookup min interval exceeded, looking up time for key",
				zap.String("key", key1))

			t, err := module.fetchMessageTimestamp(client, topic, partition, offset)
			if err != nil {
				return time.Time{}, err
			}

			val.LastUpdated = time.Now()
			val.MsgTimestamp = t

			module.commitMetadataCacheLock.Lock()
			module.commitMetadataCache[key1] = val
			module.commitMetadataCacheLock.Unlock()
		}

		return val.MsgTimestamp, nil
	}
}

// func (module *KafkaCluster) getMessageTimestamp(client helpers.SaramaClient, topic string, partition int32, offset int64) (time.Time, error) {
// 	consumer, err := client.NewConsumerFromClient()
// 	if err != nil {
// 		module.Log.Error("Error creating new consumer from client")
// 		return time.Time{}, fmt.Errorf("error getting new consumer from client")
// 	}

// 	pConsumer, err := consumer.ConsumePartition(topic, partition, offset-1)
// 	defer consumer.Close()

// 	if err != nil {
// 		module.Log.Error("Error starting time offset consumer",
// 			zap.String("topic", topic), zap.Int32("partition", partition), zap.Int64("offset", offset))
// 		return time.Time{}, fmt.Errorf("Error starting time offset consumer for: %s:%d:%d", topic, partition, offset)
// 	}
// 	for message := range pConsumer.Messages() {
// 		module.Log.Info("Received message for finding timestamp for",
// 			zap.String("topic", topic), zap.Int32("partition", partition), zap.Int64("offset", offset))

// 		return message.Timestamp, nil
// 	}

// 	return time.Time{}, fmt.Errorf("No messages found for get timestamp for at TPO: %s:%d:%d", topic, partition, offset)
// }

// this function finds the leader for a given topic+partition and issues a fetch request.
// The timestamp from the response is returned.
func (module *KafkaCluster) fetchMessageTimestamp(client helpers.SaramaClient, topic string, partition int32, offset int64) (time.Time, error) {

	broker, err := client.Leader(topic, partition)
	if err != nil {
		return time.Time{}, err
	}

	request := &sarama.FetchRequest{
		MinBytes:    client.Config().Consumer.Fetch.Min,
		MaxWaitTime: int32(client.Config().Consumer.MaxWaitTime / time.Millisecond),
	}

	if client.Config().Version.IsAtLeast(sarama.V0_9_0_0) {
		request.Version = 1
	}
	if client.Config().Version.IsAtLeast(sarama.V0_10_0_0) {
		request.Version = 2
	}
	if client.Config().Version.IsAtLeast(sarama.V0_10_1_0) {
		request.Version = 3
		request.MaxBytes = sarama.MaxResponseSize
	}
	if client.Config().Version.IsAtLeast(sarama.V0_11_0_0) {
		request.Version = 4
		request.Isolation = client.Config().Consumer.IsolationLevel
	}
	if client.Config().Version.IsAtLeast(sarama.V1_1_0_0) {
		request.Version = 7
		// We do not currently implement KIP-227 FetchSessions. Setting the id to 0
		// and the epoch to -1 tells the broker not to generate as session ID we're going
		// to just ignore anyway.
		request.SessionID = 0
		request.SessionEpoch = -1
	}
	if client.Config().Version.IsAtLeast(sarama.V2_1_0_0) {
		request.Version = 10
	}
	if client.Config().Version.IsAtLeast(sarama.V2_3_0_0) {
		request.Version = 11
		request.RackID = client.Config().RackID
	}

	request.AddBlock(topic, partition, offset, client.Config().Consumer.Fetch.Default)

	response, err := broker.Fetch(request)
	if err != nil {
		return time.Time{}, err
	}

	var msgTimestamp time.Time

	block := response.GetBlock(topic, partition)
	if block == nil {
		return time.Time{}, sarama.ErrIncompleteResponse
	}

	//module.Log.Info("block data", zap.Any("blockdump", block))

	if block.Err != sarama.ErrNoError {
		return time.Time{}, fmt.Errorf("Block Error:%d", block.Err)
	}

	if len(block.RecordsSet) <= 0 {
		return time.Time{}, fmt.Errorf("No records in block for: %s:%d:%d", topic, partition, offset)
	}

	if block.Partial {
		return time.Time{}, fmt.Errorf("partial block data for: %s:%d:%d", topic, partition, offset)
	}

	if len(block.RecordsSet[0].MsgSet.Messages) <= 0 {
		return time.Time{}, fmt.Errorf("No messages in recordset for: %s:%d:%d", topic, partition, offset)
	}

	msgTimestamp = block.RecordsSet[0].MsgSet.Messages[0].Msg.Timestamp

	module.Log.Info("Looked up timestamp for message",
		zap.String("topic", topic),
		zap.Int32("partition", partition),
		zap.Int64("offset", offset),
		zap.String("timestamp", msgTimestamp.String()),
	)

	return msgTimestamp, nil
}

func (module *KafkaCluster) maybeUpdateMetadataAndDeleteTopics(client helpers.SaramaClient) {
	if module.fetchMetadata {
		module.fetchMetadata = false
		client.RefreshMetadata()

		// Get the current list of topics and make a map
		topicList, err := client.Topics()
		if err != nil {
			module.Log.Error("failed to fetch topic list", zap.String("sarama_error", err.Error()))
			return
		}

		// We'll use the partition counts later
		topicMap := make(map[string]int)
		for _, topic := range topicList {
			partitions, err := client.Partitions(topic)
			if err != nil {
				module.Log.Error("failed to fetch partition list", zap.String("sarama_error", err.Error()))
				return
			}
			topicMap[topic] = len(partitions)
		}

		// Check for deleted topics if we have a previous map to check against
		if module.topicMap != nil {
			for topic := range module.topicMap {
				if _, ok := topicMap[topic]; !ok {
					// Topic no longer exists - tell storage to delete it
					module.App.StorageChannel <- &protocol.StorageRequest{
						RequestType: protocol.StorageSetDeleteTopic,
						Cluster:     module.name,
						Topic:       topic,
					}
				}
			}
		}

		// Save the new topicMap for next time
		module.topicMap = topicMap
	}
}

func (module *KafkaCluster) generateOffsetRequests(client helpers.SaramaClient) (map[int32]*sarama.OffsetRequest, map[int32]helpers.SaramaBroker) {
	requests := make(map[int32]*sarama.OffsetRequest)
	brokers := make(map[int32]helpers.SaramaBroker)

	// Generate an OffsetRequest for each topic:partition and bucket it to the leader broker
	errorTopics := make(map[string]bool)
	for topic, partitions := range module.topicMap {
		for i := 0; i < partitions; i++ {
			broker, err := client.Leader(topic, int32(i))
			if err != nil {
				module.Log.Warn("failed to fetch leader for partition",
					zap.String("topic", topic),
					zap.Int("partition", i),
					zap.String("sarama_error", err.Error()))
				errorTopics[topic] = true
				continue
			}
			if _, ok := requests[broker.ID()]; !ok {
				requests[broker.ID()] = &sarama.OffsetRequest{}
			}
			brokers[broker.ID()] = broker
			requests[broker.ID()].AddBlock(topic, int32(i), sarama.OffsetNewest, 1)
		}
	}

	// If there are any topics that had errors, force a metadata refresh on the next run
	if len(errorTopics) > 0 {
		module.fetchMetadata = true
	}

	return requests, brokers
}

// This function performs massively parallel OffsetRequests, which is better than Sarama's internal implementation,
// which does one at a time. Several orders of magnitude faster.
func (module *KafkaCluster) getOffsets(client helpers.SaramaClient) {
	module.maybeUpdateMetadataAndDeleteTopics(client)
	requests, brokers := module.generateOffsetRequests(client)

	// Send out the OffsetRequest to each broker for all the partitions it is leader for
	// The results go to the offset storage module
	var wg = sync.WaitGroup{}
	var errorTopics = sync.Map{}

	getBrokerOffsets := func(brokerID int32, request *sarama.OffsetRequest) {
		defer wg.Done()
		response, err := brokers[brokerID].GetAvailableOffsets(request)
		if err != nil {
			module.Log.Error("failed to fetch offsets from broker",
				zap.String("sarama_error", err.Error()),
				zap.Int32("broker", brokerID),
			)
			brokers[brokerID].Close()
			return
		}
		ts := time.Now().Unix() * 1000
		for topic, partitions := range response.Blocks {
			for partition, offsetResponse := range partitions {
				if offsetResponse.Err != sarama.ErrNoError {
					module.Log.Warn("error in OffsetResponse",
						zap.String("sarama_error", offsetResponse.Err.Error()),
						zap.Int32("broker", brokerID),
						zap.String("topic", topic),
						zap.Int32("partition", partition),
					)

					// Gather a list of topics that had errors
					errorTopics.Store(topic, true)
					continue
				}

				// The log end offset is the offset of next message that will be produced to. So we use but one message (offsetValue.Offset-1).
				msgTimestamp, err := module.getOffsetTimestamp(client, "high-watermark", topic, partition, (offsetResponse.Offsets[0] - 1))
				if err != nil {
					module.Log.Error("Error fetching timestamp for",
						zap.String("topic", topic),
						zap.Int32("partition", partition),
						zap.Int64("offset", offsetResponse.Offsets[0]),
						zap.Error(err),
					)
					//return
				}

				offset := &protocol.StorageRequest{
					RequestType:         protocol.StorageSetBrokerOffset,
					Cluster:             module.name,
					Topic:               topic,
					Partition:           partition,
					Offset:              offsetResponse.Offsets[0],
					Timestamp:           ts,
					TopicPartitionCount: int32(module.topicMap[topic]),
					MsgTimestamp:        msgTimestamp,
				}

				module.Log.Info("StorageSetBrokerOffset timestamp", zap.String("msgtime", offset.MsgTimestamp.String()))
				helpers.TimeoutSendStorageRequest(module.App.StorageChannel, offset, 1)
			}
		}
	}

	for brokerID, request := range requests {
		wg.Add(1)
		go getBrokerOffsets(brokerID, request)
	}

	wg.Wait()

	// If there are any topics that had errors, force a metadata refresh on the next run
	errorTopics.Range(func(key, value interface{}) bool {
		module.fetchMetadata = true
		return false
	})
}
