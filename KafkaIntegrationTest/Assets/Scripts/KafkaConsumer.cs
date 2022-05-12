using UnityEngine;
using Confluent.Kafka;
using System.Threading;
using System.Collections.Concurrent;
using System;
using System.Globalization;

namespace com.flyingcrow.kafka
{
    public class KafkaConsumer : MonoBehaviour
    {
        [SerializeField]
        private string bootstrapServer = "127.0.0.1:9092";
        [SerializeField]
        private string topicName = "unity-movement-test";
        [SerializeField]
        private string consumerGroupId;

        private Thread thread;
        private CancellationTokenSource cancel;

        [SerializeField]
        private bool keepRunning;

        private string playerName;

        private ConcurrentQueue<string> stringReceived;

        public void StartKafkaConsumer(string playerName)
        {
            this.playerName = playerName;
            stringReceived = new ConcurrentQueue<string>();
            cancel = new CancellationTokenSource();
            thread = new Thread(KafkaReader);
            thread.Start(cancel.Token);
        }

        private void KafkaReader(object obj)
        {
            KafkaReader((CancellationToken) obj);
        }

        // Update is called once per frame
        void FixedUpdate()
        {
            // Print out all messages we received since last frame.
            string message;
            Vector3 movement = transform.position;
            while (stringReceived.TryDequeue(out message))
            {
                movement = JsonUtility.FromJson<Vector3>(message);
            }
            transform.position = movement;
        }

        private void OnDestroy()
        {
            if (thread == null) return;
            // Tell the thread to stop on its next loop.
            keepRunning = false;
            // Abort the current consume action.
            cancel.Cancel();
            // Wait until the thread has completed.
            thread.Join();
        }

        void KafkaReader(CancellationToken cancellationToken)
        {

            ConsumerConfig config = new ConsumerConfig
            {
                BootstrapServers = bootstrapServer,
                GroupId = consumerGroupId,
                SessionTimeoutMs = 6000,
                AutoOffsetReset = AutoOffsetReset.Earliest
            };

            using (var consumer = new ConsumerBuilder<string, string>(config)
                // Note: All handlers are called on the main .Consume thread.
                .SetErrorHandler((_, e) => Console.WriteLine($"Error: {e.Reason}"))
                .SetStatisticsHandler((_, json) => Console.WriteLine($"Statistics: {json}"))
                .Build())
            {
                consumer.Subscribe(topicName); 
                while (keepRunning)
                {
                    try
                    {
                        ConsumeResult<string, string> consumeResult = consumer.Consume(cancellationToken);
                        if (consumeResult.Message.Key.Trim().Equals(playerName))
                        {
                            stringReceived.Enqueue(consumeResult.Message.Value);
                        }
                    }
                    catch (ConsumeException e)
                    {
                        Debug.LogError("Consume error: " + e.Error.Reason);
                    }
                    catch (OperationCanceledException oce)
                    {
                        Debug.Log("Exiting Consumer for: " + playerName);
                        consumer.Close();
                        break;
                    }
                }
            }
                    
        }

    }
}
