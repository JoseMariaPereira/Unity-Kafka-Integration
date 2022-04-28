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

        private Thread thread;
        private CancellationTokenSource cancel;

        [SerializeField]
        private bool keepRunning;

        private string playerName;

        private ConcurrentQueue<string> stringReceived;

        void Start()
        {
            playerName = this.name;
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
                movement = StringToVector3(message);
            }
            transform.position = movement;
        }

        public static Vector3 StringToVector3(string sVector)
        {
            // Remove the parentheses
            if (sVector.StartsWith("(") && sVector.EndsWith(")"))
            {
                sVector = sVector.Substring(1, sVector.Length - 2);
            }

            // split the items
            string[] sArray = sVector.Split(',');

            // store as a Vector3
            Vector3 result = new Vector3(
                float.Parse(sArray[0].Trim(), CultureInfo.InvariantCulture),
                float.Parse(sArray[1].Trim(), CultureInfo.InvariantCulture),
                float.Parse(sArray[2].Trim(), CultureInfo.InvariantCulture));

            return result;
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
                GroupId = topicName ,
                SessionTimeoutMs = 6000,
                AutoOffsetReset = AutoOffsetReset.Earliest
            };

            using (var consumer = new ConsumerBuilder<Ignore, string>(config)
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
                        ConsumeResult<Ignore, string> consumeResult = consumer.Consume(cancellationToken);
                        stringReceived.Enqueue(consumeResult.Message.Value);
                    }
                    catch (ConsumeException e)
                    {
                        stringReceived.Enqueue("Consume error: " + e.Error.Reason);
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
