using UnityEngine;
using Confluent.Kafka;
using System.Threading;
using System.Collections.Concurrent;
using System;
using System.Threading.Tasks;
using System.Collections.Generic;

namespace com.flyingcrow.kafka
{
    public class KafkaProducer : MonoBehaviour
    {
        [SerializeField]
        private string bootstrapServer = "127.0.0.1:9092";
        [SerializeField]
        private string topicName = "unity-movement-test";

        private Thread thread;
        private CancellationTokenSource cancel;

        [SerializeField]
        private bool keepRunning;

        private ConcurrentQueue<MessageWrapper> stringReceived;

        public void StartKafkaProducer()
        {
            stringReceived = new ConcurrentQueue<MessageWrapper>();
            cancel = new CancellationTokenSource();
            thread = new Thread(KafkaWriter);
            thread.Start(cancel.Token);
        }

        private void KafkaWriter(object obj)
        {
            _ = KafkaWriterAsync((CancellationToken)obj);
        }

        private async Task KafkaWriterAsync(CancellationToken cancellation)
        {
            ProducerConfig config = new ProducerConfig { 
                BootstrapServers = bootstrapServer,
                SecurityProtocol = SecurityProtocol.Plaintext
            };

            using (var producer = new ProducerBuilder<string, string>(config)
                .Build())
            {
                while (keepRunning)
                {
                    try
                    {
                        MessageWrapper mw = new MessageWrapper();
                        MessageWrapper finalMw = null;
                        while (stringReceived.TryDequeue(out mw))
                        {
                            finalMw = mw;
                        }
                        if (finalMw != null) {
                            var deliveryReport = await producer.ProduceAsync(
                            topicName, new Message<string, string> { Key = finalMw.player, Value = finalMw.value }, cancellation
                            );
                        }
                    }
                    catch (ProduceException<Ignore, MessageWrapper> e)
                    {
                        Debug.LogError("Consume error: " + e.Error.Reason);
                    }
                    catch (OperationCanceledException oce)
                    {
                        Debug.Log("Exiting Producer");
                        producer.Dispose();
                        break;
                    }
                }
            }
        }

        public void SendMovement(Vector3 movement, string player)
        {
            MessageWrapper mw = new MessageWrapper();
            mw.player = player;
            mw.value = JsonUtility.ToJson(movement);
            stringReceived.Enqueue(mw);
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
    }

    class MessageWrapper
    {
        public string player;
        public string value;
    }
}
