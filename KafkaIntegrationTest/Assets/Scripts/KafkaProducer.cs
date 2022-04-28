using UnityEngine;
using Confluent.Kafka;
using System.Threading;
using System.Collections.Concurrent;
using System;
using System.Threading.Tasks;

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

        private ConcurrentQueue<string> stringReceived;

        // Start is called before the first frame update
        void Start()
        {
            stringReceived = new ConcurrentQueue<string>();
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
            ProducerConfig config = new ProducerConfig { BootstrapServers = bootstrapServer };

            using (var producer = new ProducerBuilder<string, string>(config)
                .Build())
            {
                while (keepRunning)
                {
                    try
                    {
                        string message;
                        while (stringReceived.TryDequeue(out message))
                        {
                            var deliveryReport = await producer.ProduceAsync(
                            topicName, new Message<string, string> { Key = "", Value = message }, cancellation
                            );
                        }

                    }
                    catch (ProduceException<Ignore, string> e)
                    {
                        stringReceived.Enqueue("Consume error: " + e.Error.Reason);
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

        public void SendMovement(Vector3 movement)
        {
            stringReceived.Enqueue("(" + movement.x.ToString().Replace(",", ".") + ", " + movement.y.ToString().Replace(",", ".") + ", " + movement.z.ToString().Replace(",", ".") + ")");
            //stringReceived.Enqueue(movement.ToString());
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
}
