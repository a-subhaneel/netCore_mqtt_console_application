using System;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using MQTTnet;
using MQTTnet.Client;
using MQTTnet.Client.Options;
using Newtonsoft.Json;

namespace ConsolePublisher
{
    class Program
    {
        private static IMqttClient _client;
        private static IMqttClientOptions _options;

        static void Main(string[] args)
        {
            Console.WriteLine("Starting Publisher....");
            try
            {
                var payloads = "Publisher device charge emptied.! reconnect again!!";
                var serializedPayloads = JsonConvert.SerializeObject(payloads);
                var encodedPayloads = Encoding.UTF8.GetBytes(serializedPayloads);
                // Create a new MQTT client.
                var factory = new MqttFactory();
                _client = factory.CreateMqttClient();

                var will = new MqttApplicationMessage()
                {
                    Topic = "topic/set",
                    Payload = System.Text.Encoding.UTF8.GetBytes("publisher disconnected, reconnect again!"),
                    Retain = true,
                    QualityOfServiceLevel = MQTTnet.Protocol.MqttQualityOfServiceLevel.ExactlyOnce,
                };
                //configure options
                _options = new MqttClientOptionsBuilder()
                    .WithClientId("PublisherId")
                    .WithTcpServer("127.0.0.1", 1883)
                    .WithCredentials("", "")
                    .WithCleanSession(false)
                    .WithWillMessage(will)
                    .Build();
                //handlers
                _client.UseConnectedHandler(e =>
                {
                    Console.WriteLine("Connected successfully with MQTT Brokers.");
                });
                _client.UseDisconnectedHandler(e =>
                {
                    Console.WriteLine("Disconnected from MQTT Brokers.");
                });
                _client.UseApplicationMessageReceivedHandler(e =>
                {
                    try
                    {
                        string topic = e.ApplicationMessage.Topic;
                        if (string.IsNullOrWhiteSpace(topic) == false)
                        {
                            string payload = Encoding.UTF8.GetString(e.ApplicationMessage.Payload);
                            Console.WriteLine($"Topic: {topic}. Message Received: {payload}");
                        }
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine(ex.Message, ex);
                    }
                });
                //connect
                _client.ConnectAsync(_options).Wait();
                string WillMessageCheck = Encoding.Default.GetString(_options.WillMessage.Payload);
                Console.WriteLine("Press key to publish message.");
                string readPublishMessage = Console.ReadLine();
                //simulating publish
                SimulatePublish(readPublishMessage);
                Console.WriteLine("Simulation ended! press any key to exit.");
                Console.ReadLine();
                //To keep the app running in container
                //https://stackoverflow.com/questions/38549006/docker-container-exits-immediately-even-with-console-readline-in-a-net-core-c
                Task.Run(() => Thread.Sleep(Timeout.Infinite)).Wait();
                _client.DisconnectAsync().Wait();
            }
            catch (Exception e)
            {
                Console.WriteLine(e);
                throw;
            }
        }
        //This method send messages to topic "test"
        static void SimulatePublish(string readPublishMessage)
        {
            MqttApplicationMessage willmessage = new MqttApplicationMessageBuilder()
         .WithTopic("topic/set")
         .WithPayload("publisher offline.. please reconnect!!")
         .Build();
            long counter = 0;
            while (counter < 9999999999999999)
            {
                counter++;
                var testMessage = new MqttApplicationMessageBuilder()
                    .WithTopic("topic/set")
                    .WithPayload($"Payload {readPublishMessage}: {counter}")
                    .WithExactlyOnceQoS()
                    .WithRetainFlag(true)
                    .Build();
                if (_client.IsConnected)
                {
                    Console.WriteLine($"publishing at {DateTime.UtcNow}");
                    string WillMessageCheck = Encoding.Default.GetString(willmessage.Payload);
                    _client.PublishAsync(testMessage);
                }
                Thread.Sleep(10000);
            }
        }
    }
}
