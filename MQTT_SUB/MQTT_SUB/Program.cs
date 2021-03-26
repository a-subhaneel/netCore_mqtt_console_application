using MQTTnet;
using MQTTnet.Client;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Threading.Tasks;

namespace MQTT_SUB
{
    class Program
    {
        public class Content
        {
            public IList<string> format { get; set; }
            public IList<IList<string>> value { get; set; }
        }

        public class RootObject
        {
            public Content content { get; set; }
            public string type { get; set; }
            public string uuid { get; set; }
        }

        static void Main(string[] args)
        {
            Task.Run(async () =>
            {
                var factory = new MqttFactory();
                var mqttClient = factory.CreateMqttClient();

                var will = new MqttApplicationMessage() {Topic = "topic/set", Payload = System.Text.Encoding.UTF8.GetBytes("subscriber disconnected, reconnect again!"), Retain = true,
                    QualityOfServiceLevel = MQTTnet.Protocol.MqttQualityOfServiceLevel.ExactlyOnce,
                };
                var options = new MqttClientOptionsBuilder()
                .WithClientId("Subscriber CMD")
                .WithTcpServer("127.0.0.1", 1883)
                .WithCredentials(null, null)
                .WithCleanSession(false)
                .WithWillMessage(will)
                .Build();
                mqttClient.Connected += async (s, e) =>
                {
                    Console.WriteLine("### CONNECTED WITH SERVER ###");
                    // Subscribe to a topic
                    await mqttClient.SubscribeAsync(new TopicFilterBuilder().WithTopic("topic/set").Build());
                    Console.WriteLine("### SUBSCRIBED ###");
                };
                await mqttClient.ConnectAsync(options);
                mqttClient.ApplicationMessageReceived += (s, e) =>
                {
                    string fileName = "D:\\subMqtt.txt";
                    FileStream fStream = new FileStream(fileName, FileMode.OpenOrCreate, FileAccess.Write);
                    StreamWriter sWriter = new StreamWriter(fStream);
                    sWriter.AutoFlush = true;
                    Console.WriteLine("### RECEIVED APPLICATION MESSAGE ###");
                    Console.WriteLine($"+ Topic = {e.ApplicationMessage.Topic}");
                    Console.WriteLine($"+ Payload = {Encoding.UTF8.GetString(e.ApplicationMessage.Payload)}");
                    Console.WriteLine($"+ QoS = {e.ApplicationMessage.QualityOfServiceLevel}");
                    Console.WriteLine($"+ Retain = {e.ApplicationMessage.Retain}");
                    Console.WriteLine();
                    try
                    {
                        string jsonData = Encoding.UTF8.GetString(e.ApplicationMessage.Payload);
                        sWriter.Close();
                        fStream.Close();
                    }
                    catch (Exception E)
                    {
                        Console.WriteLine("ERROR MESSAGE ==>{0}", E);
                    }
                };
                Console.ReadLine();
            }).GetAwaiter().GetResult();
        }
    }
}
