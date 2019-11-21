namespace Vessel3Collector
{
    using System;
    using System.Collections.Generic;
    using System.IO;
    using System.Linq;
    using System.Runtime.InteropServices;
    using System.Runtime.Loader;
    using System.Security.Cryptography.X509Certificates;
    using System.Text;
    using System.Threading;
    using System.Threading.Tasks;
    using HtmlAgilityPack;
    using Microsoft.Azure.Devices.Client;
    using Microsoft.Azure.Devices.Client.Transport.Mqtt;
    using Newtonsoft.Json.Linq;

    class Program
    {
        static int counter;
        private static readonly string _collectorName = "Vessel3Collector";
        private static readonly string _dataFile = "Vessel3DataSet.html";
        private static readonly string _deviceID = "FoulingBench";


        static void Main(string[] args)
        {
            Init().Wait();

            // Wait until the app unloads or is cancelled
            var cts = new CancellationTokenSource();
            AssemblyLoadContext.Default.Unloading += (ctx) => cts.Cancel();
            Console.CancelKeyPress += (sender, cpe) => cts.Cancel();
            WhenCancelled(cts.Token).Wait();
        }

        /// <summary>
        /// Handles cleanup operations when app is cancelled or unloads
        /// </summary>
        public static Task WhenCancelled(CancellationToken cancellationToken)
        {
            var tcs = new TaskCompletionSource<bool>();
            cancellationToken.Register(s => ((TaskCompletionSource<bool>)s).SetResult(true), tcs);
            return tcs.Task;
        }

        /// <summary>
        /// Initializes the ModuleClient and sets up the callback to receive
        /// messages containing temperature information
        /// </summary>
        static async Task Init()
        {
            MqttTransportSettings mqttSetting = new MqttTransportSettings(TransportType.Mqtt_Tcp_Only);
            ITransportSettings[] settings = { mqttSetting };

            // Open a connection to the Edge runtime
            ModuleClient ioTHubModuleClient = await ModuleClient.CreateFromEnvironmentAsync(settings);
            await ioTHubModuleClient.OpenAsync();
            Console.WriteLine("IoT Hub module client initialized.");

            // Register callback to be called when a message is received by the module
            await ioTHubModuleClient.SetInputMessageHandlerAsync("input1", PipeMessage, ioTHubModuleClient);

            SendVesselData(ioTHubModuleClient);
        }

        /// <summary>
        /// This method is called whenever the module is sent a message from the EdgeHub. 
        /// It just pipe the messages without any change.
        /// It prints all the incoming messages.
        /// </summary>
        static async Task<MessageResponse> PipeMessage(Message message, object userContext)
        {
            int counterValue = Interlocked.Increment(ref counter);

            var moduleClient = userContext as ModuleClient;
            if (moduleClient == null)
            {
                throw new InvalidOperationException("UserContext doesn't contain " + "expected values");
            }

            byte[] messageBytes = message.GetBytes();
            string messageString = Encoding.UTF8.GetString(messageBytes);
            Console.WriteLine($"Received message: {counterValue}, Body: [{messageString}]");

            if (!string.IsNullOrEmpty(messageString))
            {
                var pipeMessage = new Message(messageBytes);
                foreach (var prop in message.Properties)
                {
                    pipeMessage.Properties.Add(prop.Key, prop.Value);
                }
                await moduleClient.SendEventAsync("output1", pipeMessage);
                Console.WriteLine("Received message sent");
            }
            return MessageResponse.Completed;
        }

        static async void SendVesselData(ModuleClient moduleClient)
        {
            if (File.Exists(_dataFile))
            {
                Console.WriteLine("Vessel3Collector: Retrieving device data from file {0}...", _dataFile);
                HtmlDocument doc = new HtmlDocument();
                doc.Load(_dataFile);
                List<HtmlNode> nodes = doc.DocumentNode.Descendants("td").Where(x => !x.InnerHtml.Contains("table")).Where(y => !y.InnerHtml.Contains("input")).Where(z => z.InnerText != "&nbsp;").ToList();
                while (true)
                {
                    DateTime dateTime = DateTime.Now;
                    StringBuilder builder = new StringBuilder();
                    builder.Append("{");
                    builder.AppendFormat("Collector:\"{0}\",", _collectorName);
                    builder.Append("CollectorType:\"Vessel3\",");

                    builder.AppendFormat("DeviceID:\"{0}\",", _deviceID);
                    builder.AppendFormat("DateTime:\"{0}\",", dateTime);
                    for (int i = 9; i < nodes.Count; i += 7)
                    {
                        if (i > 9) builder.Append(",");
                        builder.AppendFormat("{0}:\"{1}\",", nodes[i].InnerText.Trim().Replace(" ", "_").Replace("-", "_"), nodes[i + 5].InnerText.Trim());
                        builder.AppendFormat("{0}_UOM:\"{1}\",", nodes[i].InnerText.Trim().Replace(" ", "_").Replace("-", "_"), nodes[i + 6].InnerText.Trim());
                        builder.AppendFormat("{0}_AlarmStatus1:\"{1}\",", nodes[i].InnerText.Trim().Replace(" ", "_").Replace("-", "_"), nodes[i + 1].InnerText.Trim());
                        builder.AppendFormat("{0}_AlarmStatus2:\"{1}\",", nodes[i].InnerText.Trim().Replace(" ", "_").Replace("-", "_"), nodes[i + 2].InnerText.Trim());
                        builder.AppendFormat("{0}_AlarmStatus3:\"{1}\",", nodes[i].InnerText.Trim().Replace(" ", "_").Replace("-", "_"), nodes[i + 3].InnerText.Trim());
                        builder.AppendFormat("{0}_AlarmStatus4:\"{1}\"", nodes[i].InnerText.Trim().Replace(" ", "_").Replace("-", "_"), nodes[i + 4].InnerText.Trim());
                    }
                    builder.Append("}");

                    JObject jsonLog = JObject.Parse(builder.ToString());
                    Console.WriteLine("Sending Vessel3 Telemetry");
                    await moduleClient.SendEventAsync("output1", new Message(Encoding.UTF8.GetBytes(jsonLog.ToString())));
                    Console.WriteLine("Vessel3Collector: File {0} - processed successfully.", _dataFile);
                    builder.Clear();
                    await Task.Delay(30000);
                }
            }
        }
    }
}
