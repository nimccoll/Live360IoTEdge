namespace Vessel1Collector
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
    using Microsoft.Azure.Devices.Client;
    using Microsoft.Azure.Devices.Client.Transport.Mqtt;
    using Newtonsoft.Json.Linq;

    class Program
    {
        static int counter;
        private static Dictionary<string, DeviceReadingMap> _tagMap = new Dictionary<string, DeviceReadingMap>();
        private static readonly string _collectorName = "Vessel1Collector";
        private static readonly string _mapFile = "Vessel1TagMapping.csv";
        private static readonly string _dataFile = "Vessel1DataSet.csv";

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
            LoadMap();
            if (File.Exists(_dataFile))
            {
                Console.WriteLine("Vessel1 DataSet Found!");
                List<KeyValuePair<string, DeviceReadingMap>> sortedTagMap = _tagMap.OrderBy(t => t.Value.DeviceID).ToList();
                string deviceID = string.Empty;
                StringBuilder builder = new StringBuilder();


                List<string> lines = File.ReadLines(_dataFile).ToList();
                string[] headers = lines[0].Split(',');
                while (true)
                {
                    DateTime dateTime = DateTime.Now;
                    for (int i = 1; i < lines.Count; i++)
                    {
                        string [] readings = lines[i].Split(',');

                        foreach (KeyValuePair<string, DeviceReadingMap> item in sortedTagMap)
                        {
                            if (item.Value.DeviceID != deviceID)
                            {
                                if (!string.IsNullOrEmpty(deviceID))
                                {
                                    builder.Append("}");
                                    JObject jsonLog = JObject.Parse(builder.ToString());
                                    Console.WriteLine("Sending Vessel1 Telemetry");
                                    await moduleClient.SendEventAsync("output1", new Message(Encoding.UTF8.GetBytes(jsonLog.ToString())));
                                    builder.Clear();
                                    dateTime.AddSeconds(1);
                                }
                                builder.Append("{");
                                builder.AppendFormat("Collector:\"{0}\",", _collectorName);
                                builder.Append("CollectorType:\"Vessel1\",");
                                builder.AppendFormat("DeviceID:\"{0}\",", item.Value.DeviceID);
                                builder.AppendFormat("DateTime:\"{0}\"", dateTime);

                                deviceID = item.Value.DeviceID;
                            }
                            int index = 0;
                            for (int j = 0; j < headers.Length; j++)
                            {
                                if (headers[j] == item.Key)
                                {
                                    index = j;
                                    break;
                                }
                            }
                            builder.AppendFormat(",{0}:\"{1}\",",item.Value.Channel.Replace(" ", "_").Replace("-", "_"), Convert.ToDecimal(readings[index]));
                            builder.AppendFormat("{0}_UOM:\"{1}\"", item.Value.Channel.Replace(" ", "_").Replace("-", "_"), item.Value.Unit);
                        }
                        // Write last entry
                        {
                            builder.Append("}");
                            JObject jsonLog = JObject.Parse(builder.ToString());
                            Console.WriteLine("Sending Vessel1 Telemetry");
                            await moduleClient.SendEventAsync("output1", new Message(Encoding.UTF8.GetBytes(jsonLog.ToString())));
                            builder.Clear();
                            deviceID = string.Empty;
                        }

                    }
                    await Task.Delay(60000);
                }
            }
            else
            {
                Console.WriteLine("Vessel1 DataSet Not Found!");
            }
        }

        static void LoadMap()
        {
            try
            {
                FileStream stream = File.OpenRead(_mapFile);
                string map = new StreamReader(stream).ReadToEnd();
                stream.Close();
                StringReader reader = new StringReader(map);
                string line = reader.ReadLine();
                while (line != null)
                {
                    string[] parts = line.Split(new char[] { ',' });
                    _tagMap.Add(parts[0], new DeviceReadingMap() { DeviceID = parts[1], Channel = parts[2], Unit = parts[3] });
                    line = reader.ReadLine();
                }
                Console.WriteLine("Vessel1Collector[{0}]: Map file {1} loaded successfully.", _collectorName, _mapFile);
            }
            catch (Exception ex)
            {
                Guid correlationID = Guid.NewGuid();
                Console.WriteLine("Vessel1Collector[{0}]: Correlation ID {1}. Loading of map file {2} failed. Exception: {3}", _collectorName, correlationID, _mapFile, ex.Message);
                Console.WriteLine("Vessel1Collector[{0}]: Correlation ID {1}. StackTrace: {2}", _collectorName, correlationID, ex.StackTrace);
                if (ex.InnerException != null)
                {
                    Console.WriteLine("Vessel1Collector[{0}]: Correlation ID {1}. Inner exception: {2}", _collectorName, correlationID, ex.InnerException.Message);
                    Console.WriteLine("Vessel1Collector[{0}]: Correlation ID {1}. Inner exception StackTrace: {2}", _collectorName, correlationID, ex.InnerException.StackTrace);
                }
            }

        }
        
    }

    class DeviceReadingMap
    {
        public string DeviceID { get; set; }
        public string Channel { get; set; }
        public string Unit { get; set; }
    }

}
