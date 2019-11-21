namespace Vessel2Collector
{
    using System;
    using System.Collections.Generic;
    using System.IO;
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
        private static readonly string _collectorName = "Vessel2Collector";
        private static readonly string _dataFile = "Vessel2DataSet.csv";

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
                FileStream stream = null;
                string log = string.Empty;
                try
                {
                    stream = File.OpenRead(_dataFile);
                    log = new StreamReader(stream).ReadToEnd();
                    stream.Close();

                    while (true)
                    {
                        // Parse
                        bool readingMeasurements = false;
                        List<string> channels = new List<string>();
                        List<string> tags = new List<string>();
                        List<string> units = new List<string>();
                        string deviceType = string.Empty;
                        string deviceID = string.Empty;
                        StringBuilder builder = new StringBuilder();
                        StringReader reader = new StringReader(log);
                        DateTime dateTime = DateTime.Now;
                        string line = reader.ReadLine();
                        while (line != null)
                        {
                            string[] data = line.Split(new char[] { ',' }, StringSplitOptions.RemoveEmptyEntries);
                            if (data != null && data.Length > 0) // Skip empty lines
                            {
                                if (!readingMeasurements)
                                {
                                    if (data[0] == "Tag")
                                    {
                                        for (int i = 1; i < data.Length; i++)
                                        {
                                            tags.Add(data[i].Trim().Replace(" ", "_").Replace("-", "_"));
                                        }
                                    }
                                    else if (data[0] == "Unit")
                                    {
                                        for (int i = 1; i < data.Length; i++)
                                        {
                                            if (data[i].Trim() == "?S/cm") data[i] = "uS/cm";
                                            units.Add(data[i].Trim());
                                        }
                                    }
                                    else if (data[0] == "Date")
                                    {
                                        readingMeasurements = true;
                                    }
                                    else
                                    {
                                        if (data[0] == "Device Type")
                                        {
                                            deviceType = data[1].Trim();
                                        }
                                        else if (data[0] == "Serial No.")
                                        {
                                            deviceID = data[1].Trim();
                                        }
                                    }
                                }
                                else
                                {
                                    builder.Append("{");
                                    builder.AppendFormat("Collector:\"{0}\",", _collectorName);
                                    builder.Append("CollectorType:\"Vessel2\",");
                                    builder.AppendFormat("DeviceID:\"{0}\",", deviceID);
                                    builder.AppendFormat("DeviceType:\"{0}\",", deviceType);
                                    builder.AppendFormat("DateTime:\"{0}\",", dateTime);
                                    for (int i = 3; i < data.Length; i++)
                                    {
                                        if (i % 2 == 0)
                                        {
                                            builder.AppendFormat("{0}_MAX:{1},", tags[(i - 3) / 2], data[i]);
                                            builder.AppendFormat("{0}_UOM:\"{1}\"", tags[(i - 3) / 2], units[(i - 3) / 2].Trim());
                                        }
                                        else
                                        {
                                            if (i > 3) builder.Append(",");
                                            builder.AppendFormat("{0}_MIN:{1},", tags[(i - 3) / 2], data[i]);
                                        }
                                    }

                                    builder.Append("}");
                                    JObject jsonLog = JObject.Parse(builder.ToString());
                                    Console.WriteLine("Sending Vessel2 Telemetry");
                                    await moduleClient.SendEventAsync("output1", new Message(Encoding.UTF8.GetBytes(jsonLog.ToString())));
                                }
                            }
                            builder.Clear();
                            dateTime.AddSeconds(1);
                            line = reader.ReadLine();
                        }
                        Console.WriteLine("Vessel2Collector: File {0} - processed successfully.", _dataFile);
                        await Task.Delay(600000);
                    }
                }
                catch (Exception ex)
                {
                    Guid correlationID = Guid.NewGuid();
                    Console.WriteLine("Vessel2Collector: Correlation ID {0}. Processing of file {1} failed. Exception: {2}", correlationID, _dataFile, ex.Message);
                    Console.WriteLine("Vessel2Collector: Correlation ID {0}. StackTrace: {1}", correlationID, ex.StackTrace);
                    if (ex.InnerException != null)
                    {
                        Console.WriteLine("Vessel2Collector: Correlation ID {0}. Inner exception: {0}", correlationID, ex.InnerException.Message);
                        Console.WriteLine("Vessel2Collector: Correlation ID {0}. Inner exception StackTrace: {0}", correlationID, ex.InnerException.StackTrace);
                    }
                }
                finally
                {
                    if (stream != null) stream.Close();
                }
            }
            else
            {
                Console.WriteLine("Vessel2 DataSet Not Found!");
            }
        }

        private static bool IsNumeric(string val)
        {
            Double result;
            return Double.TryParse(val, out result);
        }
    }
}
