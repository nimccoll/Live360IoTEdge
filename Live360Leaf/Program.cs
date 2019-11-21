using System;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Azure.Devices.Client;
using Newtonsoft.Json.Linq;

namespace Live360Leaf
{
    class Program
    {
        static readonly string DeviceConnectionString = Environment.GetEnvironmentVariable("DEVICE_CONNECTION_STRING");
        static readonly string MessageCountEnv = Environment.GetEnvironmentVariable("MESSAGE_COUNT");
        static int messageCount = 10;

        static void Main(string[] args)
        {
            if (!string.IsNullOrWhiteSpace(MessageCountEnv))
            {
                if (!int.TryParse(MessageCountEnv, out messageCount))
                {
                    Console.WriteLine("Leaf Device: Invalid number of messages in env variable MESSAGE_COUNT. MESSAGE_COUNT set to {0}\n", messageCount);
                }
            }

            Console.WriteLine("Leaf Device: Creating device client from connection string\n");
            DeviceClient deviceClient = DeviceClient.CreateFromConnectionString(DeviceConnectionString);

            if (deviceClient == null)
            {
                Console.WriteLine("Leaf Device: Failed to create DeviceClient!");
            }
            else
            {
                SendEvents(deviceClient, messageCount).Wait();
            }

            Console.WriteLine("Leaf Device: Exiting!\n");
        }

        static async Task SendEvents(DeviceClient deviceClient, int messageCount)
        {
            Console.WriteLine("Leaf Device: Edge downstream device attempting to send {0} messages to Edge Hub...\n", messageCount);
            Random rndI2CPressure = new Random(10);
            Random rndI2CTemperature = new Random(20);
            Random rndConductivity1 = new Random(30);
            Random rndConductivity2 = new Random(40);
            Random rndFlow = new Random(50);
            Random rndPressure1 = new Random(60);
            Random rndPressure2 = new Random(70);

            while (true)
            {
                for (int count = 0; count < messageCount; count++)
                {
                    try
                    {
                        JObject deviceReading = new JObject();
                        deviceReading.Add("CollectorType", "VesselAdapter");
                        deviceReading.Add("Time", DateTime.Now.ToString());
                        deviceReading.Add("I2CPressure", rndI2CPressure.Next(100));
                        deviceReading.Add("I2CTemperature", rndI2CTemperature.Next(100));
                        deviceReading.Add("Conductivity1", rndConductivity1.Next(100));
                        deviceReading.Add("Conductivity2", rndConductivity2.Next(100));
                        deviceReading.Add("Flow", rndFlow.Next(100));
                        deviceReading.Add("Pressure1", rndPressure1.Next(100));
                        deviceReading.Add("Pressure2", rndPressure2.Next(100));

                        Message eventMessage = new Message(Encoding.UTF8.GetBytes(deviceReading.ToString()));
                        Console.WriteLine("\tLeaf Device: {0}> Sending message: {1}, Data: [{2}]", DateTime.Now.ToLocalTime(), count, deviceReading.ToString());
                        await deviceClient.SendEventAsync(eventMessage);
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine("Leaf Device: failed with the following exception. {0}", ex.Message);
                    }
                    await Task.Delay(5000);
                }
                await Task.Delay(300000);
            }
        }
    }
}
