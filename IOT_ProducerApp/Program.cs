using MongoDB.Bson;
using Newtonsoft.Json;

namespace IOT_ProducerApp
{
    public class Program
    {
        private static Timer _timer;
        private static MongoDbContext _dbContext;
        private static TimeSpan _interval = TimeSpan.FromSeconds(5);

        public static async Task Main(string[] args)
        {
            // Check if an interval argument was provided
            if (args.Length > 0 && TimeSpan.TryParse(args[0], out var parsedInterval))
            {
                _interval = parsedInterval;
            }

            Console.WriteLine($"Sending data every {_interval.TotalSeconds} seconds.");

            _dbContext = new MongoDbContext("IOTDB", "Devices", "ProductTypes");
            bool connectionTest = await _dbContext.TestConnection();
            if (!connectionTest)
            {
                Console.WriteLine("Failed to connect to Database");
                return;
            }

            // Set up a timer to call the SendData method every interval
            _timer = new Timer(async _ => await SendData(), null, TimeSpan.Zero, _interval);

            Console.ReadLine();
        }

        private static async Task SendData()
        {
            try
            {
                if (_dbContext == null)
                {
                    Console.WriteLine("Database context is not initialized.");
                    return;
                }

                // Update cache to reflect any new changes
                await _dbContext.UpdateCache();

                // Fetch and process all Device Types
                var message = await ProcessDeviceTypes(_dbContext);
                await RMQProducer.SendMessage(message);
                Console.WriteLine(message);
                Console.WriteLine("Data sent to RabbitMQ.");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error sending data: {ex.Message}");
            }
        }

        // Helper method to process device types data and return JSON data
        private static async Task<string> ProcessDeviceTypes(MongoDbContext dbContext)
        {
            var allSites = await dbContext.GetAllSites();
            var allDeviceTypes = await dbContext.GetAllDeviceTypes();

            var deviceTypeDict = allDeviceTypes.ToDictionary(
                dt => dt.GetValue("ProductTypeID", Guid.Empty),
                dt => new
                {
                    MinVal = dt.GetValue("MinVal", 0.0).ToDouble(),
                    MaxVal = dt.GetValue("MaxVal", 0.0).ToDouble(),
                    UOM = dt.GetValue("UOM", string.Empty).ToString()
                });

            var results = new List<BsonDocument>();

            // Use a task-based approach to speed up processing
            var tasks = allSites.Select(site => Task.Run(() =>
            {
                var devices = site.GetValue("Devices", new BsonArray()).AsBsonArray;

                foreach (var device in devices)
                {
                    var deviceDoc = device.AsBsonDocument;
                    var deviceId = deviceDoc.GetValue("DeviceID", Guid.Empty).ToString();
                    var productTypeId = deviceDoc.GetValue("ProductType", Guid.Empty);

                    if (deviceTypeDict.TryGetValue(productTypeId, out var deviceType))
                    {
                        var random = new Random();
                        var randomNumber = random.NextDouble() * (deviceType.MaxVal - deviceType.MinVal) + deviceType.MinVal;
                        var formattedRandomNumber = randomNumber.ToString("F2");
                        var formattedTimestamp = DateTime.UtcNow.ToString("MM-dd-yyyy/HH:mm:tt");

                        var result = new BsonDocument
                        {
                            { "deviceId", deviceId },
                            { "temperature", formattedRandomNumber },
                            { "UOM", deviceType.UOM },
                            { "ScheduledDate", formattedTimestamp }
                        };

                        lock (results)
                        {
                            results.Add(result);
                        }
                    }
                }
            }));

            await Task.WhenAll(tasks);

            // Serialize results to JSON format
            var jsonResults = JsonConvert.SerializeObject(results, Formatting.Indented);
            return jsonResults;
        }
    }
}
