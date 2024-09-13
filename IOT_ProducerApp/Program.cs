using MongoDB.Bson;
using Newtonsoft.Json;
using System.Collections.Concurrent;
using System.Threading.Tasks;
using System.Threading;

namespace IOT_ProducerApp
{
    public class Program
    {
        private static Timer _timer;
        private static MongoDbContext _dbContext;
        private static TimeSpan _interval = TimeSpan.FromSeconds(5);
        private static bool _isFirstRun = true;

        public static async Task Main(string[] args)
        {
            // Check if an interval argument was provided
            if (args.Length > 0 && TimeSpan.TryParse(args[0], out var parsedInterval))
            {
                _interval = parsedInterval;
            }

            Console.WriteLine($"Sending data every {_interval.TotalSeconds} seconds.");

            // Define actions to start and stop application processes
            Action startApplicationProcess = () => Console.WriteLine("Starting application processes...");
            Action stopApplicationProcess = () => Console.WriteLine("Stopping application processes...");

            // Initialize MongoDbContext
            _dbContext = new MongoDbContext(
                databaseName: "IOTDB",
                customerCollection: "Customer",
                deviceTypeCollection: "ProductTypes",
                startApplicationProcess: startApplicationProcess,
                stopApplicationProcess: stopApplicationProcess
            );

            bool connectionTest = await _dbContext.TestConnection();
            if (!connectionTest)
            {
                Console.WriteLine("Failed to connect to Database");
                return;
            }

            // Set up a timer to call the SendData method every interval
            _timer = new Timer(async _ => await TimerCallback(), null, TimeSpan.Zero, _interval);

            Console.ReadLine();
        }

        private static async Task TimerCallback()
        {
            try
            {
                if (_dbContext == null)
                {
                    Console.WriteLine("Database context is not initialized.");
                    return;
                }

                if (_isFirstRun)
                {
                    // Stop application processes (if any) to ensure exclusive access
                    _dbContext.StopApplicationProcess();

                    // Fetch data from database and update cache
                    await _dbContext.UpdateCache();

                    // Start application processes again
                    _dbContext.StartApplicationProcess();

                    _isFirstRun = false; // Set to false after the first run
                }

                // Fetch data from cache
                var siteCache = _dbContext.GetSiteCache();
                var deviceTypeCache = _dbContext.GetDeviceTypeCache();

                // Process device types using cached data
                var message = await ProcessDeviceTypes(siteCache, deviceTypeCache);

                // Send message to RabbitMQ
                await RMQProducer.SendMessage(message);
                Console.WriteLine(message);
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error sending data: {ex.Message}");
            }
        }

        // Helper method to process device types data and return JSON data
        private static async Task<string> ProcessDeviceTypes(IReadOnlyDictionary<Guid, BsonDocument> siteCache, IReadOnlyDictionary<Guid, BsonDocument> deviceTypeCache)
        {
            var results = new ConcurrentBag<BsonDocument>();

            // Use a task-based approach to speed up processing
            var tasks = siteCache.Select(async siteEntry =>
            {
                var site = siteEntry.Value;
                var devices = site.GetValue("Devices", new BsonArray()).AsBsonArray;

                foreach (var device in devices)
                {
                    var deviceDoc = device.AsBsonDocument;
                    var deviceId = deviceDoc.GetValue("DeviceID", BsonNull.Value).ToString();
                    var productTypeId = deviceDoc.GetValue("ProductType", BsonNull.Value);

                    // Handle BSON to GUID conversion
                    Guid productTypeGuid = GetGuidFromBsonValue(productTypeId);

                    if (deviceTypeCache.TryGetValue(productTypeGuid, out var deviceType))
                    {
                        var random = new Random();
                        var minVal = deviceType.GetValue("MinVal", 0.0).ToDouble();
                        var maxVal = deviceType.GetValue("MaxVal", 0.0).ToDouble();
                        var randomNumber = random.NextDouble() * (maxVal - minVal) + minVal;
                        var formattedRandomNumber = randomNumber.ToString("F2");
                        var formattedTimestamp = DateTime.UtcNow.ToString("MM-dd-yyyy/HH:mm:tt");

                        var result = new BsonDocument
                        {
                            { "deviceId", deviceId },
                            { "temperature", formattedRandomNumber },
                            { "UOM", deviceType.GetValue("UOM", string.Empty).ToString() },
                            { "ScheduledDate", formattedTimestamp }
                        };

                        results.Add(result);
                    }
                }
            });

            await Task.WhenAll(tasks); // Ensure all tasks are completed before proceeding

            // Serialize results to JSON format
            var jsonResults = JsonConvert.SerializeObject(results, Formatting.Indented);
            return jsonResults;
        }

        // Helper method to convert BSON value to Guid
        private static Guid GetGuidFromBsonValue(BsonValue value)
        {
            if (value == BsonNull.Value)
            {
                return Guid.Empty;
            }

            switch (value.BsonType)
            {
                case BsonType.ObjectId:
                    return new Guid(value.AsObjectId.ToByteArray());

                case BsonType.String:
                    return Guid.TryParse(value.AsString, out var guid) ? guid : Guid.Empty;

                case BsonType.Binary:
                    var binaryData = value.AsBsonBinaryData;
                    if (binaryData.SubType == BsonBinarySubType.UuidStandard || binaryData.SubType == BsonBinarySubType.UuidLegacy)
                    {
                        return new Guid(binaryData.Bytes);
                    }
                    break;
            }

            return Guid.Empty; // Return an empty Guid if conversion fails
        }
    }
}
