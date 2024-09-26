using MongoDB.Bson;
using Newtonsoft.Json;
using System.Collections.Concurrent;

namespace IOT_ProducerApp
{
    public class Program
    {
        private static Timer _timer;
        private static MongoDbContext _dbContext;
        private static TimeSpan _interval = TimeSpan.FromSeconds(5);
        private static bool _isFirstRun = true;
        private static Random _random = new Random();

        public static async Task Main(string[] args)
        {
            // Validate and parse interval argument if provided
            try
            {
                if (args.Length > 0 && !TimeSpan.TryParse(args[0], out _interval))
                {
                    Console.WriteLine($"Invalid interval format: {args[0]}. Using default interval of {_interval.TotalSeconds} seconds.");
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error parsing interval: {ex.Message}. Using default interval of {_interval.TotalSeconds} seconds.");
            }

            Console.WriteLine($"Sending data every {_interval.TotalSeconds} seconds.");

            // Define actions to start and stop application processes
            Action startApplicationProcess = () => Console.WriteLine("Starting application processes...");
            Action stopApplicationProcess = () => Console.WriteLine("Stopping application processes...");

            try
            {
                // Initialize MongoDbContext
                _dbContext = new MongoDbContext(
                    databaseName: "IOTDB",
                    customerCollection: "Customer",
                    deviceTypeCollection: "ProductTypes",
                    startApplicationProcess: startApplicationProcess,
                    stopApplicationProcess: stopApplicationProcess
                );
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error initializing database context: {ex.Message}");
                return;
            }

            // Test MongoDB connection
            bool connectionTest = await _dbContext.TestConnection();
            if (!connectionTest)
            {
                Console.WriteLine("Failed to connect to Database.");
                return;
            }

            // Set up a timer to call the SendData method every interval
            _timer = new Timer(async _ => await TimerCallback(), null, TimeSpan.Zero, _interval);

            // Wait for user input to exit
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

                // Only send message to RabbitMQ if it's not empty
                if (!string.IsNullOrWhiteSpace(message))
                {
                    RMQProducer.SendMessage(message);
                    Console.WriteLine(message);
                }
                else
                {
                    Console.WriteLine("No data available to send.");
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error in TimerCallback: {ex.Message}");
                // Optionally terminate application on critical errors
            }
        }

        // Helper method to process device types data and return JSON data
        private static async Task<string> ProcessDeviceTypes(IReadOnlyDictionary<Guid, BsonDocument> siteCache, IReadOnlyDictionary<Guid, BsonDocument> deviceTypeCache)
        {
            var results = new ConcurrentBag<BsonDocument>();

            try
            {
                var tasks = siteCache.Select(async siteEntry =>
                {
                    var site = siteEntry.Value;
                    var devices = site.GetValue("Devices", new BsonArray()).AsBsonArray;

                    foreach (var device in devices)
                    {
                        var deviceDoc = device.AsBsonDocument;

                        // Check if the device is active by checking the IsDeleted field
                        var isDeleted = deviceDoc.GetValue("IsDeleted", BsonBoolean.False).AsBoolean;
                        if (isDeleted)
                        {
                            continue;
                        }

                        var deviceId = deviceDoc.GetValue("DeviceID", BsonNull.Value).ToString();
                        var productTypeId = deviceDoc.GetValue("ProductType", BsonNull.Value);

                        // Validate GUID conversion
                        Guid productTypeGuid = GetGuidFromBsonValue(productTypeId);
                        if (productTypeGuid == Guid.Empty)
                        {
                            Console.WriteLine($"Invalid ProductType ID for device {deviceId}. Skipping...");
                            continue;
                        }

                        if (deviceTypeCache.TryGetValue(productTypeGuid, out var deviceType))
                        {
                            var minVal = deviceType.GetValue("MinVal", 0.0).ToDouble();
                            var maxVal = deviceType.GetValue("MaxVal", 0.0).ToDouble();
                            var randomNumber = _random.NextDouble() * (maxVal - minVal) + minVal;
                            var formattedRandomNumber = randomNumber.ToString("F2");
                            var formattedTimestamp = DateTime.UtcNow.ToString("MM-dd-yyyy/HH:mm:tt");

                            var result = new BsonDocument
                            {
                                { "deviceid", deviceId },
                                { "value", formattedRandomNumber },
                                { "uom", deviceType.GetValue("UOM", string.Empty).ToString() },
                                { "timestamp", formattedTimestamp }
                            };

                            results.Add(result);
                        }
                    }
                });

                await Task.WhenAll(tasks); // Ensure all tasks are completed before proceeding
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error processing device types: {ex.Message}");
            }

            // Serialize results to JSON format only if there are results
            if (results.Count == 0)
            {
                return null;
            }

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

            try
            {
                return value.BsonType switch
                {
                    BsonType.ObjectId => new Guid(value.AsObjectId.ToByteArray()),
                    BsonType.String => Guid.TryParse(value.AsString, out var guid) ? guid : Guid.Empty,
                    BsonType.Binary => (value.AsBsonBinaryData.SubType == BsonBinarySubType.UuidStandard ||
                                        value.AsBsonBinaryData.SubType == BsonBinarySubType.UuidLegacy)
                                        ? new Guid(value.AsBsonBinaryData.Bytes)
                                        : Guid.Empty,
                    _ => Guid.Empty
                };
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error converting BSON value to Guid: {ex.Message}");
            }

            return Guid.Empty; // Return an empty Guid if conversion fails
        }
    }
}
