using MongoDB.Bson;
using MongoDB.Driver;
using System.Collections.Concurrent;

namespace IOT_ProducerApp
{
    public class MongoDbContext
    {
        private readonly IMongoDatabase _database;
        private readonly IMongoCollection<BsonDocument> _customerCollection;
        private readonly IMongoCollection<BsonDocument> _deviceTypeCollection;

        private readonly TimeSpan _pollingTime = TimeSpan.FromHours(1);
        private readonly CancellationTokenSource _cancellationTokenSource = new CancellationTokenSource();

        private readonly Action _startApplicationProcess;
        private readonly Action _stopApplicationProcess;

        private readonly ConcurrentDictionary<Guid, BsonDocument> _siteCache = new ConcurrentDictionary<Guid, BsonDocument>();
        private readonly ConcurrentDictionary<Guid, BsonDocument> _deviceTypeCache = new ConcurrentDictionary<Guid, BsonDocument>();

        private static readonly SemaphoreSlim _cacheLock = new SemaphoreSlim(1, 1);

        public IReadOnlyDictionary<Guid, BsonDocument> GetSiteCache() => _siteCache;
        public IReadOnlyDictionary<Guid, BsonDocument> GetDeviceTypeCache() => _deviceTypeCache;

        public MongoDbContext(string databaseName, string customerCollection, string deviceTypeCollection,
            Action startApplicationProcess, Action stopApplicationProcess)
        {
            var mongoConnectionString = Environment.GetEnvironmentVariable("MONGO_DB_CONNECTION_STRING");

            // Validate the MongoDB connection string
            if (string.IsNullOrWhiteSpace(mongoConnectionString))
            {
                throw new ArgumentException("MONGO_DB_CONNECTION_STRING environment variable is not set or empty.");
            }

            try
            {
                var client = new MongoClient(mongoConnectionString);
                _database = client.GetDatabase(databaseName);
                _customerCollection = _database.GetCollection<BsonDocument>(customerCollection);
                _deviceTypeCollection = _database.GetCollection<BsonDocument>(deviceTypeCollection);
            }
            catch (MongoConfigurationException ex)
            {
                Console.Error.WriteLine($"Invalid MongoDB configuration: {ex.Message}");
                throw new InvalidOperationException("Failed to initialize MongoDB context.", ex);
            }
            catch (Exception ex)
            {
                Console.Error.WriteLine($"Failed to initialize MongoDB connection: {ex.Message}");
                throw;
            }

            _startApplicationProcess = startApplicationProcess ?? throw new ArgumentNullException(nameof(startApplicationProcess));
            _stopApplicationProcess = stopApplicationProcess ?? throw new ArgumentNullException(nameof(stopApplicationProcess));

            InitializeCache().Wait();
            StartPolling();
            WatchDeviceTypeCollection();
        }

        // Test database connection
        public async Task<bool> TestConnection()
        {
            try
            {
                var command = new BsonDocument("ping", 1);
                await _database.RunCommandAsync<BsonDocument>(command);
                return true;
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Connection test failed: {ex.Message}");
                return false;
            }
        }

        // Get all sites from the database, with error handling
        public async Task<List<BsonDocument>> GetAllSites()
        {
            try
            {
                var pipeline = new[]
                {
                    new BsonDocument("$unwind", "$Sites"),
                    new BsonDocument("$project", new BsonDocument
                    {
                        { "_id", 0 },
                        { "SiteID", "$Sites.SiteID" },
                        { "Devices", "$Sites.Devices" }
                    })
                };

                return await _database.GetCollection<BsonDocument>("Customers").Aggregate<BsonDocument>(pipeline).ToListAsync();
            }
            catch (MongoException ex)
            {
                Console.WriteLine($"MongoDB error fetching sites: {ex.Message}");
                return new List<BsonDocument>();
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error fetching sites: {ex.Message}");
                return new List<BsonDocument>();
            }
        }

        // Get all device types from the database
        public async Task<List<BsonDocument>> GetAllDeviceTypes()
        {
            try
            {
                return await _deviceTypeCollection.Find(new BsonDocument()).ToListAsync();
            }
            catch (MongoException ex)
            {
                Console.WriteLine($"MongoDB error fetching device types: {ex.Message}");
                return new List<BsonDocument>();
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error fetching device types: {ex.Message}");
                return new List<BsonDocument>();
            }
        }

        // Initialize in-memory cache
        private async Task InitializeCache()
        {
            await UpdateCache();
        }

        // Update cache with thread safety
        public async Task UpdateCache()
        {
            await _cacheLock.WaitAsync();
            try
            {
                _stopApplicationProcess?.Invoke(); // Stop RabbitMQ or other operations

                // Update site cache
                var sites = await GetAllSites();
                var updatedSiteCache = new ConcurrentDictionary<Guid, BsonDocument>();
                foreach (var site in sites)
                {
                    var siteId = site.GetValue("SiteID", BsonNull.Value);
                    var id = GetGuidFromBsonValue(siteId);
                    if (id != Guid.Empty)
                    {
                        updatedSiteCache[id] = site;
                    }
                }

                _siteCache.Clear();
                foreach (var entry in updatedSiteCache)
                {
                    _siteCache[entry.Key] = entry.Value;
                }

                // Update device type cache
                var deviceTypes = await GetAllDeviceTypes();
                var updatedDeviceTypeCache = new ConcurrentDictionary<Guid, BsonDocument>();
                foreach (var deviceType in deviceTypes)
                {
                    var productTypeId = deviceType.GetValue("ProductTypeID", BsonNull.Value);
                    var id = GetGuidFromBsonValue(productTypeId);
                    if (id != Guid.Empty)
                    {
                        updatedDeviceTypeCache[id] = deviceType;
                    }
                }

                _deviceTypeCache.Clear();
                foreach (var entry in updatedDeviceTypeCache)
                {
                    _deviceTypeCache[entry.Key] = entry.Value;
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error updating cache: {ex.Message}");
            }
            finally
            {
                _startApplicationProcess?.Invoke(); // Resume RabbitMQ or other operations
                _cacheLock.Release();
            }
        }

        // Watch for new devices being added
        public void WatchDeviceTypeCollection()
        {
            Task.Run(async () =>
            {
                var changeStreamOptions = new ChangeStreamOptions
                {
                    FullDocument = ChangeStreamFullDocumentOption.UpdateLookup
                };

                using (var changeStream = _deviceTypeCollection.Watch(changeStreamOptions))
                {
                    await changeStream.ForEachAsync(change =>
                    {
                        if (change.OperationType == ChangeStreamOperationType.Insert)
                        {
                            Console.WriteLine("New device added, updating cache...");
                            UpdateCache().Wait(); // You might want to make this async properly
                        }
                    });
                }
            });
        }

        // Polling method to periodically fetch records from the database
        private void StartPolling()
        {
            Task.Run(async () =>
            {
                while (!_cancellationTokenSource.Token.IsCancellationRequested)
                {
                    await UpdateCache();
                    try
                    {
                        await Task.Delay(_pollingTime, _cancellationTokenSource.Token);
                    }
                    catch (TaskCanceledException) { }
                }
            }, _cancellationTokenSource.Token);
        }

        // Helper method to convert BSON ID to Guid
        private Guid GetGuidFromBsonValue(BsonValue value)
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
                    _ => throw new InvalidCastException($"Cannot convert BSON value to Guid: {value}")
                };
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error converting BSON value to Guid: {ex.Message}");
                return Guid.Empty;
            }
        }

        public void StartApplicationProcess()
        {
            _startApplicationProcess?.Invoke();
        }

        public void StopApplicationProcess()
        {
            _stopApplicationProcess?.Invoke();
        }
    }
}
