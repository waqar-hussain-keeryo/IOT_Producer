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

        private static bool _isFetchingData = false;
        private static readonly object _syncLock = new object();

        public IReadOnlyDictionary<Guid, BsonDocument> GetSiteCache() => _siteCache;
        public IReadOnlyDictionary<Guid, BsonDocument> GetDeviceTypeCache() => _deviceTypeCache;

        public MongoDbContext(string databaseName, string customerCollection, string deviceTypeCollection,
            Action startApplicationProcess, Action stopApplicationProcess)
        {
            var client = new MongoClient("mongodb://localhost:27017");
            _database = client.GetDatabase(databaseName);
            _customerCollection = _database.GetCollection<BsonDocument>(customerCollection);
            _deviceTypeCollection = _database.GetCollection<BsonDocument>(deviceTypeCollection);

            _startApplicationProcess = startApplicationProcess ?? throw new ArgumentNullException(nameof(startApplicationProcess));
            _stopApplicationProcess = stopApplicationProcess ?? throw new ArgumentNullException(nameof(stopApplicationProcess));

            InitializeCache().Wait();
            StartPolling();
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
                Console.WriteLine($"Connection failed: {ex.Message}");
                return false;
            }
        }

        // Get all sites from database
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
            catch (Exception ex)
            {
                Console.WriteLine($"Error fetching sites: {ex.Message}");
                return new List<BsonDocument>();
            }
        }

        // Get all deviceTypes from database
        public async Task<List<BsonDocument>> GetAllDeviceTypes()
        {
            try
            {
                return await _deviceTypeCollection.Find(new BsonDocument()).ToListAsync();
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error fetching device types: {ex.Message}");
                return new List<BsonDocument>();
            }
        }

        // Initialize Cache in-memory
        private async Task InitializeCache()
        {
            await UpdateCache();
        }

        // Update cache
        public async Task UpdateCache()
        {
            try
            {
                lock (_syncLock)
                {
                    _isFetchingData = true;
                }
                _stopApplicationProcess?.Invoke(); // Stop RabbitMQ operations

                // Fetch and update sites
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
                foreach (var key in updatedSiteCache)
                {
                    _siteCache[key.Key] = key.Value;
                }

                // Fetch and update device types
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
                foreach (var key in updatedDeviceTypeCache)
                {
                    _deviceTypeCache[key.Key] = key.Value;
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error updating cache: {ex.Message}");
            }
            finally
            {
                lock (_syncLock)
                {
                    _isFetchingData = false;
                }
                _startApplicationProcess?.Invoke(); // Resume RabbitMQ operations
            }
        }

        // Start Polling in 1 hour to fetch new records from database
        private void StartPolling()
        {
            Task.Run(async () =>
            {
                while (!_cancellationTokenSource.Token.IsCancellationRequested)
                {
                    await UpdateCache();
                    await Task.Delay(_pollingTime, _cancellationTokenSource.Token);
                }
            }, _cancellationTokenSource.Token);
        }

        // Helper method to check BSON ID and convert into Guid ID 
        private Guid GetGuidFromBsonValue(BsonValue value)
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

            throw new InvalidCastException($"Cannot convert BSON value to Guid: {value}");
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
