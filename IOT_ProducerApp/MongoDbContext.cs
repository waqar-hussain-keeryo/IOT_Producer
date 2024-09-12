using MongoDB.Bson;
using MongoDB.Driver;
using System.Collections.Concurrent;

namespace IOT_ProducerApp
{
    public class MongoDbContext
    {
        private readonly IMongoDatabase _database;
        private readonly IMongoCollection<BsonDocument> _deviceCollection;
        private readonly IMongoCollection<BsonDocument> _deviceTypeCollection;

        private readonly ConcurrentDictionary<Guid, BsonDocument> _deviceCache = new ConcurrentDictionary<Guid, BsonDocument>();
        private readonly ConcurrentDictionary<Guid, BsonDocument> _deviceTypeCache = new ConcurrentDictionary<Guid, BsonDocument>();
        private readonly TimeSpan _pollingInterval = TimeSpan.FromSeconds(5);
        private readonly CancellationTokenSource _cancellationTokenSource = new CancellationTokenSource();

        public MongoDbContext(string databaseName, string deviceCollection, string deviceTypeCollection)
        {
            var client = new MongoClient("mongodb://localhost:27017");
            _database = client.GetDatabase(databaseName);
            _deviceCollection = _database.GetCollection<BsonDocument>(deviceCollection);
            _deviceTypeCollection = _database.GetCollection<BsonDocument>(deviceTypeCollection);

            InitializeCache().Wait();
            StartPolling();
        }

        private async Task InitializeCache()
        {
            await UpdateCache();
        }

        public async Task UpdateCache()
        {
            try
            {
                var devices = await _deviceCollection.Find(new BsonDocument()).ToListAsync();
                var deviceTypes = await _deviceTypeCollection.Find(new BsonDocument()).ToListAsync();

                var updatedDeviceCache = new ConcurrentDictionary<Guid, BsonDocument>();
                foreach (var device in devices)
                {
                    var deviceId = device.GetValue("DeviceID", BsonNull.Value);
                    var id = GetGuidFromBsonValue(deviceId);
                    if (id != Guid.Empty)
                    {
                        updatedDeviceCache[id] = device;
                    }
                }

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

                // Swap caches atomically
                _deviceCache.Clear();
                foreach (var kvp in updatedDeviceCache)
                {
                    _deviceCache[kvp.Key] = kvp.Value;
                }

                _deviceTypeCache.Clear();
                foreach (var kvp in updatedDeviceTypeCache)
                {
                    _deviceTypeCache[kvp.Key] = kvp.Value;
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error updating cache: {ex.Message}");
            }
        }

        private void StartPolling()
        {
            Task.Run(async () =>
            {
                while (!_cancellationTokenSource.Token.IsCancellationRequested)
                {
                    await UpdateCache();
                    await Task.Delay(_pollingInterval, _cancellationTokenSource.Token);
                }
            }, _cancellationTokenSource.Token);
        }

        public IReadOnlyDictionary<Guid, BsonDocument> GetDeviceCache() => _deviceCache;
        public IReadOnlyDictionary<Guid, BsonDocument> GetDeviceTypeCache() => _deviceTypeCache;

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
    }
}
