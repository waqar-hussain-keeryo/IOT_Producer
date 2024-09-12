using MongoDB.Bson;
using MongoDB.Driver;
using System.Collections.Concurrent;

namespace IOT_ProducerApp
{
    public class MongoDbContext
    {
        private readonly IMongoDatabase _database;
        private readonly IMongoCollection<BsonDocument> _customerCollection;
        private readonly IMongoCollection<BsonDocument> _siteCollection;
        private readonly IMongoCollection<BsonDocument> _deviceCollection;
        private readonly IMongoCollection<BsonDocument> _deviceTypeCollection;

        private readonly ConcurrentDictionary<Guid, BsonDocument> _deviceCache = new ConcurrentDictionary<Guid, BsonDocument>();
        private readonly ConcurrentDictionary<Guid, BsonDocument> _deviceTypeCache = new ConcurrentDictionary<Guid, BsonDocument>();

        public MongoDbContext(string databaseName, string customerCollection, string siteCollection, string deviceCollection, string deviceTypeCollection)
        {
            var client = new MongoClient("mongodb://localhost:27017");
            _database = client.GetDatabase(databaseName);
            _customerCollection = _database.GetCollection<BsonDocument>(customerCollection);
            _siteCollection = _database.GetCollection<BsonDocument>(siteCollection);
            _deviceCollection = _database.GetCollection<BsonDocument>(deviceCollection);
            _deviceTypeCollection = _database.GetCollection<BsonDocument>(deviceTypeCollection);
        }

        public async Task InitializeCache()
        {
            var devices = await _deviceCollection.Find(new BsonDocument()).ToListAsync();
            foreach (var device in devices)
            {
                var deviceId = device.GetValue("DeviceID", BsonNull.Value);
                _deviceCache[GetGuidFromBsonValue(deviceId)] = device;
            }

            var deviceTypes = await _deviceTypeCollection.Find(new BsonDocument()).ToListAsync();
            foreach (var deviceType in deviceTypes)
            {
                var productTypeId = deviceType.GetValue("ProductTypeID", BsonNull.Value);
                _deviceTypeCache[GetGuidFromBsonValue(productTypeId)] = deviceType;
            }
        }

        public async Task UpdateCache()
        {
            var newDevices = await _deviceCollection.Find(new BsonDocument()).ToListAsync();
            foreach (var device in newDevices)
            {
                var deviceId = device.GetValue("DeviceID", BsonNull.Value);
                _deviceCache[GetGuidFromBsonValue(deviceId)] = device;
            }

            var newDeviceTypes = await _deviceTypeCollection.Find(new BsonDocument()).ToListAsync();
            foreach (var deviceType in newDeviceTypes)
            {
                var productTypeId = deviceType.GetValue("ProductTypeID", BsonNull.Value);
                _deviceTypeCache[GetGuidFromBsonValue(productTypeId)] = deviceType;
            }
        }

        public IReadOnlyDictionary<Guid, BsonDocument> GetDeviceTypeCache()
        {
            return _deviceTypeCache;
        }

        private Guid GetGuidFromBsonValue(BsonValue value)
        {
            if (value == BsonNull.Value)
            {
                return Guid.Empty;
            }

            switch (value.BsonType)
            {
                case BsonType.ObjectId:
                    // Convert ObjectId to Guid if needed
                    return new Guid(value.AsObjectId.ToByteArray());

                case BsonType.String:
                    // Convert string to Guid if it's in string format
                    if (Guid.TryParse(value.AsString, out var guid))
                    {
                        return guid;
                    }
                    break;

                case BsonType.Binary:
                    // Convert Binary to Guid if the binary data represents a Guid
                    var binaryData = value.AsBsonBinaryData;
                    if (binaryData.SubType == BsonBinarySubType.UuidLegacy || binaryData.SubType == BsonBinarySubType.UuidStandard)
                    {
                        return new Guid(binaryData.Bytes);
                    }
                    break;
            }

            // Handle other cases or throw an exception if conversion is not possible
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

        public async Task<List<BsonDocument>> GetAllCustomers()
        {
            try
            {
                return await _customerCollection.Find(new BsonDocument()).ToListAsync();
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error fetching Customers: {ex.Message}");
                return new List<BsonDocument>();
            }
        }

        public async Task<List<BsonDocument>> GetAllSites()
        {
            try
            {
                var pipeline = new[]
                {
            // Unwind the Sites array
            new BsonDocument("$unwind", "$Sites"),

            // Project the required fields
            new BsonDocument("$project", new BsonDocument
            {
                { "_id", 0 },
                { "SiteID", "$Sites.SiteID" },
                { "Devices", "$Sites.Devices" }
            })
        };

                var results = await _customerCollection.Aggregate<BsonDocument>(pipeline).ToListAsync();
                return results;
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error fetching sites: {ex.Message}");
                return new List<BsonDocument>();
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
    }
}
