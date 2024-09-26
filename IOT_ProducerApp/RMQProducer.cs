using RabbitMQ.Client;
using System.Text;
using System.Collections.Concurrent;

namespace IOT_ProducerApp
{
    public static class RMQProducer
    {
        private static IConnection _connection;
        private static IModel _channel;
        private static readonly object _lock = new object();
        private static readonly string ExchangeName = "IOTDeviceExchange";
        private static readonly string RoutingKey = "deviceKey";
        private static readonly ConcurrentQueue<string> _messageQueue = new ConcurrentQueue<string>();
        private static readonly Task _messageProcessingTask;
        private static readonly SemaphoreSlim _semaphore = new SemaphoreSlim(10); // Increase for higher concurrency
        private static readonly int MaxRetries = 3;

        static RMQProducer()
        {
            _messageProcessingTask = Task.Run(ProcessMessages);
            InitializeConnection();
        }

        private static void InitializeConnection()
        {
            lock (_lock)
            {
                if (_connection != null && _connection.IsOpen)
                {
                    return; // Already connected
                }

                try
                {
                    var hostName = Environment.GetEnvironmentVariable("RABBITMQ_HOST");
                    if (string.IsNullOrWhiteSpace(hostName))
                        throw new ArgumentException("RABBITMQ_HOST environment variable is not set.");

                    var factory = new ConnectionFactory() { HostName = hostName };
                    _connection = factory.CreateConnection();
                    _channel = _connection.CreateModel();
                    Console.WriteLine("RabbitMQ connection successfully initialized.");
                }
                catch (Exception ex)
                {
                    Console.Error.WriteLine($"Failed to initialize RabbitMQ connection: {ex.Message}");
                    throw;
                }
            }
        }

        public static void SendMessage(string message)
        {
            if (string.IsNullOrWhiteSpace(message))
            {
                Console.Error.WriteLine("Attempted to send an empty or null message.");
                throw new ArgumentNullException(nameof(message), "Message cannot be null or empty.");
            }

            _messageQueue.Enqueue(message);
        }

        private static async Task ProcessMessages()
        {
            while (true)
            {
                if (_messageQueue.TryDequeue(out var message))
                {
                    await _semaphore.WaitAsync(); // Limit concurrency
                    _ = Task.Run(async () => // Fire-and-forget
                    {
                        try
                        {
                            await SendMessageWithRetry(message);
                        }
                        catch (Exception ex)
                        {
                            Console.Error.WriteLine($"Error processing message: {ex.Message}");
                        }
                        finally
                        {
                            _semaphore.Release(); // Release the semaphore slot
                        }
                    });
                }
                else
                {
                    await Task.Delay(100); // Avoid busy-waiting
                }
            }
        }

        private static async Task<bool> SendMessageWithRetry(string message)
        {
            for (int attempts = 0; attempts < MaxRetries; attempts++)
            {
                try
                {
                    EnsureConnection();

                    var bodyMessage = Encoding.UTF8.GetBytes(message);
                    var properties = _channel.CreateBasicProperties();
                    properties.DeliveryMode = 2; // Persistent

                    _channel.BasicPublish(exchange: ExchangeName, routingKey: RoutingKey, basicProperties: properties, body: bodyMessage);
                    Console.WriteLine($"Message published to RabbitMQ: {message}");
                    return true; // Success
                }
                catch (Exception ex)
                {
                    Console.Error.WriteLine($"Attempt {attempts + 1} failed for message '{message}': {ex.Message}");
                    await Task.Delay(TimeSpan.FromSeconds(Math.Pow(2, attempts))); // Exponential backoff
                }
            }

            return false; // All attempts failed
        }

        private static void EnsureConnection()
        {
            lock (_lock)
            {
                if (_connection == null || !_connection.IsOpen)
                {
                    Console.Error.WriteLine("RabbitMQ connection is closed or unavailable. Reinitializing...");
                    InitializeConnection();
                }
            }
        }

        public static void CloseConnection()
        {
            lock (_lock)
            {
                try
                {
                    _channel?.Close();
                    _channel?.Dispose();
                    _connection?.Close();
                    _connection?.Dispose();
                    Console.WriteLine("RabbitMQ connection and channel closed.");
                }
                catch (Exception ex)
                {
                    Console.Error.WriteLine($"Failed to properly close RabbitMQ connection and channel: {ex.Message}");
                }
            }
        }
    }
}
