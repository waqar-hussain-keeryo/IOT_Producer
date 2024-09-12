using RabbitMQ.Client;
using System.Text;

namespace IOT_ProducerApp
{
    public static class RMQProducer
    {
        private static IConnection _connection;
        private static IModel _channel;

        static RMQProducer()
        {
            var factory = new ConnectionFactory() { HostName = "localhost" };
            _connection = factory.CreateConnection();
            _channel = _connection.CreateModel();
            //_channel.ExchangeDeclare(exchange: "AmazonFanoutExchange", type: "fanout");
        }

        public static async Task SendMessage(string message)
        {
            await Task.Run(() =>
            {
                var bodyMessage = Encoding.UTF8.GetBytes(message);
                _channel.BasicPublish(exchange: "IOTDeviceExchange", routingKey: "deviceKey", body: bodyMessage);
            });
        }
    }
}
