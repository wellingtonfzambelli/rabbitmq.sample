using RabbitMQ.Client;
using System;

namespace RabbitMQ.Sample
{
    internal class Program
    {
        static void Main(string[] args)
        {
            var queueName = "test_time_to_live";

            var factory = new ConnectionFactory() { HostName = "localhost" };
            using (var connection = factory.CreateConnection())
            using (var channel = connection.CreateModel())
            {
                channel.QueueDeclare(queue: queueName, durable: false, exclusive: false, autoDelete: false, arguments: null);

                var body = Encoding.UTF8.GetBytes($"Hellog World! Date/Hour: {DateTime.Now}");

                var props = channel.CreateBasicProperties();

                props.Expiration = "20000";

                channel.BasicPublish(exchange: "", routingKey: queueName, basicProperties: props, body: body);
            }

            Console.WriteLine("Press [enter] to exit");
            Console.ReadLine();
        }
    }
}
