using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System;
using System.Text;

namespace RabbitMQ.Sample.Producer.Consumer
{
    internal class Program
    {
        private const string HOST_NAME = "localhost";
        private const string QUEUE = "q.hello";
        private const string EXCHANGE = "x.hello";
        private const string ROUTING_KEY = "rk.hello";

        static void Main(string[] args)
        {
            Producer();
            Consumer();
        }

        static void Producer()
        {
            var factory = new ConnectionFactory() { HostName = HOST_NAME};
            using (var connection = factory.CreateConnection())
            using (var channel = connection.CreateModel())
            {
                channel.QueueDeclare(queue: QUEUE,
                                     durable: false,
                                     exclusive: false,
                                     autoDelete: false,
                                     arguments: null);

                string message = "Hello World!";
                var body = Encoding.UTF8.GetBytes(message);

                channel.BasicPublish(exchange: EXCHANGE,
                                     routingKey: ROUTING_KEY,
                                     basicProperties: null,
                                     body: body);

                Console.WriteLine($" Sent {message}");
            }

            Console.WriteLine(" Press [enter] to exit.");
            Console.ReadLine();
        }

        static void Consumer()
        {
            var factory = new ConnectionFactory() { HostName = HOST_NAME };
            using (var connection = factory.CreateConnection())
            using (var channel = connection.CreateModel())
            {
                channel.QueueDeclare(queue: QUEUE,
                                     durable: false,
                                     exclusive: false,
                                     autoDelete: false,
                                     arguments: null);

                var consumer = new EventingBasicConsumer(channel);
                consumer.Received += (model, ea) =>
                {
                    var body = ea.Body.ToArray();
                    var message = Encoding.UTF8.GetString(body);
                    Console.WriteLine($" Received: {message}");
                };

                channel.BasicConsume(queue: QUEUE,
                                     autoAck: true,
                                     consumer: consumer);

                Console.WriteLine(" Press [enter] to exit.");
                Console.ReadLine();
            }
        }
    }
}
