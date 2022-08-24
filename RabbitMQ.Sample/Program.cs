using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System;
using System.Text;

namespace RabbitMQ.Sample.Producer.Consumer
{
    internal class Program
    {
        private const string VHOST = "vh.samples";
        private const string HOST_NAME = "localhost";
        private const string EXCHANGE = "x.hello";
        private const string ROUTING_KEY = "rk.hello";

        private const string QUEUE = "q.hello";

        static void Main(string[] args)
        {
            Producer();
            Consumer();
        }

        static void Producer()
        {
            var factory = new ConnectionFactory() 
            { 
                HostName = HOST_NAME,
                VirtualHost = VHOST
            };

            using (var connection = factory.CreateConnection())
            using (var channel = connection.CreateModel())
            {
                channel.QueueDeclare(queue: QUEUE,
                                     durable: true,
                                     exclusive: false,
                                     autoDelete: false,
                                     arguments: null);

                string message = "Hello World!";
                var body = Encoding.UTF8.GetBytes(message);

                channel.BasicPublish(exchange: EXCHANGE,
                                     routingKey: ROUTING_KEY,
                                     basicProperties: null,
                                     body: body);

                Console.WriteLine($" Sent: {message}");
            }
        }

        static void Consumer()
        {
            var factory = new ConnectionFactory() 
            { 
                HostName = HOST_NAME,
                VirtualHost= VHOST
            };

            using (var connection = factory.CreateConnection())
            using (var channel = connection.CreateModel())
            {
                channel.QueueDeclare(queue: QUEUE,
                                     durable: true,
                                     exclusive: false,
                                     autoDelete: false,
                                     arguments: null);

                var consumer = new EventingBasicConsumer(channel);
                consumer.Received += (model, ea) =>
                {
                    var body = ea.Body.ToArray();
                    var message = Encoding.UTF8.GetString(body);
                    Console.WriteLine($"Received: {message}");
                };

                channel.BasicConsume(queue: QUEUE,
                                     autoAck: true,
                                     consumer: consumer);
            }
        }
    }
}
