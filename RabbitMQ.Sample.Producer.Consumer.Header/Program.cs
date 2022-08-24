using Newtonsoft.Json;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System;
using System.Collections.Generic;
using System.Text;

namespace RabbitMQ.Sample.Producer.Consumer.Header
{
    internal class Program
    {
        private const string HOST_NAME = "localhost";
        private const string EXCHANGE = "x.departament";
        private const string VHOST = "vh.samples";

        static void Main(string[] args)
        {
            Producer("Hello FINANCIAL departament, test message!", DepartamentType.financial);
            Producer("Hello SALES departament, test message!", DepartamentType.sales);

            Consumer("q.financial.one");
            Consumer("q.financial.two");

            Consumer("q.sales.one");
            Consumer("q.sales.two");
        }

        static void Producer(string message, DepartamentType departamentType)
        {
            var factory = new ConnectionFactory()
            {
                HostName = HOST_NAME,
                VirtualHost = VHOST
            };

            using (var connection = factory.CreateConnection())
            using (var channel = connection.CreateModel())
            {
                channel.ExchangeDeclare(EXCHANGE, ExchangeType.Headers, true, false);

                var messageQueue = new { Name = EXCHANGE, Message = message };
                var body = Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(messageQueue));

                var properties = channel.CreateBasicProperties();
                properties.Headers = new Dictionary<string, object> { { "departament", departamentType.ToString() } };
                channel.BasicPublish(EXCHANGE, string.Empty, properties, body);

                Console.WriteLine($"The message has been produced to {departamentType} departament!");
            }
        }

        static void Consumer(string queue)
        {
            var factory = new ConnectionFactory()
            {
                HostName = HOST_NAME,
                VirtualHost = VHOST
            };

            using (var connection = factory.CreateConnection())
            using (var channel = connection.CreateModel())
            {
                channel.QueueDeclare(queue: queue,
                                     durable: true,
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

                channel.BasicConsume(queue: queue,
                                     autoAck: true,
                                     consumer: consumer);
            }
        }
    }
}