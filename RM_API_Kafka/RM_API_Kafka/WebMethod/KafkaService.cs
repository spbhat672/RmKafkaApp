using RM_API_Kafka.Models;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;
using RM_API_Kafka.Utils;
using KafkaNet.Model;
using KafkaNet;
using KafkaNet.Protocol;
using Newtonsoft.Json.Linq;
using Newtonsoft.Json;
using System.Text;
using Confluent.Kafka;
using System.Threading;
using System.Threading.Tasks;

namespace RM_API_Kafka.WebMethod
{
    public static class KafkaService
    {

        public static List<ResourceWithValue> GetResource(long? id)
        {
            List<ResourceWithValue> resList = new List<ResourceWithValue>();
            try
            {
                string requestTopic = ServiceTopics.rmResourceBulk;
                Uri uri = new Uri("http://localhost:9092");
                var requestOptions = new KafkaOptions(uri);
                var requestRouter = new BrokerRouter(requestOptions);
                //var consumer = new Consumer(new ConsumerOptions(requestTopic, requestRouter));

                //var config = new Dictionary<String, String>()
                //{
                //    {"group.id","test-consumer-group" },
                //    {"bootstrap.servers", "localhost:9092" },
                //    { "enable.auto.commit", "true" }
                //};


                var conf = new ConsumerConfig
                {
                    GroupId = "test-consumer-group",
                    BootstrapServers = "localhost:9092",
                    AutoOffsetReset = AutoOffsetReset.Earliest,
                    EnableAutoCommit = true,
                    SessionTimeoutMs = 60000
                };

                try
                {
                    using (var builder = new ConsumerBuilder<Ignore,
    string>(conf).Build())
                    {
                        builder.Subscribe(requestTopic);
                        var cancelToken = new CancellationTokenSource();
                        try
                        {
                            while (true)
                            {
                                Task task = Task.Factory.StartNew(() =>
                                {
                                    if (!builder.Consume(cancelToken.Token).IsPartitionEOF)
                                    {
                                        var consumerc = builder.Consume(cancelToken.Token);
                                        string msg = consumerc.Message.Value;
                                        ResourceWithValue resource = JsonConvert.DeserializeObject<ResourceWithValue>(Encoding.UTF8.GetString(Encoding.ASCII.GetBytes(msg.ToString())));
                                        resList.Add(resource);
                                    }
                                    else
                                    {
                                        cancelToken.Cancel();
                                    }
                                });
                                task.Wait(1000);
                                
                                
                                //Thread worker = new Thread(() =>
                                //{
                                //    var consumerc = builder.Consume(cancelToken.Token).IsPartitionEOF
                                //    string msg = consumerc.Message.Value;
                                //    ResourceWithValue resource = JsonConvert.DeserializeObject<ResourceWithValue>(Encoding.UTF8.GetString(Encoding.ASCII.GetBytes(msg.ToString())));
                                //    resList.Add(resource);
                                //    //Console.WriteLine($"Message: {consumerc.Message.Value} received from {consumerc.TopicPartitionOffset}");
                                //});
                                //worker.SetApartmentState(ApartmentState.STA);
                                //worker.IsBackground = false;
                                //worker.Start();
                                //if (worker.Join(TimeSpan.FromSeconds(5)))
                                //{
                                //    worker.Abort();
                                //    break;
                                //}
                                //Task task = Task.Factory.StartNew(() =>
                                //{
                                //    var consumerc = builder.Consume(cancelToken.Token);
                                //    string msg = consumerc.Message.Value;
                                //    ResourceWithValue resource = JsonConvert.DeserializeObject<ResourceWithValue>(Encoding.UTF8.GetString(Encoding.ASCII.GetBytes(msg.ToString())));
                                //    resList.Add(resource);
                                //    //Console.WriteLine($"Message: {consumerc.Message.Value} received from {consumerc.TopicPartitionOffset}");
                                //});
                                //task.Wait(1000);
                                //var x = resList;
                                //if (task.IsCompleted)
                                //    task.Dispose();
                                //CancellationTokenSource _exportCts = new CancellationTokenSource();
                                //var token = _exportCts.Token;

                                //Thread thread = new Thread(new ThreadStart(() =>
                                //{
                                //    var consumerc = builder.Consume(cancelToken.Token);
                                //    string msg = consumerc.Message.Value;
                                //    ResourceWithValue resource = JsonConvert.DeserializeObject<ResourceWithValue>(Encoding.UTF8.GetString(Encoding.ASCII.GetBytes(msg.ToString())));
                                //    resList.Add(resource);
                                //    //Console.WriteLine($"Message: {consumerc.Message.Value} received from {consumerc.TopicPartitionOffset}");
                                //}));
                                //thread.SetApartmentState(ApartmentState.STA);
                                //thread.IsBackground = false;
                                ////_exportCts.CancelAfter(1000);
                                //thread.Start();
                                //Thread.Sleep(1000);
                                //if (builder.)
                                //{
                                //    thread.Abort();
                                //    break;
                                //}

                            }
                        }
                        catch (Exception ex)
                        {
                            //builder.Close();
                            builder.Dispose();
                        }
                    }
                }
                catch (Exception ex)
                {

                }


                //    try
                //{

                //    var consumeF = new ConsumerBuilder<string, string>(new Dictionary<string, string>
                //                            {
                //                                { "bootstrap.servers", "localhost:9092" },
                //                                { "group.id", "test-consumer-group" },
                //                                { "auto.offset.reset", "latest"},
                //                                { "compression.codec", "gzip" },
                //                                { "enable.auto.commit", "true" }
                //                            }).Build();

                //        consumeF.Subscribe("rmResourceBulk");

                //    ConsumeResult<string,string> result;
                //    while (true)
                //    {
                //        result = consumeF.Consume();
                //        ResourceWithValue resource = JsonConvert.DeserializeObject<ResourceWithValue>(Encoding.UTF8.GetString(Encoding.ASCII.GetBytes(result.ToString())));
                //        resList.Add(resource);
                //        if (result == null || result.Message.Key == null || result.Message.Value == null)
                //        {
                //            System.Console.WriteLine("that's it");
                //            break;                            
                //        }                        
                //        //System.Console.WriteLine(cr.Message.Key + ": " + cr.Message.Value);
                //    }

                //var config = new Dictionary<String, String>()
                //{
                //    {"group.id","test-consumer-group" },
                //    {"bootstrap.servers", "localhost:9092" },
                //    { "enable.auto.commit", "true" }
                //};

                //var consumerr = (IConsumer<string,string>)(new ConsumerBuilder<string, string>(config).Build());
                //var res = consumerr.Consume();
                //while (!res.IsPartitionEOF)
                //{
                //    string msg = res.Message.Value;
                //    ResourceWithValue resource = JsonConvert.DeserializeObject<ResourceWithValue>(Encoding.UTF8.GetString(Encoding.ASCII.GetBytes(msg.ToString())));
                //    resList.Add(resource);
                //}

                //using (var consumerrr = new ConsumerBuilder<Ignore, string>(new ConsumerConfig(config)).Build())
                //{
                //    consumerrr.Subscribe("rmResourceBulk");
                //    foreach(var message in consumerrr.Consume().Message.ToString())
                //    {
                //        ResourceWithValue resource = JsonConvert.DeserializeObject<ResourceWithValue>(Encoding.UTF8.GetString(Encoding.ASCII.GetBytes(message.ToString())));
                //        resList.Add(resource);
                //    }
                //}
            }
            catch (Exception ex)
            {
                //
            }
                

                //IEnumerable<Message> bulk = null;

                //    try
                //    {
                //        bulk = consumer.Consume().
                //    }
                //    catch (Exception e)
                //    {
                //        Console.WriteLine($"Exception caught:\n {e.StackTrace}");
                //    }
                //    finally
                //    {
                //        consumer.Dispose();
                //    }

                //if (bulk != null){

                //    foreach (var message in bulk)
                //    {
                //        ResourceWithValue resource = JsonConvert.DeserializeObject<ResourceWithValue>(Encoding.UTF8.GetString(Encoding.ASCII.GetBytes(message.ToString())));
                //        resList.Add(resource);
                //    }
                //}
                //var config = new ConsumerConfig();
                //{
                //    GroupId = "test-consumer-group",
                //    BootstrapServers = "34.196.151.95:9092,3.213.212.236:9092,18.213.93.46:9092",
                //    SslCaLocation = "/PathTO/cluster-ca-certificate.pem",
                //    SecurityProtocol = SecurityProtocol.SaslSsl,
                //    SaslMechanism = SaslMechanism.ScramSha256,
                //    SaslUsername = "ickafka",
                //    SaslPassword = "493f586cc469a59987c8a9148669b9ecc570bb031bf3fa639e894be185331dce",
                //    AutoOffsetReset = AutoOffsetReset.Earliest,
                //};

                //using (var c = new ConsumerBuilder<Ignore, string>(config).Build())
                //{
                //    c.Subscribe(requestTopic);

                //    CancellationTokenSource cts = new CancellationTokenSource();
                //    Console.CancelKeyPress += (_, e) =>
                //    {
                //        e.Cancel = true; // prevent the process from terminating.
                //        cts.Cancel();
                //    };

                //    try
                //    {
                //        while (true)
                //        {
                //            try
                //            {
                //                var cr = c.Consume(cts.Token);
                //                //foreach (var message in cr.Value.as)
                //                //{
                //                //    ResourceWithValue resource = JsonConvert.DeserializeObject<ResourceWithValue>(Encoding.UTF8.GetString(Encoding.ASCII.GetBytes(message.ToString())));
                //                //    resList.Add(resource);
                //                //}
                //                //Console.WriteLine($"Consumed message '{cr.Value}' at: '{cr.TopicPartitionOffset}'.");
                //            }
                //            catch (ConsumeException e)
                //            {
                //                Console.WriteLine($"Error occurred: {e.Error.Reason}");

                //            }
                //        }
                //    }
                //    catch (OperationCanceledException)
                //    {
                //        // Ensure the consumer leaves the group cleanly and final offsets are committed.
                //        c.Close();
                //    }
                //}
                //    using (var consumerr = new ConsumerBuilder<Ignore, string>(consumerConfig).Build())
                //{
                //    consumerr.Subscribe("");
                //    var consumeResult = consumerr.Consume().Value;
                //    if (consumeResult.Length > 0)
                //    { 
                //        foreach(var message in consumeResult)
                //        {
                //            ResourceWithValue resource = JsonConvert.DeserializeObject<ResourceWithValue>(Encoding.UTF8.GetString(Encoding.ASCII.GetBytes(message.ToString())));
                //            resList.Add(resource);
                //        }
                //    }
                //}
                //long msgCount = bulk.LongCount<Message>();
                //int index = 0;
                //while(true)
                //{

                //    if()
                //}
                //foreach(var message in bulk)
                //{
                //    ResourceWithValue resource = JsonConvert.DeserializeObject<ResourceWithValue>(Encoding.UTF8.GetString(message.Value));
                //    resList.Add(resource);
                //    if()
                //}



                //var y = bulk


                if (id != null && resList != null && resList.Count > 0)
                    return resList.Where(x => x.Id == id).GroupBy(x => x.Id).Select(x => x.LastOrDefault()).ToList<ResourceWithValue>();
                else if (resList != null && resList.Count > 0)
                    return resList.GroupBy(x => x.Id).Select(x => x.LastOrDefault()).ToList<ResourceWithValue>();
            //catch(Exception ex)
            //{
            //    Console.WriteLine("Error reading bulk to kafka server" + Environment.NewLine + ex.Message);
            //}
            return resList;
        }


        public static void PostResource(ResourceWithValue resource)
        {
            try
            {
                JObject jsonObj = JObject.FromObject(resource);
                string payload = jsonObj.ToString();
                string topic = ServiceTopics.rmResourceBulk;
                Message msg = new Message(payload);
                Uri uri = new Uri("http://localhost:9092");
                var options = new KafkaOptions(uri);
                var router = new BrokerRouter(options);
                var client = new Producer(router);
                client.SendMessageAsync(topic, new List<Message> { msg }).Wait();
            }
            catch (Exception ex)
            {
                Console.WriteLine("Error saving bulk to kafka server" + Environment.NewLine + ex.Message);
            }
        }
    }


}
