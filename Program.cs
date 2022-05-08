using Amazon;
using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.DocumentModel;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Http;
using System.Text.RegularExpressions;
using System.Threading.Tasks;

namespace DynamoDbTransfer
{
    class Program
    {
        static void Main(string[] args)
        {
            // If your table does not have auto scale, the process can increase/decrease the capacity for you
            bool increaseCapacity = args.Any(a => a == "-d");

            // Specify here the credentials to the origin and target
            AmazonDynamoDBClient clientOrigin = new AmazonDynamoDBClient(awsAccessKeyId: "", awsSecretAccessKey: "", RegionEndpoint.USEast1);
            AmazonDynamoDBClient clientTarget = new AmazonDynamoDBClient(awsAccessKeyId: "", awsSecretAccessKey: "", RegionEndpoint.USEast1);

            // Bellow you can see some sample tables specifications, deppending on the table attributtes, you need to find the optimum configurations for each one.
            // For tables with bunch of registers, I recommend to use more capacitity with a pagesize bigger than the batchsize, it will produce more threads copying each
            // page.
            // You can also use beforeToCopyAction parameter to customize parameters, filter out items and other operations before to copy to the target. It can be useful
            // if you want to set TTLs for example.
            List<TableWorker> workers = new List<TableWorker>() {
                new TableWorker("Table1", clientOrigin, clientTarget),
                new TableWorker("Table2", clientOrigin, clientTarget, capacityIncrease: 160, batchSize: 20, pageSize: 100),
                new TableWorker("Table3", clientOrigin, clientTarget, beforeToCopyAction: buildBeforeCopyFor()),                
            };

            List<Task> tasks = new List<Task>();

            foreach (var worker in workers)
            {
                tasks.Add(worker.CopyData(increaseCapacity));
            }

            Task.WhenAll(tasks).Wait();
        }

        private static Func<Document, bool> buildBeforeCopyFor()
        {
            return document => {

                // Just a sample function, if you desire to filter out some registers, you can use a similar function to do it,
                // The function has a bool return, if it returns false, the item WILL not be copied to the target.
                // If you need to add or update some attribute, it's a good option as well.

                return true;
            };
        }
    }
}
