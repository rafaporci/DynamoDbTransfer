using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.DocumentModel;
using System;
using System.Linq;
using System.Threading.Tasks;

namespace DynamoDbTransfer
{
    public class TableWorker
    {
        private readonly TableReader tableReader;
        private readonly TableWriter tableWriter;
        private readonly string tableName;
        private readonly AmazonDynamoDBClient clientTarget;

        private long persistedCounter = 0;
        private long scannedCounter = 0;
        private int capacityIncrease = 0;

        /// <param name="capacityIncrease">If increaseCapacity parameter is provided, the target will have capacity increaded with [capacityIncrease} unities in beginning of the task, decreasing in the end</param>
        /// <param name="batchSize">Number of items written using the BatchWrite operation</param>
        /// <param name="pageSize">Number of pages written per time (if the pageSize is greather than batchSize, you will have tasks in parallel writing each page, e.g. pageSize 60, batchSize: 20 = 3 tasks writting 20 items</param>
        /// <param name="beforeToCopyAction">Action to run before the copy, if the funcion returns false, the document will not be copied</param>
        public TableWorker(string tableName, AmazonDynamoDBClient clientOrigin, AmazonDynamoDBClient clientTarget, int capacityIncrease = 40, int batchSize = 22, int pageSize = 22, Func<Document, bool> beforeToCopyAction = null)
        {
            this.tableReader = new TableReader(clientOrigin, tableName);
            this.tableWriter = new TableWriter(clientTarget, tableName, batchSize, pageSize, beforeToCopyAction);
            this.tableName = tableName;
            this.clientTarget = clientTarget;
            this.capacityIncrease = capacityIncrease;
        }

        public async Task CopyData(bool updateDynamoDbCapacity = true)
        {
            bool capacityIncreased = true;

            try
            { 
                long itemCountOrigin = await this.tableReader.GetItemCount();

                if (updateDynamoDbCapacity)
                {
                    await this.changeDynamoDbCapacity(this.tableName, this.capacityIncrease);
                    capacityIncreased = true;
                }

                string token = null;
                while (true)
                {
                    var page = await this.tableReader.FetchDataPage(token);
                    scannedCounter += page.Documents.Count();

                    persistedCounter += await this.tableWriter.PersistDataPage(page);

                    this.showStatus(scannedCounter, persistedCounter, itemCountOrigin);

                    if (page.IsLastPage)
                        break;

                    token = page.PaginationToken;
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Fatal error {this.tableName}: {ex.Message}");
            }
            finally
            {
                if (updateDynamoDbCapacity && capacityIncreased)
                {
                    await this.changeDynamoDbCapacity(this.tableName, 1);
                }
            }
        }

        private void showStatus(long scannedCounter, long persistedCounter, long itemCountOrigin)
        {
            Console.WriteLine($"Table {this.tableName}: {DateTime.Now.ToString("dd/MM/yyyy HH:mm:ss")} {itemCountOrigin} in the origin, {scannedCounter} scanned {Math.Round(scannedCounter*1.0/itemCountOrigin*100.0, 5)}% and {persistedCounter} persisted");
        }

        private async Task changeDynamoDbCapacity(string tableName, int writeCapacity)
        {
            var client = this.clientTarget;
            var tableDesc = await client.DescribeTableAsync(tableName);

            var upd = new Amazon.DynamoDBv2.Model.UpdateTableRequest()
            {
                ProvisionedThroughput = new Amazon.DynamoDBv2.Model.ProvisionedThroughput()
                {
                    ReadCapacityUnits = tableDesc.Table.ProvisionedThroughput.ReadCapacityUnits,
                    WriteCapacityUnits = writeCapacity
                }
            };

            foreach (var index in tableDesc.Table.GlobalSecondaryIndexes)
            {
                upd.GlobalSecondaryIndexUpdates.Add(new Amazon.DynamoDBv2.Model.GlobalSecondaryIndexUpdate()
                {
                    Update = new Amazon.DynamoDBv2.Model.UpdateGlobalSecondaryIndexAction()
                    {
                        IndexName = index.IndexName,
                        ProvisionedThroughput = new Amazon.DynamoDBv2.Model.ProvisionedThroughput()
                        {
                            ReadCapacityUnits = tableDesc.Table.ProvisionedThroughput.ReadCapacityUnits,
                            WriteCapacityUnits = writeCapacity
                        }
                    }
                });
            }

            upd.TableName = tableName;

            Console.WriteLine($"Table {this.tableName}: Capacity updated {writeCapacity}");

            await client.UpdateTableAsync(upd);
        }
    }
}
