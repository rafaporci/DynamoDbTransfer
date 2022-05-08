using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.DocumentModel;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace DynamoDbTransfer
{
    public class TableWriter
    {
        private readonly AmazonDynamoDBClient amazonDynamoDBClient;
        private readonly string tableName;
        private readonly Func<Document, bool> beforeToCopyAction;
        private readonly int batchSize = 0;
        private readonly int pageSize = 0;

        public TableWriter(AmazonDynamoDBClient amazonDynamoDBClient, string tableName, int batchSize, int pageSize, Func<Document, bool> beforeToCopyAction = null)
        {
            this.amazonDynamoDBClient = amazonDynamoDBClient;
            this.tableName = tableName;
            this.beforeToCopyAction = beforeToCopyAction;
            this.batchSize = batchSize;
            this.pageSize = pageSize;
        }

        public async Task<int> PersistDataPage(Model.DocumentsPage documentsPage)
        {
            var documentCounter = 0;
            var table = Table.LoadTable(this.amazonDynamoDBClient, this.tableName);
            var currentIndex = 0;

            while (currentIndex < documentsPage.Documents.Count())
            {
                var page = documentsPage.Documents.Skip(currentIndex).Take(pageSize);

                List<DocumentBatchWrite> documentBatchWrites = new List<DocumentBatchWrite>();

                var currentIndexBatch = 0;
                
                while (currentIndexBatch < page.Count())
                {
                    var pageBatch = page.Skip(currentIndexBatch).Take(batchSize);
                    var batch = table.CreateBatchWrite();
                    documentBatchWrites.Add(batch);

                    foreach (var document in pageBatch)
                    { 
                        if (this.beforeToCopyAction != null && !this.beforeToCopyAction(document))
                            continue;

                        batch.AddDocumentToPut(document);
                        documentCounter++;
                    }

                    currentIndexBatch += batchSize;
                }

                var tasks = documentBatchWrites.Select(
                        batch => TaskHelper.ExecWithThroughputControl(async () => await batch.ExecuteAsync())
                    ).ToArray();

                await Task.WhenAll(tasks);

                currentIndex += pageSize;
            }

            return documentCounter;
        }
    }
}
