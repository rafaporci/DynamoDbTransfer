using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.DocumentModel;
using System;
using System.Threading.Tasks;

namespace DynamoDbTransfer
{
    public class TableReader
    {
        private readonly AmazonDynamoDBClient amazonDynamoDBClient;
        private readonly string tableName;

        public TableReader(AmazonDynamoDBClient amazonDynamoDBClient, string tableName)
        {
            this.amazonDynamoDBClient = amazonDynamoDBClient;
            this.tableName = tableName;
        }

        public async Task<Model.DocumentsPage> FetchDataPage(string paginationToken = null)
        {
            var table = Table.LoadTable(this.amazonDynamoDBClient, this.tableName);

            var config = new ScanOperationConfig()
            {
                PaginationToken = paginationToken,
                Select = SelectValues.AllAttributes,
                Limit = 1000
            };

            var search = table.Scan(config);

            var documents = await TaskHelper.ExecWithThroughputControlWithResult(async () => await search.GetNextSetAsync());
            
            return new Model.DocumentsPage(search, documents);
        }

        public async Task<long> GetItemCount()
        {
            var desc = await this.amazonDynamoDBClient.DescribeTableAsync(this.tableName);

            return desc.Table.ItemCount;
        }
    }
}
