using Amazon.DynamoDBv2.DocumentModel;
using System;
using System.Collections.Generic;
using System.Text;

namespace DynamoDbTransfer.Model
{
    public class DocumentsPage
    {
        public DocumentsPage(Search search, IEnumerable<Document> documents)
        {
            this.IsLastPage = String.IsNullOrEmpty(search.PaginationToken) || search.PaginationToken == "{}";
            this.PaginationToken = search.PaginationToken;
            this.Documents = documents;
        }

        public bool IsLastPage { get; private set; }

        public IEnumerable<Document> Documents { get; private set; }

        public string PaginationToken { get; private set; }
    }
}
