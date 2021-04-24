using AzureTableCleanerCore.Models;
using Microsoft.Azure.Cosmos.Table;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace AzureTableCleanerCore.Repositories
{
    class AzureTableRepository : IAzureTableRepository
    {
        private readonly int _maxNumberOfRowsForEachQuery;
        private readonly string _storageConnectionString;
        private readonly string _tableName;
        private readonly CloudTable _cloudTable;

        public AzureTableRepository(string storageAccountName, string tableName, string accessKey, int maxNumberOfRowsForEachQuery)
        {
            // Build the connection string
            _storageConnectionString = string.Format("DefaultEndpointsProtocol=https;AccountName={0};AccountKey={1}", 
                storageAccountName, 
                accessKey);

            _tableName = tableName;
            _maxNumberOfRowsForEachQuery = maxNumberOfRowsForEachQuery;

            _cloudTable = GetCloudTable();
        }

        private CloudTable GetCloudTable()
        {
            CloudStorageAccount storageAccount = CloudStorageAccount.Parse(_storageConnectionString);

            // Create the table client and prepare the access to the SystemAlerts table
            var tableClient = storageAccount.CreateCloudTableClient();
            var table = tableClient.GetTableReference(_tableName);
            return table;
        }

        public bool HasRows()
        {
            var (rows, continuationToken) = GetRows(null, 1);
            return rows.Count() > 0;
        }

        public (IEnumerable<AzureTableRow>, TableContinuationToken) GetRows(TableContinuationToken continuationToken, int maxNumberResults = 1000)
        {
            TableQuery<DynamicTableEntity> query = new TableQuery<DynamicTableEntity>()
                .Where(TableQuery.GenerateFilterConditionForDate("Timestamp", QueryComparisons.LessThan, DateTime.Now.Date.AddMonths(-1)))
                .Take(_maxNumberOfRowsForEachQuery)
                .Select(new string[] { "PartitionKey", "RowKey", "Timestamp" });

            // Define an entiy resolver to work with the entity after retrieval
            EntityResolver<AzureTableRow> resolver = (partitionKey, rowKey, timeStamp, props, etag) => new AzureTableRow
            {
                PartitionKey = partitionKey,
                RowKey = rowKey,
                Timestamp = timeStamp
            };
            
            var segment = _cloudTable.ExecuteQuerySegmented(query, resolver, continuationToken);
            return (segment.ToList(), segment.ContinuationToken);
        }

        public void Delete(IEnumerable<AzureTableRow> rows)
        {
            // The batch operation can delete a max of 100 entities. For this reason, we need to split the colleciton of rows
            // in groups of 100 entities
            var rowsGroups = rows
                .Select((entity, index) => new { Entity = entity, GroupIndex = index / 100 })
                .GroupBy(item => item.GroupIndex)
                .Select(grp => grp.ToArray())
                .ToArray();

            var tasks = new List<Task>();
            foreach(var rowsGroup in rowsGroups)
            {
                var task = Task.Run(() => DeleteBatch(rowsGroup.Select(item => item.Entity).ToArray()));
                tasks.Add(task);
            }

            Task.WaitAll(tasks.ToArray());
        }

        private void DeleteBatch(AzureTableRow[] rows)
        {
            var batchOperation = new TableBatchOperation();
            foreach (var row in rows)
            {
                var tableEntity = new TableEntity(row.PartitionKey, row.RowKey);
                tableEntity.ETag = "*";
                batchOperation.Delete(tableEntity);
            }

            try
            {
                _cloudTable.ExecuteBatch(batchOperation);
            }
            catch (StorageException ex)
            {
                // It might happen that a row doesn't exist anymore in the Azure Table. In this case, the previous call
                // returns an exception but we don't want to interrupt the process in this case
                if (ex.RequestInformation.HttpStatusCode != 404)
                {
                    throw ex;
                }

                // If we arrive here, we are swallowing the exception
            }
        }
    }
}
