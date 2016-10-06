using Microsoft.Azure;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Table;
using System;
using System.Collections.Generic;
using System.Linq;

namespace AzureTableCleaner
{
    class Program
    {
        static void Main(string[] args)
        {
            var options = ParseArgs(args);

            ValidateOptions(options);

            // Create the storage account
            string connectionString;
            if( options.UseDeveloperEnvironment )
            {
                connectionString = CloudConfigurationManager.GetSetting("StorageConnectionString");
            }
            else
            {
                // Build the connection string
                var template = "DefaultEndpointsProtocol=https;AccountName={0};AccountKey={1}";
                connectionString = string.Format(template, options.AccountName, options.AccountName);
            }
            CloudStorageAccount storageAccount = CloudStorageAccount.Parse(connectionString);

            // Create the table client and prepare the access to the SystemAlerts table
            var tableClient = storageAccount.CreateCloudTableClient();
            var table = tableClient.GetTableReference("SystemAlerts");

            //CreateDummyData(table, 200);
            CleanTable(table);
        }

        private static Options ParseArgs(string[] args)
        {
            var options = new Options();

            options.UseDeveloperEnvironment = args.Contains("--useDeveloperEnvironment", StringComparer.InvariantCultureIgnoreCase);

            var accountNameArg = args.SingleOrDefault(item => item.Equals("AccountName", StringComparison.InvariantCultureIgnoreCase));
            if (!options.UseDeveloperEnvironment && accountNameArg != null)
            {
                var index = Array.IndexOf(args, accountNameArg);
                options.AzureTableAccessKey = args[index + 1];
            }

            var accessKeyArg = args.SingleOrDefault(item => item.Equals("AzureTableAccessKey", StringComparison.InvariantCultureIgnoreCase));
            if( !options.UseDeveloperEnvironment && accessKeyArg != null )
            {
                var index = Array.IndexOf(args, accessKeyArg);
                options.AzureTableAccessKey = args[index + 1];
            }    

            var tableNameArg = args.SingleOrDefault(item => item.Equals("TableName", StringComparison.InvariantCultureIgnoreCase));
            if (!options.UseDeveloperEnvironment && tableNameArg != null)
            {
                var index = Array.IndexOf(args, tableNameArg);
                options.TableName = args[index + 1];
            }

            return options;
        }

        private static void CleanTable(CloudTable table)
        {
            const int chunckSize = 100;

            // Query the table
            var query = new TableQuery<SystemAlertsTableRow>()
                .Where(TableQuery.GenerateFilterConditionForDate("Timestamp", QueryComparisons.LessThan,
                       new DateTimeOffset(DateTime.Today)));

            var rows = table.ExecuteQuery(query).ToArray();

            var chunks = GroupRows(rows, chunckSize);

            var accumulatedCounter = 0;
            foreach(var chunk in chunks)
            {
                Console.WriteLine("Deleting {0} rows. {1}/{2}", chunk.Length, accumulatedCounter, rows.Length);
                DeleteRows(chunk, table);
                accumulatedCounter += chunk.Length;
            }
        }

        private static SystemAlertsTableRow[][] GroupRows(IEnumerable<SystemAlertsTableRow> rows, int groupSize)
        {
            var items = rows
                .Select((item, index) => new { Item = item, GroupIndex = index / groupSize })
                .GroupBy(item => item.GroupIndex)
                .Select(group => group.Select(item => item.Item).ToArray())
                .ToArray();
            return items;
        }

        private static void DeleteRows(SystemAlertsTableRow[] rows, CloudTable table)
        {
            var batchOperation = new TableBatchOperation();
            foreach(var row in rows)
            {
                batchOperation.Delete(row);
            }

            table.ExecuteBatch(batchOperation);
        }

        private static void CreateDummyData(CloudTable table, int numberOfRows)
        {
            table.CreateIfNotExists();

            for(int i = 0; i < numberOfRows; i++)
            {
                var entity = new SystemAlertsTableRow {
                    PartitionKey = "a",
                    RowKey = Guid.NewGuid().ToString(),
                    Timestamp = TimeSpan.FromTicks(DateTime.Now.Ticks)
                };
                var operation = TableOperation.Insert(entity);
                table.Execute(operation);
            }
        }
    }
}
