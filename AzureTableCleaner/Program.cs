using Microsoft.Azure;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Table;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;

namespace AzureTableCleaner
{
    class Program
    {
        private readonly static string TempFolderName = "temp";
        private const int chunckSizeForFile = 100;

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

            ProcessTempFiles();

            
            CleanTable(table);
        }

        private static void ValidateOptions(Options options)
        {
            // Nothing to do now
            return;
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
            // Query the table
            var query = new TableQuery<SystemAlertsTableRow>()
                .Where(TableQuery.GenerateFilterConditionForDate("Timestamp", QueryComparisons.LessThan,
                       new DateTimeOffset(DateTime.Today.AddDays(1))));

            var rows = table.ExecuteQuery(query).ToArray();

            var chunks = GroupRows(rows, chunckSizeForFile);

            var accumulatedCounter = 0;
            foreach(var chunk in chunks)
            {
                Console.WriteLine("Deleting {0} rows. {1}/{2}", chunk.Length, accumulatedCounter, rows.Length);
                //DeleteRows(chunk, table);
                WriteRowChunkInTempFile(chunk);
                accumulatedCounter += chunk.Length;
            }
        }

        private static void WriteRowChunkInTempFile(SystemAlertsTableRow[] chunk)
        {
            // Create the temp folder if it not exists
            Directory.CreateDirectory(TempFolderName);
            var tempFileName = Path.GetRandomFileName();
            var tempFilePath = Path.Combine(Environment.CurrentDirectory + "\\" + TempFolderName, tempFileName);

            using (var file = File.CreateText(tempFilePath))
            {
                foreach(var item in chunk)
                {
                    // Write on file onlye PartitionKey and RowKey
                    file.WriteLine(item.PartitionKey + ";" + item.RowKey);
                }
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
                var tableEntity = new TableEntity(row.PartitionKey, row.RowKey);
                tableEntity.ETag = "*";
                batchOperation.Delete(tableEntity);
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
                Console.WriteLine("Creating dummy item #" + i);
                table.Execute(operation);
            }
        }
    }
}
