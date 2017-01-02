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
        private const string TempFolderName = "temp";

        // Number max of rows for each query we send to Azure Table
        private const int MaxNumberOfRowsForEachQuery = 10000;

        // Max number of delete command for each transaction
        private const int ChunkSizeForTransaction = 100;

        // Max number of rows in each temporary file
        private const int ChunkSizeForTempFile = 1000;

        // The complete path to the temp folder
        private static string TempFolderPath
        {
            get { return Environment.CurrentDirectory + "\\" + TempFolderName; }
        }

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

            while (true)
            {
                // First, we need to check if there are some temp files waiting to be processed
                ProcessTempFiles();

                // Retrieve the data from the Azure table and create the temp files
                Log("Reading data from Azure Table");
                var rows = RetrieveDataFromAzure(table);
                if (!rows.Any())
                {
                    break;
                }

                // Store the rows in temp files
                Log("Start writing data in tem files");
                StoreRowsInTempFiles(rows);
            }

            //CleanTable(table);
        }

        private static void ProcessTempFiles()
        {
            // Get the list of the file in the temp directory
            var tempFileList = Directory.GetFiles(TempFolderPath, "*.csv");

            foreach(var tempFile in tempFileList)
            {
                var fileProcessor = new TempFileProcessor(tempFile, ChunkSizeForTransaction);
                fileProcessor.Process();
            }
        }

        private static void ProcessTempFile(string tempFile)
        {
            
        }

        private static SystemAlertsTableRow[] RetrieveDataFromAzure(CloudTable table)
        {
            // Construct the projectionQuery to get only "PartitionKey", "RowKey" and "Timestamp"
            TableQuery<DynamicTableEntity> projectionQuery = new TableQuery<DynamicTableEntity>()
                .Where(TableQuery.GenerateFilterConditionForDate("Timestamp", QueryComparisons.LessThan, DateTime.Now.Date.AddMonths(-1)))
                .Take(MaxNumberOfRowsForEachQuery)
                .Select(new string[] { "PartitionKey", "RowKey", "Timestamp" });

            // Define an entiy resolver to work with the entity after retrieval
            EntityResolver<SystemAlertsTableRow> resolver = (partitionKey, rowKey, timeStamp, props, etag) => new SystemAlertsTableRow
            {
                PartitionKey = partitionKey,
                RowKey = rowKey,
                Timestamp = timeStamp
            };

            var rows = table.ExecuteQuery(projectionQuery, resolver);
            return rows.ToArray();
        }

        private static void StoreRowsInTempFiles(SystemAlertsTableRow[] rows)
        {
            // We need to:
            // 1. group the rows by partition key
            // 2. for each group:
            //      2a. build chunks of N rows
            //      2b. write each chunk in a separate temp file

            var groupsByPartitionKey = from row in rows
                                       group row by row.PartitionKey into grp
                                       select new RowsGroup { Key = grp.Key, Rows = grp.ToArray() };

            foreach(var groupByPartitionKey in groupsByPartitionKey)
            {
                BuildTempFilesForGroupByPartitionKey(groupByPartitionKey);
            }
        }

        private static void BuildTempFilesForGroupByPartitionKey(RowsGroup group)
        {
            // Let's add a row index for each row in the group
            var rowsWithIndex = group.Rows
                .Select((row, index) => new {
                    Index = index,
                    Row = row })
                .ToArray();

            // We split the row group in separate temp files
            var groupsForTempFiles = from row in rowsWithIndex
                                     group row by row.Index / ChunkSizeForTempFile into grp
                                     select new { GroupIndex = grp.Key, Rows = grp.ToArray() };

            // Fro each group, let's build a temp file where store
            // - PartitionKey
            // - RowKey
            // - TimeStamp
            foreach(var groupForTempFile in groupsForTempFiles)
            {
                var tempFileName = GenerateTempFileName();

                Directory.CreateDirectory(TempFolderPath);

                var tempFileWithPath = Path.Combine(TempFolderPath, tempFileName);

                using (var streamWriter = new StreamWriter(tempFileWithPath))
                {
                    foreach(var row in groupForTempFile.Rows)
                    {
                        var line = string.Format("{0};{1};{2}", row.Row.PartitionKey, row.Row.RowKey, row.Row.Timestamp);
                        streamWriter.WriteLine(line);
                    }
                }
            }              
        }

        private static string GenerateTempFileName()
        {
            var tempFileName = Path.GetTempFileName();
            tempFileName = Path.GetFileNameWithoutExtension(tempFileName) + ".csv";
            return tempFileName;
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
            

            //var rows = table.ExecuteQuery(query).ToArray();

            //var chunks = GroupRows(rows, chunckSizeForFile);

            //var accumulatedCounter = 0;
            //foreach(var chunk in chunks)
            //{
            //    Console.WriteLine("Deleting {0} rows. {1}/{2}", chunk.Length, accumulatedCounter, rows.Length);
            //    //DeleteRows(chunk, table);
            //    WriteRowChunkInTempFile(chunk);
            //    accumulatedCounter += chunk.Length;
            //}
        }

        private static void WriteRowChunkInTempFile(SystemAlertsTableRow[] chunk)
        {
            // Create the temp folder if it not exists
            Directory.CreateDirectory(TempFolderName);
            var tempFileName = Path.GetRandomFileName();
            var tempFilePath = Path.Combine(Environment.CurrentDirectory + "\\" + TempFolderName, tempFileName);

            using (var file = File.CreateText(tempFilePath))
            {
                Log(string.Format("Writing {0} rows in file {1}", chunk.Length, tempFilePath));
                foreach(var item in chunk)
                {
                    // Write on file onlye PartitionKey and RowKey
                    file.WriteLine(item.PartitionKey + ";" + item.RowKey);
                }
            }
        }

        //private static SystemAlertsTableRow[][] GroupRows(IEnumerable<SystemAlertsTableRow> rows, int groupSize)
        //{
        //    var items = rows
        //        .Select((item, index) => new { Item = item, GroupIndex = index / groupSize })
        //        .GroupBy(item => item.GroupIndex)
        //        .Select(group => group.Select(item => item.Item).ToArray())
        //        .ToArray();
        //    return items;
        //}

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
                    Timestamp = DateTime.Now
                };
                var operation = TableOperation.Insert(entity);
                Log("Creating dummy item #" + i);
                table.Execute(operation);
            }
        }

        private static void Log(string message)
        {
            var text = string.Format("[{0}] - {1}", DateTime.Now.ToString(), message);
            Console.WriteLine(text);
        }
    }
}
