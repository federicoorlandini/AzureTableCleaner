using Microsoft.Azure;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Table;
using NLog;
using System;
using System.Configuration;
using System.IO;
using System.Linq;

namespace AzureTableCleaner
{
    class Program
    {
        // The folder name where the temp files will be created
        private const string TempFolderName = "temp";

        // Number max of results for each query we send to Azure Table
        private static readonly int _maxNumberOfRowsForEachQuery = int.Parse(ConfigurationManager.AppSettings["MaxNumberOfRowsForEachQuery"]);

        // Max number of delete command for each transaction
        private static readonly int _chunkSizeForTransaction = int.Parse(ConfigurationManager.AppSettings["ChunkSizeForTransaction"]);

        // Max number of rows in each temporary file
        private static readonly int _chunkSizeForTempFile = int.Parse(ConfigurationManager.AppSettings["ChunkSizeForTempFile"]);

        private static readonly ILogger _logger = LogManager.GetCurrentClassLogger();

        // The complete path to the temp folder
        private static string TempFolderPath
        {
            get { return Environment.CurrentDirectory + "\\" + TempFolderName; }
        }

        /// <summary>
        /// Gets a value indicating whether this instance is using azure storage emulator.
        /// </summary>
        /// <value>
        /// <c>true</c> if this instance is using azure storage emulator; otherwise, <c>false</c>.
        /// </value>
        private static bool IsUsingAzureStorageEmulator
        {
            get
            {
                var connectionString = CloudConfigurationManager.GetSetting("StorageConnectionString");
                return connectionString.Contains("UseDevelopmentStorage");
            }
        }

        static void Main(string[] args)
        {
            // The code parse the options args but it doesn't use them actually
            var options = ParseArgs(args);

            ValidateOptions(options);
            CloudTable table = GetCloudTable(options);

            // Uncomment the following line to create some dummy rows
            // Useful for testing purpose in the Azure Storage Emulator
            //CreateDummyData(table, 200);

            while (true)
            {
                // First, we need to check if there are some temp files waiting to be processed
                ProcessTempFiles(table);

                // Retrieve the data from the Azure table and create the temp files
                _logger.Trace("Reading data from Azure Table");
                var rows = RetrieveDataFromAzure(table);
                _logger.Trace("Found {0} rows (limit {1})", rows.Length, _maxNumberOfRowsForEachQuery);
                if (!rows.Any())
                {
                    _logger.Trace("No more rows to process. Exiting.....");
                    break;
                }

                // Store the rows in temp files
                _logger.Trace("Start writing data in temp files");
                StoreRowsInTempFiles(rows);

                // Let's process the temp files already created
                ProcessTempFiles(table);
            }
        }

        private static CloudTable GetCloudTable(Options options)
        {
            // Create the storage account
            string connectionString;
            if (options.UseDeveloperEnvironment)
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
            return table;
        }

        private static string[] GetTempFiles()
        {
            return Directory.GetFiles(TempFolderPath, "*.csv");
        }

        private static void ProcessTempFiles(CloudTable table)
        {
            // Get the list of the file in the temp directory
            var tempFileList = GetTempFiles();

            foreach(var tempFile in tempFileList)
            {
                _logger.Trace("Processing file " + tempFile);
                var fileProcessor = new TempFileProcessor(tempFile, _chunkSizeForTransaction, table, _logger);
                fileProcessor.Process();

                _logger.Trace("The file " + tempFile + " has been processed");

                // Delete the file that has been just processed
                _logger.Trace("Deleting file " + tempFile);
                File.Delete(tempFile);
            }
        }

        private static SystemAlertsTableRow[] RetrieveDataFromAzure(CloudTable table)
        {
            // Construct the projectionQuery to get only "PartitionKey", "RowKey" and "Timestamp"
            TableQuery<DynamicTableEntity> projectionQuery = new TableQuery<DynamicTableEntity>()
                .Where(TableQuery.GenerateFilterConditionForDate("Timestamp", QueryComparisons.LessThan, DateTime.Now.Date.AddMonths(-1)))
                .Take(_maxNumberOfRowsForEachQuery)
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
                                     group row by row.Index / _chunkSizeForTempFile into grp
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

                _logger.Trace("Wrinting {0} rows in {1}", groupForTempFile.Rows.Length, tempFileWithPath);

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

        private static void CreateDummyData(CloudTable table, int numberOfRows)
        {
            if( !IsUsingAzureStorageEmulator)
            {
                throw new InvalidOperationException("You cannot create dummy data if you are not referencing the local Azure Storage Emulator");
            }

            table.CreateIfNotExists();

            for(int i = 0; i < numberOfRows; i++)
            {
                var entity = new SystemAlertsTableRow {
                    PartitionKey = "a",
                    RowKey = Guid.NewGuid().ToString(),
                    Timestamp = DateTime.Now
                };
                var operation = TableOperation.Insert(entity);
                _logger.Trace("Creating dummy item #" + i);
                table.Execute(operation);
            }
        }
    }
}
