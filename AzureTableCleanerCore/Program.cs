using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using AzureTableCleanerCore.Logic;
using AzureTableCleanerCore.Model;
using AzureTableCleanerCore.Repositories;
using Microsoft.Extensions.Configuration;
using NLog;

namespace AzureTableCleanerCore
{
    class Program
    {
        private static ILogger _logger;
        private static ILocalDbRepository _localDbRepository;
        private static IAzureTableRepository _azureTableRepositoy;
        private static ILocalDbRowsProcessor _localDbRowsProcessor;
        private static IAzureTableDataDownloader _azureTableDataDownloader;

        private const int MAX_NUMBER_OF_ROWS_TO_FETCH_FROM_LOCAL_DB = 1000;

        private const int MAX_NUMBER_OF_ROWS_TO_FETCH_FROM_AZURE_TABLE = 1000;

        static async Task Main(string[] args)
        {
            var storageAccountConfigurations = ReadStorageAccountConfiguration();

            _logger = LogManager.GetCurrentClassLogger();

            // Let's clean up all the storage accounts
            foreach (var storageAccountConfiguration in storageAccountConfigurations)
            {
                await ProcessStorageAccountAsync(storageAccountConfiguration);
            }
        }

        private static async Task ProcessStorageAccountAsync(StorageAccountConfiguration storageAccountConfiguration)
        {
            foreach(var tableName in storageAccountConfiguration.TableNames)
            {
                _logger.Info($"Processing storage account { storageAccountConfiguration.Name }, table { tableName }");

                _localDbRepository = new LocalDbRepository($"{ storageAccountConfiguration.Name }_{ tableName }", _logger);
                _azureTableRepositoy = new AzureTableRepository(storageAccountConfiguration.Name,
                    tableName,
                    storageAccountConfiguration.Key,
                    MAX_NUMBER_OF_ROWS_TO_FETCH_FROM_AZURE_TABLE);
                _localDbRowsProcessor = new LocalDbRowsProcessor(_localDbRepository, _azureTableRepositoy, _logger, MAX_NUMBER_OF_ROWS_TO_FETCH_FROM_LOCAL_DB);
                _azureTableDataDownloader = new AzureTableDataDownloader(_azureTableRepositoy, _localDbRepository, _logger);

                var cleanAgent = new CleanAgent(_localDbRowsProcessor,
                    _localDbRepository,
                    _azureTableRepositoy,
                    _azureTableDataDownloader,
                    _logger);
                await cleanAgent.ExecuteAsync();
            }
        }

        private static List<StorageAccountConfiguration> ReadStorageAccountConfiguration()
        {
            IConfiguration configuration = new ConfigurationBuilder()
                            .AddJsonFile("appsettings.json")
                            .Build();

            var storageAccountConfigurations = new List<StorageAccountConfiguration>();
            configuration.GetSection("StorageAccounts").Bind(storageAccountConfigurations);

            return storageAccountConfigurations;
        }
    }
}
