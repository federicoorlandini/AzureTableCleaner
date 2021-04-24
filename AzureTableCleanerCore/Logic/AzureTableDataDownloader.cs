using AzureTableCleanerCore.Repositories;
using Microsoft.Azure.Cosmos.Table;
using NLog;
using System.Linq;
using System.Threading.Tasks;

namespace AzureTableCleanerCore.Logic
{
    /// <summary>
    /// This class download rows from the Azure Table and store the relevant information in the local database for further processing
    /// </summary>
    class AzureTableDataDownloader : IAzureTableDataDownloader
    {
        private readonly IAzureTableRepository _azureTableRepository;
        private readonly ILocalDbRepository _localDbRepository;
        private readonly ILogger _logger;

        public AzureTableDataDownloader(IAzureTableRepository azureTableRepository,
            ILocalDbRepository localDbRepository,
            ILogger logger)
        {
            _azureTableRepository = azureTableRepository;
            _localDbRepository = localDbRepository;
            _logger = logger;
        }

        /// <summary>
        /// Download rows from the remote Azure Table to the local database, until the number
        /// of rows in the local database reaches the max number of rows allowed
        /// </summary>
        /// <param name="maxNumberOfRowsInTheLocalDatabase">The max number of rows we can have in the local database</param>
        /// <returns>TRUE if there are rows to download, FALSE if the remote azure
        /// table hasn't any rows</returns>
        public async Task<bool> DownloadAsync(long maxNumberOfRowsInTheLocalDatabase)
        {
            // Check if there are rows to download
            if (!_azureTableRepository.HasRows())
            {
                return false;
            }

            TableContinuationToken continuationToken = null;
            long numberOfRowsInTheLocalDatabase;
            while ((numberOfRowsInTheLocalDatabase = await _localDbRepository.CountRowsAsync()) < maxNumberOfRowsInTheLocalDatabase)
            {
                _logger.Info($"{ numberOfRowsInTheLocalDatabase }/{ maxNumberOfRowsInTheLocalDatabase } rows in the local database of. Retriving rows from the Azure Table");

                var (entities, newContinuationToken) = _azureTableRepository.GetRows(continuationToken);
                continuationToken = newContinuationToken;

                if (!entities.Any())
                {
                    break;
                }

                _logger.Info($"Inserting { entities.Count() } into the local database.");
                _localDbRepository.Insert(entities);

                if (continuationToken == null)
                {
                    // No more rows in the Azure Table
                    _logger.Info("No new continuation token. Exiting the download loop...");
                    break;
                }
            }

            return true;
        }
    }
}
