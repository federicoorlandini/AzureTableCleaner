using AzureTableCleanerCore.Repositories;
using NLog;
using System.Threading.Tasks;

namespace AzureTableCleanerCore.Logic
{
    internal class CleanAgent
    {
        private const int MAX_NUMBER_OF_ROWS_TO_FETCH_FROM_LOCAL_DB = 10000;
        private const long MAX_NUMBER_OF_ROWS_IN_LOCAL_DB = 1000000;
        private readonly ILocalDbRowsProcessor _localDbRowsProcessor;
        private readonly ILocalDbRepository _localDbRepository;
        private readonly IAzureTableRepository _azureTableRepository;
        private readonly IAzureTableDataDownloader _azureTableDownloader;

        private readonly ILogger _logger;

        public CleanAgent(ILocalDbRowsProcessor localDbRowsProcessor,
            ILocalDbRepository localDbRepository, 
            IAzureTableRepository azureTableRepository, 
            IAzureTableDataDownloader azureTableDataDownloader,
            ILogger logger)
        {
            _localDbRowsProcessor = localDbRowsProcessor;
            _localDbRepository = localDbRepository;
            _azureTableRepository = azureTableRepository;
            _azureTableDownloader = azureTableDataDownloader;
            _logger = logger;
        }

        public async Task ExecuteAsync()
        {
            bool mustContinue;
            do
            {
                mustContinue = await IterationAsync();
            }
            while (mustContinue);
        }

        internal async Task<bool> IterationAsync()
        {
            while (await _localDbRepository.HasRowsAsync())
            {
                _localDbRowsProcessor.Process(MAX_NUMBER_OF_ROWS_TO_FETCH_FROM_LOCAL_DB);
            }

            _logger.Info("No rows found in the local database. Checking if there are rows in the Azure table left...");

            // No rows locally, so check if there are rows in the Azure table. If no rows, we finished
            if (!_azureTableRepository.HasRows())
            {
                _logger.Info("No rows found in the Azure Table.");
                _localDbRepository.DropTable();
                _logger.Info("Nothing else to do. Exiting...");
                return false;
            }

            // There are still rows in the Azure table, so let's download some
            await _azureTableDownloader.DownloadAsync(MAX_NUMBER_OF_ROWS_IN_LOCAL_DB);

            // If we arrive here, we need to continue with another iteration
            return true;
        }
    }
}
