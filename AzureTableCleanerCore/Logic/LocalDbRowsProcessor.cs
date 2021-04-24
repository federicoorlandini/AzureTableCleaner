using AzureTableCleanerCore.Models;
using AzureTableCleanerCore.Repositories;
using NLog;
using System.Collections.Generic;
using System.Linq;

namespace AzureTableCleanerCore.Logic
{
    /// <summary>
    /// This class contains the logic to process the rows in the local database
    /// </summary>
    class LocalDbRowsProcessor : ILocalDbRowsProcessor
    {
        private readonly ILocalDbRepository _localDbRepository;
        private readonly IAzureTableRepository _azureTableRepository;
        private readonly ILogger _logger;

        public LocalDbRowsProcessor(ILocalDbRepository localDbRepository,
            IAzureTableRepository azureTableRepository,
            ILogger logger,
            int maxNumberOfRowsToFetchFromLocalDb)
        {
            _localDbRepository = localDbRepository;
            _azureTableRepository = azureTableRepository;
            _logger = logger;
        }

        public void Process(int maxNumberOfRowsToFetchFromLocalDb)
        {
            _logger.Info("found rows in the local database. Getting rows...");

            // Get the first chunk or rows
            var entitiesToDelete = _localDbRepository.GetRows(maxNumberOfRowsToFetchFromLocalDb);

            _logger.Info($"{ entitiesToDelete.Count() } rows retrieved from the local database. Deleting from the Azure table...");

            var entitiesPerPartition = GroupEntitiesByPartition(entitiesToDelete);

            foreach(var item in entitiesPerPartition)
            {
                // Delete the rows from Azure Table
                _azureTableRepository.Delete(item.Value);
            }

            _logger.Info("Deleting from the local database...");

            // Delete the rows from the local database
            _localDbRepository.Delete(entitiesToDelete);
        }

        /// <summary>
        /// Groups the entities passed as parameter by PartitionKey.
        /// </summary>
        /// <param name="entities">The collection of entities to be grouped</param>
        /// <returns>A Dictionary of "Partition Key" - "Entities in the partition"</returns>
        private Dictionary<string, List<AzureTableRow>> GroupEntitiesByPartition(IEnumerable<AzureTableRow> entities)
        {
            var query = from entity in entities
                        group entity by entity.PartitionKey into grp
                        select new
                        {
                            grp.Key,
                            Entities = grp.ToList()
                        };

            return query.ToDictionary(item => item.Key, item => item.Entities);
        }
    }
}
