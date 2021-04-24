using AzureTableCleanerCore.Models;
using Microsoft.Azure.Cosmos.Table;
using System.Collections.Generic;

namespace AzureTableCleanerCore.Repositories
{
    interface IAzureTableRepository
    {
        void Delete(IEnumerable<AzureTableRow> rows);

        bool HasRows();

        (IEnumerable<AzureTableRow>, TableContinuationToken) GetRows(TableContinuationToken continuationToken, int maxNumberResults = 1000); 
    }
}