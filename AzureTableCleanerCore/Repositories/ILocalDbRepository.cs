using AzureTableCleanerCore.Models;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace AzureTableCleanerCore.Repositories
{
    internal interface ILocalDbRepository
    {
        Task<bool> HasRowsAsync();

        Task<long> CountRowsAsync();

        IEnumerable<AzureTableRow> GetRows(int maxNumberOfRows);

        void Insert(IEnumerable<AzureTableRow> azureTableRows);

        void Delete(IEnumerable<AzureTableRow> azureTableRows);

        void DropTable();
    }
}