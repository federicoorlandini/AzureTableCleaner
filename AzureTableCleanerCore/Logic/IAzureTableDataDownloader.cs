using System.Threading.Tasks;

namespace AzureTableCleanerCore.Logic
{
    interface IAzureTableDataDownloader
    {
        Task<bool> DownloadAsync(long maxNumberOfRowsInTheLocalDatabase);
    }
}