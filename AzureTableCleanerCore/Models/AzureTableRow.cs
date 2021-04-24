using Microsoft.Azure.Cosmos.Table;

namespace AzureTableCleanerCore.Models
{
    /// <summary>
    /// A row in the Azure Table
    /// </summary>
    class AzureTableRow : TableEntity
    {
        // We only need the PartitionKey, RowKey and Timestamp
    }
}
