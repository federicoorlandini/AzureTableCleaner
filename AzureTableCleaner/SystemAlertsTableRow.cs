using Microsoft.WindowsAzure.Storage.Table;

namespace AzureTableCleaner
{
    /// <summary>
    /// A row in the Azure Table
    /// </summary>
    /// <seealso cref="Microsoft.WindowsAzure.Storage.Table.TableEntity" />
    public class SystemAlertsTableRow : TableEntity
    {
        // We only need the PartitionKey, RowKey and Timestamp
    }
}
