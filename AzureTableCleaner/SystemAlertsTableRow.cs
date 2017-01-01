using Microsoft.WindowsAzure.Storage.Table;

namespace AzureTableCleaner
{
    public class SystemAlertsTableRow : TableEntity
    {
        // We only need the PartitionKey, RowKey and Timestamp
    }
}
