using Microsoft.WindowsAzure.Storage.Table;
using System;

namespace AzureTableCleaner
{
    public class SystemAlertsTableRow : TableEntity
    {
        public TimeSpan Timestamp { get; set; }
    }
}
