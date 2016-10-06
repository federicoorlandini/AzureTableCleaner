using Microsoft.WindowsAzure.Storage.Table;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace AzureTableCleaner
{
    public class SystemAlertsTableRow : TableEntity
    {
        public TimeSpan Timestamp { get; set; }
    }
}
