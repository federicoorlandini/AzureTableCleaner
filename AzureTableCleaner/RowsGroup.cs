using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace AzureTableCleaner
{
    internal class RowsGroup
    {
        public string Key { get; set; }
        public SystemAlertsTableRow[] Rows { get; set; }
    }
}
