using System.Collections.Generic;

namespace AzureTableCleanerCore.Model
{
    class StorageAccountConfiguration
    {
        public string Name { get; set; }
        public string Key { get; set; }
        public List<string> TableNames { get; set; }
    }
}
