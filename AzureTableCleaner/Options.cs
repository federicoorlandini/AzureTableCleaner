namespace AzureTableCleaner
{
    internal class Options
    {
        public bool UseDeveloperEnvironment { get; set; }
        public string AccountName { get; set; }
        public string AzureTableAccessKey { get; set; }
        public string TableName { get; set; }
    }
}
