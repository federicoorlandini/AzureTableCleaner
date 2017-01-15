namespace AzureTableCleaner
{
    /// <summary>
    /// A group of table rows that share the same partition key
    /// </summary>
    internal class RowsGroup
    {
        public string Key { get; set; }
        public SystemAlertsTableRow[] Rows { get; set; }
    }
}
