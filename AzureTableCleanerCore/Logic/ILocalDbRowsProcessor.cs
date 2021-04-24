namespace AzureTableCleanerCore.Logic
{
    interface ILocalDbRowsProcessor
    {
        void Process(int maxNumberOfRowsToFetchFromLocalDb);
    }
}