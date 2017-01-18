using Microsoft.WindowsAzure.Storage.Table;
using NLog;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace AzureTableCleaner
{
    /// <summary>
    /// This class process a temp file, deleting the Azure Table rows using the partition key and
    /// row keys that are stored in the temp file
    /// </summary>
    internal class TempFileProcessor
    {
        private readonly string _pathToTempFile;
        private readonly int _transactionChunkSize;
        private readonly CloudTable _table;
        private readonly ILogger _logger;
         
        public TempFileProcessor(string pathToTheTempFile, int transactionChunckSize, CloudTable table, ILogger logger)
        {
            _pathToTempFile = pathToTheTempFile;
            _transactionChunkSize = transactionChunckSize;
            _table = table;
            _logger = logger;
        }

        public void Process()
        {
            // Read all the rows
            var rows = ReadAllRows();

            // Check that all the rows has the same partition key because this is
            // a prerequisite to execute a single batch transaction
            var hasTheSamePartitionKey = HasAllRowsTheSamePartitionKey(rows);
            if( !hasTheSamePartitionKey )
            {
                throw new InvalidOperationException("The file " + _pathToTempFile + " contains rows that has more than one partition key");
            }   
                         
            // Group the rows in chunck for transaction.
            var rowChunksForBatches = GroupRowsForBatches(rows);

            // For each group, start a Task to delete all the rows in a chunk in a single batch transaction
            var tasks = new List<Task>();
            foreach (var rowChunk in rowChunksForBatches)
            {
                var task = Task.Run(() => DeleteRows(rowChunk, _table));
                tasks.Add(task);
            }

            Task.WaitAll(tasks.ToArray());

            _logger.Trace("{0} rows has been deleted", rows.Count);
        }

        private void DeleteRows(SystemAlertsTableRow[] rows, CloudTable table)
        {
            var batchOperation = new TableBatchOperation();
            foreach (var row in rows)
            {
                var tableEntity = new TableEntity(row.PartitionKey, row.RowKey);
                tableEntity.ETag = "*";
                batchOperation.Delete(tableEntity);
            }

            table.ExecuteBatch(batchOperation);
        }

        private IEnumerable<SystemAlertsTableRow[]> GroupRowsForBatches(List<SystemAlertsTableRow> rows)
        {
            var rowsWithIndex = rows.Select((row, index) => new { Index = index, Row = row });

            return from rowWithIndex in rowsWithIndex
                   group rowWithIndex by rowWithIndex.Index / _transactionChunkSize into grp
                   select grp.Select(item => item.Row).ToArray();
        }

        private bool HasAllRowsTheSamePartitionKey(List<SystemAlertsTableRow> rows)
        {
            var groupKeys = from row in rows
                   group row by row.PartitionKey into grp
                   select grp.Key;

            return groupKeys.Count() == 1;
        }

        private List<SystemAlertsTableRow> ReadAllRows()
        {
            var lines = System.IO.File.ReadLines(_pathToTempFile);
            return lines.Select(line => {
                var lineParts = line.Split(';');
                return new SystemAlertsTableRow
                {
                    PartitionKey = lineParts[0],
                    RowKey = lineParts[1],
                    Timestamp = DateTime.Parse(lineParts[2])
                };
            })
            .ToList();
        }
    }
}
