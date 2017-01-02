using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace AzureTableCleaner
{
    internal class TempFileProcessor
    {
        private readonly string _pathToTempFile;
        private readonly int _transactionChunkSize;

        public TempFileProcessor(string pathToTheTempFile, int transactionChunckSize)
        {
            _pathToTempFile = pathToTheTempFile;
            _transactionChunkSize = transactionChunckSize;
        }

        public void Process()
        {
            // Read all the rows
            var items = ReadAllRows();

            // Group the rows in chunck for transaction

            // For each group, start a Task to delete all the rows in a chunk in a single batch transaction

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
