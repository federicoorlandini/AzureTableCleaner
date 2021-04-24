using AzureTableCleanerCore.Models;
using Microsoft.Data.Sqlite;
using NLog;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace AzureTableCleanerCore.Repositories
{
    internal class LocalDbRepository : ILocalDbRepository
    {
        private const string DATABASE_FILE_PATH = @".\databases\azure-table.db";
        private readonly string _tableName;
        private readonly ILogger _logger;
        private readonly string _connectionString;

        public LocalDbRepository(string tableName, ILogger logger)
        {
            _tableName = tableName;
            _logger = logger;
            var connectionStringBuilder = new SqliteConnectionStringBuilder();
            connectionStringBuilder.DataSource = DATABASE_FILE_PATH;
            connectionStringBuilder.Mode = SqliteOpenMode.ReadWriteCreate;
            _connectionString = connectionStringBuilder.ConnectionString;

            CreateTableIfNotExists();
        }

        private void CreateTableIfNotExists()
        {
            using(var connection = new SqliteConnection(_connectionString))
            {
                connection.Open();

                var command = connection.CreateCommand();
                command.CommandText = $"CREATE TABLE IF NOT EXISTS { _tableName } (PartitionKey NVARCHAR(64), RowKey NVARCHAR(64)); CREATE INDEX IF NOT EXISTS idx_partitionkey_rowkey ON { _tableName }(PartitionKey, RowKey); ";
                command.ExecuteNonQuery();

                connection.Close();
            }
        }

        public async Task<bool> HasRowsAsync()
        {
            return await CountRowsAsync() > 0;
        }

        public async Task<long> CountRowsAsync()
        {
            using (var connection = new SqliteConnection(_connectionString))
            {
                await connection.OpenAsync();

                var command = connection.CreateCommand();
                command.CommandText = $"SELECT COUNT(1) FROM { _tableName }";
                var result = (long) await command.ExecuteScalarAsync();

                return result;
            }
        }

        public IEnumerable<AzureTableRow> GetRows(int maxNumberOfRows)
        {
            using (var connection = new SqliteConnection(_connectionString))
            {
                connection.Open();
                
                var command = connection.CreateCommand();
                command.CommandText = $"SELECT PartitionKey, RowKey FROM { _tableName } ORDER BY PartitionKey ASC LIMIT { maxNumberOfRows }";
                var reader = command.ExecuteReader();
                
                var result = new List<AzureTableRow>();

                while(reader.Read())
                {
                    result.Add(new AzureTableRow
                    {
                        PartitionKey = reader["PartitionKey"].ToString(),
                        RowKey = reader["RowKey"].ToString()
                    });
                }

                return result;
            }
        }

        public void Insert(IEnumerable<AzureTableRow> azureTableRows)
        {
            using (var connection = new SqliteConnection(_connectionString))
            {
                connection.Open();

                using (var transaction = connection.BeginTransaction())
                {
                    var command = connection.CreateCommand();
                    command.CommandText = $"INSERT INTO { _tableName } VALUES ($partitionKey, $rowKey)";

                    var partitionKeyParameter = command.CreateParameter();
                    partitionKeyParameter.ParameterName = "$partitionKey";
                    command.Parameters.Add(partitionKeyParameter);

                    var rowKeyParameter = command.CreateParameter();
                    rowKeyParameter.ParameterName = "$rowKey";
                    command.Parameters.Add(rowKeyParameter);

                    // Insert a lot of data
                    foreach (var row in azureTableRows)
                    {
                        partitionKeyParameter.Value = row.PartitionKey;
                        rowKeyParameter.Value = row.RowKey;

                        command.ExecuteNonQuery();
                    }

                    transaction.Commit();
                }
            }
        }

        public void Delete(IEnumerable<AzureTableRow> azureTableRows)
        {
            using (var connection = new SqliteConnection(_connectionString))
            {
                connection.Open();

                using (var transaction = connection.BeginTransaction())
                {
                    var command = connection.CreateCommand();
                    command.CommandText = $"DELETE FROM { _tableName } WHERE PartitionKey = $partitionKey AND RowKey = $rowKey";

                    var partitionKeyParameter = command.CreateParameter();
                    partitionKeyParameter.ParameterName = "$partitionKey";
                    command.Parameters.Add(partitionKeyParameter);

                    var rowKeyParameter = command.CreateParameter();
                    rowKeyParameter.ParameterName = "$rowKey";
                    command.Parameters.Add(rowKeyParameter);

                    // Insert a lot of data
                    foreach (var row in azureTableRows)
                    {
                        partitionKeyParameter.Value = row.PartitionKey;
                        rowKeyParameter.Value = row.RowKey;

                        command.ExecuteNonQuery();
                    }

                    transaction.Commit();
                }
            }
        }

        public void DropTable()
        {
            _logger.Info($"Dropping the local table { _tableName }.");

            using (var connection = new SqliteConnection(_connectionString))
            {
                connection.Open();

                var command = connection.CreateCommand();
                command.CommandText = $"DROP TABLE IF EXISTS { _tableName }";
                command.ExecuteNonQuery();

                connection.Close();
            }
        }
    }
}
