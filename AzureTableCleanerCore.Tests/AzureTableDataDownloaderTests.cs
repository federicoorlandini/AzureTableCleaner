using AzureTableCleanerCore.Logic;
using AzureTableCleanerCore.Models;
using AzureTableCleanerCore.Repositories;
using Microsoft.Azure.Cosmos.Table;
using Moq;
using NLog;
using NUnit.Framework;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace AzureTableCleanerCore.Tests
{
    [TestFixture]
    public class AzureTableDataDownloaderTests
    {
        private Mock<ILogger> _loggerMock = new Mock<ILogger>();

        [Test]
        public async Task Download_whenThereAreNoRowsInTheRemoteTable_shouldReturnFalse()
        {
            // Given there are no rows in the remote Azure table
            // When calling the Download method
            // Should return false
            var azureTableRepositoryMock = new Mock<IAzureTableRepository>(MockBehavior.Strict);
            azureTableRepositoryMock
                .Setup(mock => mock.HasRows())
                .Returns(false);

            var localDbRepositoryMock = new Mock<ILocalDbRepository>(MockBehavior.Strict);

            var azureDownloader = new AzureTableDataDownloader(azureTableRepositoryMock.Object,
                localDbRepositoryMock.Object,
                _loggerMock.Object);
            var result = await azureDownloader.DownloadAsync(It.IsAny<long>());

            Assert.IsFalse(result, "The result of the call should be false");
        }

        [Test]
        public async Task Download_whenThereAreNoRowsInTheRemoteTable_andNotReturnBackAContinuationToken_shouldDownloadTheRowsAndInsertInTheLocalDatabaseOnce()
        {
            // Given there are rows in the remote Azure table
            // And the number of rows in the local database is below the threshold
            // And the Azure table call doesn't return a continuation token
            // When calling the Download method
            // Should retrieve rows from the Azure table only once
            // And should insert the data in the local database only once
            var azureTableRepositoryMock = new Mock<IAzureTableRepository>(MockBehavior.Strict);
            azureTableRepositoryMock
                .Setup(mock => mock.HasRows())
                .Returns(true);

            var azureTableRowCollection = new List<AzureTableRow> { 
                new AzureTableRow { 
                    PartitionKey = "PartionKey", 
                    RowKey = "abcd" } 
            };
            azureTableRepositoryMock
                .Setup(mock => mock.GetRows(null, It.IsAny<int>()))
                .Returns((azureTableRowCollection, null));

            var maxNumberRowsInLocalDatabase = 1000;
            var localDbRepositoryMock = new Mock<ILocalDbRepository>(MockBehavior.Strict);
            localDbRepositoryMock
                .Setup(mock => mock.CountRowsAsync())
                .Returns(Task.FromResult<long>(maxNumberRowsInLocalDatabase - 100));

            localDbRepositoryMock
                .Setup(mock => mock.Insert(azureTableRowCollection))
                .Verifiable();

            var azureDownloader = new AzureTableDataDownloader(azureTableRepositoryMock.Object,
                localDbRepositoryMock.Object,
                _loggerMock.Object);
            var result = await azureDownloader.DownloadAsync(maxNumberRowsInLocalDatabase);

            localDbRepositoryMock.VerifyAll();
        }

        [Test]
        public async Task Download_whenThereAreNoRowsInTheRemoteTable_andReturnsBackAContinuationToken_shouldDownloadTheRowsAndInsertInTheLocalDatabaseTwice()
        {
            // Given there are rows in the remote Azure table
            // And the number of rows in the local database is below the threshold
            // And the Azure table call return a continuation token
            // When calling the Download method
            // Should retrieve rows from the Azure table
            // And should insert the data in the local database
            // And should retrieve other rows from the Azure table
            // And should insert the data in the local database
            var azureTableRepositoryMock = new Mock<IAzureTableRepository>(MockBehavior.Strict);
            azureTableRepositoryMock
                .Setup(mock => mock.HasRows())
                .Returns(true);

            // First check on the local database number of rows
            var maxNumberRowsInLocalDatabase = 1000;
            var localDbRepositoryMock = new Mock<ILocalDbRepository>(MockBehavior.Strict);
            localDbRepositoryMock
                .Setup(mock => mock.CountRowsAsync())
                .Returns(Task.FromResult<long>(maxNumberRowsInLocalDatabase - 100));

            // First call to the remote Azure Table to retrieve rows
            var azureTableRowCollection = new List<AzureTableRow> {
                new AzureTableRow {
                    PartitionKey = "PartionKey",
                    RowKey = "abcd" }
            };
            var continuationToken = new TableContinuationToken();
            azureTableRepositoryMock
                .Setup(mock => mock.GetRows(null, It.IsAny<int>()))
                .Returns((azureTableRowCollection, continuationToken));

            // First insert in the local database
            localDbRepositoryMock
                .Setup(mock => mock.Insert(azureTableRowCollection))
                .Verifiable();

            // Second check on the local database number of rows
            localDbRepositoryMock
                .Setup(mock => mock.CountRowsAsync())
                .Returns(Task.FromResult<long>(maxNumberRowsInLocalDatabase - 50));

            // Second call to the remote Azure Table to retrieve rows
            azureTableRowCollection = new List<AzureTableRow> {
                new AzureTableRow {
                    PartitionKey = "PartionKey",
                    RowKey = "1234" }
            };
            
            azureTableRepositoryMock
                .Setup(mock => mock.GetRows(continuationToken, It.IsAny<int>()))
                .Returns((azureTableRowCollection, null));

            // Second insert in the local database
            localDbRepositoryMock
                .Setup(mock => mock.Insert(azureTableRowCollection))
                .Verifiable();

            var azureDownloader = new AzureTableDataDownloader(azureTableRepositoryMock.Object,
                localDbRepositoryMock.Object,
                _loggerMock.Object);
            var result = await azureDownloader.DownloadAsync(maxNumberRowsInLocalDatabase);

            localDbRepositoryMock.VerifyAll();
            azureTableRepositoryMock.VerifyAll();
        }

        [Test]
        public async Task Download_whenThereAreNoRowsInTheRemoteTable_andTooManyRowsInTheLocalDatabase_shouldNotReadDataFromTheAzureTable_andNotInsertDataIntheLocalDatabase()
        {
            // Given there are rows in the remote Azure table
            // And the number of rows in the local database is above the threshold
            // When calling the Download method
            // Should not retrieve rows from the Azure table
            // And should not insert the data in the local database
            var azureTableRepositoryMock = new Mock<IAzureTableRepository>(MockBehavior.Strict);
            azureTableRepositoryMock
                .Setup(mock => mock.HasRows())
                .Returns(true);

            // First check on the local database number of rows
            var maxNumberRowsInLocalDatabase = 1000;
            var localDbRepositoryMock = new Mock<ILocalDbRepository>(MockBehavior.Strict);
            localDbRepositoryMock
                .Setup(mock => mock.CountRowsAsync())
                .Returns(Task.FromResult<long>(maxNumberRowsInLocalDatabase + 100));

            // Should not call the Azure table to retrieve data
            azureTableRepositoryMock
                .Setup(mock => mock.GetRows(null, It.IsAny<int>()))
                .Verifiable();

            var azureDownloader = new AzureTableDataDownloader(azureTableRepositoryMock.Object,
                localDbRepositoryMock.Object,
                _loggerMock.Object);
            var result = await azureDownloader.DownloadAsync(maxNumberRowsInLocalDatabase);

            localDbRepositoryMock.VerifyAll();
            azureTableRepositoryMock
                .Verify(mock => mock.GetRows(It.IsAny<TableContinuationToken>(), It.IsAny<int>()), Times.Never);
        }
    }
}
