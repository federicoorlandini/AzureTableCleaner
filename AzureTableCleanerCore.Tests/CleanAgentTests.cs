using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using AzureTableCleanerCore.Logic;
using AzureTableCleanerCore.Models;
using AzureTableCleanerCore.Repositories;

using Moq;
using NLog;
using NUnit.Framework;

namespace AzureTableCleanerCore.Tests
{
    [TestFixture]
    public class CleanAgentTests
    {
        private Mock<ILogger> _logger = new Mock<ILogger>();

        [Test]
        public async Task IterationAsync_shouldCheckIfLocalDatabaseContainsRowsToDelete()
        {
            // Given there
            // When calling the Iteration method
            // Then the code should call the repository method to check if there is any rows in the local database

            var localDbRepositoryMock = new Mock<ILocalDbRepository>(MockBehavior.Default);
            localDbRepositoryMock
                .SetupSequence(mock => mock.HasRowsAsync())
                .Returns(Task.FromResult(false));

            var azureTableRepositoryMock = new Mock<IAzureTableRepository>(MockBehavior.Default);

            var localDbRowsProcessorMock = new Mock<ILocalDbRowsProcessor>(MockBehavior.Strict);
            localDbRowsProcessorMock
                .Setup(mock => mock.Process(It.IsAny<int>()))
                .Verifiable();

            var azureTableDataDownloaderMock = new Mock<IAzureTableDataDownloader>(MockBehavior.Strict);

            var agent = new CleanAgent(localDbRowsProcessorMock.Object, 
                localDbRepositoryMock.Object, 
                azureTableRepositoryMock.Object, 
                azureTableDataDownloaderMock.Object,
                _logger.Object);
            await agent.IterationAsync();

            localDbRowsProcessorMock.Verify(m => m.Process(It.IsAny<int>()), Times.Never);
            azureTableRepositoryMock.VerifyAll();
            localDbRepositoryMock.VerifyAll();
            azureTableDataDownloaderMock.VerifyAll();
        }

        [Test]
        public async Task IterationAsync_whenThereAreRowsInTheLocalDatabase_shouldCallTheLocalDbRowsProcessor()
        {
            // Given there are rows in the local database
            // When calling the Iteration method
            // Then the code should call the LocalDbRowsProcessor.Process() method

            var localDbRepositoryMock = new Mock<ILocalDbRepository>(MockBehavior.Default);
            localDbRepositoryMock
                .SetupSequence(mock => mock.HasRowsAsync())
                .Returns(Task.FromResult(true))
                .Returns(Task.FromResult(false));

            var azureTableRepositoryMock = new Mock<IAzureTableRepository>(MockBehavior.Default);

            var localDbRowsProcessorMock = new Mock<ILocalDbRowsProcessor>(MockBehavior.Strict);
            localDbRowsProcessorMock
                .Setup(mock => mock.Process(It.IsAny<int>()))
                .Verifiable();

            var azureTableDataDownloaderMock = new Mock<IAzureTableDataDownloader>(MockBehavior.Strict);

            var agent = new CleanAgent(localDbRowsProcessorMock.Object,
                localDbRepositoryMock.Object,
                azureTableRepositoryMock.Object,
                azureTableDataDownloaderMock.Object,
                _logger.Object);
            await agent.IterationAsync();

            localDbRowsProcessorMock.Verify(m => m.Process(It.IsAny<int>()), Times.Once);
            azureTableRepositoryMock.VerifyAll();
            localDbRepositoryMock.VerifyAll();
            azureTableDataDownloaderMock.VerifyAll();
        }

        [Test]
        public async Task IterationAsync_WhenThereIsNoRowsInTheLocalDatabase_shouldCheckIfThereIsAnyRowInTheAzureTable()
        {
            // Given there is no rows in the local database
            // When calling the Iteration method
            // Then the code should call the Azure table repository to check if there is any row in the remote table

            var localDbRepositoryMock = new Mock<ILocalDbRepository>(MockBehavior.Strict);
            localDbRepositoryMock
                .Setup(mock => mock.DropTable())
                .Verifiable();

            localDbRepositoryMock
                .SetupSequence(mock => mock.HasRowsAsync())
                .Returns(Task.FromResult(false));

            var azureTableRepositoryMock = new Mock<IAzureTableRepository>(MockBehavior.Strict);
            azureTableRepositoryMock
                .Setup(mock => mock.HasRows())
                .Returns(false);

            var azureTableDataDownloaderMock = new Mock<IAzureTableDataDownloader>(MockBehavior.Strict);

            var localDbRowsProcessorMock = new Mock<ILocalDbRowsProcessor>(MockBehavior.Strict);

            var agent = new CleanAgent(localDbRowsProcessorMock.Object,
                localDbRepositoryMock.Object,
                azureTableRepositoryMock.Object,
                azureTableDataDownloaderMock.Object,
                _logger.Object);

            await agent.IterationAsync();

            azureTableRepositoryMock.VerifyAll();
        }

        [Test]
        public async Task IterationAsync_whereThereIsNoRowsInTheLocalDatabaseAndInTheAzureTable_shouldReturnFalse()
        {
            // Given there is no rows in the local database
            // And there is no row in the azure table
            // When calling the Iteration method
            // Then the code should return false
            var localDbRepositoryMock = new Mock<ILocalDbRepository>(MockBehavior.Strict);
            localDbRepositoryMock
                .Setup(mock => mock.DropTable())
                .Verifiable();

            localDbRepositoryMock
                .SetupSequence(mock => mock.HasRowsAsync())
                .Returns(Task.FromResult(false));

            var azureTableRepositoryMock = new Mock<IAzureTableRepository>(MockBehavior.Strict);
            azureTableRepositoryMock
                .Setup(mock => mock.HasRows())
                .Returns(false);

            var azureTableDataDownloaderMock = new Mock<IAzureTableDataDownloader>(MockBehavior.Strict);

            var localDbRowsProcessorMock = new Mock<ILocalDbRowsProcessor>(MockBehavior.Strict);

            var agent = new CleanAgent(localDbRowsProcessorMock.Object,
                localDbRepositoryMock.Object,
                azureTableRepositoryMock.Object,
                azureTableDataDownloaderMock.Object,
                _logger.Object);

            var result = await agent.IterationAsync();

            Assert.IsFalse(result, "The Iteration method should return false");
        }

        [Test]
        public async Task IterationAsync_whereThereIsNoRowsInTheLocalDatabaseButThereAreInTheAzureTable_shouldTriggerTheAzureTableDownloaderAndReturnTrue()
        {
            // Given there are no rows in the local database
            // And the query to the Azure table returns rows
            // Then the code should call the AzureTableDownloader.Download() method
            // And the code should return true
            var localDbRepositoryMock = new Mock<ILocalDbRepository>(MockBehavior.Strict);
            localDbRepositoryMock
                .Setup(mock => mock.DropTable())
                .Verifiable();

            localDbRepositoryMock
                .SetupSequence(mock => mock.HasRowsAsync())
                .Returns(Task.FromResult(false));

            var azureTableRepositoryMock = new Mock<IAzureTableRepository>(MockBehavior.Strict);
            azureTableRepositoryMock
                .Setup(mock => mock.HasRows())
                .Returns(true);

            var azureTableDataDownloaderMock = new Mock<IAzureTableDataDownloader>(MockBehavior.Strict);
            azureTableDataDownloaderMock
                .Setup(mock => mock.DownloadAsync(It.IsAny<long>()))
                .Returns(Task.FromResult(true));

            var localDbRowsProcessorMock = new Mock<ILocalDbRowsProcessor>(MockBehavior.Strict);

            var agent = new CleanAgent(localDbRowsProcessorMock.Object,
                localDbRepositoryMock.Object,
                azureTableRepositoryMock.Object,
                azureTableDataDownloaderMock.Object,
                _logger.Object);

            var result = await agent.IterationAsync();

            Assert.IsTrue(result, "The Iteration method should return true");
        }
    }
}
