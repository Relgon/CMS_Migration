using Microsoft.Extensions.Configuration;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;

namespace CMS_Migration
{
    class Program
    {
        private static StorageOptions _options;

        static async Task Main(string[] args)
        {
            try
            {
                IConfigurationRoot configuration = new ConfigurationBuilder()
                    .SetBasePath(Directory.GetCurrentDirectory())
                    .AddJsonFile("appsettings.json", false, true)
                    .Build();

                _options = configuration.GetSection("Storage").Get<StorageOptions>();
                await MigrateFiles(_options.ContentFolderPath);
                await MigrateFiles(_options.AlternativeContentFolderPath);
                await MigrateBlobs();

                //?
                //Task filesTask = MigrateFiles(_options.ContentFolderPath);
                //Task alternativeFilesTask = MigrateFiles(_options.AlternativeContentFolderPath);
                //Task blobsTask = MigrateBlobs();
                //await Task.WhenAll(filesTask, alternativeFilesTask, blobsTask);

            }
            catch (Exception e)
            {
                Console.ForegroundColor = ConsoleColor.Red;
                Console.WriteLine(e.Message);
                Console.ReadKey();
            }

            Console.WriteLine("Processed.");
            
        }

        private static async Task MigrateFiles(string sourceAppDataPath)
        {
            CloudBlobClient uatBlobClient = CloudStorageAccount.Parse(_options.UatConnectionString).CreateCloudBlobClient();

            IEnumerable<string> customerFolders = Directory.EnumerateDirectories(sourceAppDataPath);
            foreach (string customerFolder in customerFolders)
            {
                if (!int.TryParse(Path.GetFileName(customerFolder), out int customerId))
                {
                    continue;
                }

                CloudBlobContainer destinationContainer = uatBlobClient.GetContainerReference($"customer-{customerId}");
                Task labelTask = MigrateFiles(destinationContainer, customerFolder, "Label");
                Task tileTask = MigrateFiles(destinationContainer, customerFolder, "Tile");
                await Task.WhenAll(labelTask, tileTask);

            }
        }

        private static async Task MigrateFiles(CloudBlobContainer destinationContainer,string customerFolderPath, string folderToMigrate)
        {
            
            string directoryPath = Path.Combine(customerFolderPath, folderToMigrate);
            if (!Directory.Exists(directoryPath))
            {
                return;
            }
            Console.WriteLine($"Begin copy {folderToMigrate} from folder {directoryPath}");
            IEnumerable<string> filePaths = Directory.EnumerateFiles(directoryPath, "*.*", SearchOption.AllDirectories)
                .ToList();
            if (filePaths.Any())
            {
                await destinationContainer.CreateIfNotExistsAsync();
            }

            foreach (string filePath in filePaths)
            {
                string blobName = filePath
                    .Replace(customerFolderPath, "")
                    .TrimStart('\\')
                    .Replace('\\', '/');
                CloudBlockBlob destinationBlob = destinationContainer.GetBlockBlobReference(blobName);
                using (FileStream stream = File.OpenRead(filePath))
                {
                    await destinationBlob.UploadFromStreamAsync(stream);
                }

                await destinationBlob.FetchAttributesAsync();
                destinationBlob.Metadata.Add("WithCompression", false.ToString());
                await destinationBlob.SetMetadataAsync();
            }

            Console.WriteLine($"End copy {folderToMigrate} from folder {directoryPath}. Processed : {filePaths.Count()}");
        }

        public static async Task MigrateBlobs()
        {
            CloudBlobClient liveBlobClient = CloudStorageAccount.Parse(_options.LiveConnectionString).CreateCloudBlobClient();
            CloudBlobClient uatBlobClient = CloudStorageAccount.Parse(_options.UatConnectionString).CreateCloudBlobClient();

            BlobContinuationToken containerCt = null;

            do
            {
                ContainerResultSegment response = await liveBlobClient.ListContainersSegmentedAsync(containerCt);
                foreach (CloudBlobContainer sourceContainer in response.Results)
                {
                    CloudBlobContainer destinationContainer = uatBlobClient.GetContainerReference(sourceContainer.Name);
                    Task labelTask = MigrateBlobs(destinationContainer, sourceContainer, "Label");
                    Task tileTask = MigrateBlobs(destinationContainer, sourceContainer, "Tile");
                    await Task.WhenAll(labelTask, tileTask);
                }

                containerCt = response.ContinuationToken;
            } while (containerCt != null);


        }

        private static async Task MigrateBlobs(CloudBlobContainer destinationContainer, CloudBlobContainer sourceContainer, string prefix)
        {
            Console.WriteLine($"Begin copy {prefix} from container {sourceContainer.Name}");
            
            BlobContinuationToken blobCt = null;
            int blobsCount = 0;
            do
            {
                BlobResultSegment response = await sourceContainer.ListBlobsSegmentedAsync(prefix,true, BlobListingDetails.Metadata, 100, blobCt, new BlobRequestOptions(), new OperationContext());
                if (response.Results.Any())
                {
                    await destinationContainer.CreateIfNotExistsAsync();
                }
                foreach (CloudBlockBlob sourceBlob in response.Results)
                {
                    CloudBlockBlob destinationBlob = destinationContainer.GetBlockBlobReference(sourceBlob.Name);
                    using (Stream stream = await sourceBlob.OpenReadAsync())
                    {
                        await destinationBlob.UploadFromStreamAsync(stream);
                    }
                    foreach (KeyValuePair<string, string> keyValuePair in sourceBlob.Metadata)
                    {
                        destinationBlob.Metadata.Add(keyValuePair);
                    }

                    await destinationBlob.SetMetadataAsync();
                    blobsCount++;
                }

                blobCt = response.ContinuationToken;
            } while (blobCt != null);

            Console.WriteLine($"Begin copy {prefix} from container {sourceContainer.Name}. Processed : {blobsCount} records");
        }
    }
}
