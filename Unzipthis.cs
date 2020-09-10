
using System;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Threading.Tasks;
using System.Web.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.Extensions.Logging;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.File;


namespace AzUnzipEverything
{
    public class PostData
    {
        public string name { get;set; }   
        public string storageAccount { get; set; } 
        public string sourceFileShare { get; set; }
        public string destinationFolder { get; set; }
    }
    public static class Unzipthis
    {
        [FunctionName("Unzipthis")]
        public static async Task<IActionResult> Run([HttpTrigger(AuthorizationLevel.Function, "post")] PostData data, ILogger log)
        {
            
            log.LogInformation($"C# Blob trigger function Processed blob\n Name:{data.name}\n StorageAccount: {data.storageAccount}\n SourceFileShare: {data.sourceFileShare}\n DestinationFolder: {data.destinationFolder}");

            if(data.name.Split('.').Last().ToLower() == "zip"){

                try{ 
                    CloudStorageAccount storageAccount = CloudStorageAccount.Parse(data.storageAccount);
                    CloudFileClient fileClient = storageAccount.CreateCloudFileClient();
                    CloudFileShare fileShare = fileClient.GetShareReference(data.sourceFileShare);
                    CloudFile zipFile = fileShare.GetRootDirectoryReference().GetFileReference(data.name);
                    
                    MemoryStream blobMemStream = new MemoryStream();
                        
                    await zipFile.DownloadToStreamAsync(blobMemStream);
                    extract(blobMemStream, log, fileShare, data);
                }
                catch(Exception ex){
                    log.LogInformation($"Error! Something went wrong: {ex.Message}");
                    return new ExceptionResult(ex, true); 
                }  
            }
            
            return new OkObjectResult("Unzip succesfull");
        }
        public static async void extract(Stream memStream, ILogger log, CloudFileShare fileShare, PostData data) {                
            using(ZipArchive archive = new ZipArchive(memStream))
            {
                foreach (ZipArchiveEntry entry in archive.Entries)
                {
                    log.LogInformation($"Now processing {entry.FullName}");

                    CloudFile destination = fileShare.GetRootDirectoryReference().GetDirectoryReference(data.destinationFolder).GetFileReference(entry.FullName);          
                    log.LogInformation($"Destination is: {destination.StorageUri}");
                    await destination.Parent.CreateIfNotExistsAsync();

                    if (entry.FullName.EndsWith(".zip")) {
                        log.LogInformation($"Recursive call on file {entry.FullName}");
                        extract(entry.Open(), log, fileShare, data);
                    } else {
                        using (var fileStream = entry.Open())
                        {
                            StreamReader reader = new StreamReader( fileStream );
                            string text = reader.ReadToEnd();
                            await destination.UploadTextAsync(text);
                        }
                    }
                }
            }
            
        }

    }
}