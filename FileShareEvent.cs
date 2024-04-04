using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Http;
using System.Text;
using System.Threading.Tasks;
using Azure.Messaging.EventHubs;
using Microsoft.Azure.WebJobs;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;

namespace Fileshare_Trigger
{
    public static class FileShareEvent
    {
        [FunctionName("FileShareTrigger")]
        public static async Task Run(
            [EventHubTrigger("fileshare-write-event", 
            Connection = "EVENT_HUB_CONNECTION_STRING")] EventData[] events, 
            ILogger log)
        {
            var exceptions = new List<Exception>();

            foreach (EventData eventData in events)
            {
                try
                {
                    // Process message here
                    await Task.Yield();
                    string targetPath = Environment.GetEnvironmentVariable("TARGET_DIRECTORY");
                    LogStream logs = JsonConvert.DeserializeObject<LogStream>(eventData.EventBody.ToString());
                    foreach (Record record in logs.records)
                    {
                        // Only process for file that is completed
                        if (record.operationName == "PutRange")
                        {
                            string url = record.uri;
                            Uri uri = new Uri(url);
                            string filePath = uri.AbsolutePath;

                            if(targetPath != null && filePath.Contains(targetPath))
                            {
                                // Target path is defined
                                await TriggerLogicApp(filePath, log);
                            }
                            else if(targetPath == null)
                            {
                                // No target path defined, so all files will be sent out.
                                await TriggerLogicApp(filePath, log);
                            }
                        }
                    };
                }
                catch (Exception e)
                {
                    // We need to keep processing the rest of the batch - capture this exception and continue.
                    // Also, consider capturing details of the message that failed processing so it can be processed again later.
                    exceptions.Add(e);
                }
            }

            // Once processing of the batch is complete, if any messages in the batch failed processing throw an exception so that there is a record of the failure.

            if (exceptions.Count > 1)
                throw new AggregateException(exceptions);

            if (exceptions.Count == 1)
                throw exceptions.Single();
        }

        public static async Task TriggerLogicApp(string filePath, ILogger log)
        {
            LogicAppBody body = new LogicAppBody()
            {
                path = filePath
            };

            HttpClient client = new HttpClient();
            string logicAppURL = Environment.GetEnvironmentVariable("LOGIC_APP_URL");

            var response = await client.PostAsync(
                logicAppURL, 
                new StringContent(JsonConvert.SerializeObject(body), 
                Encoding.UTF8, 
                "application/json"));

            if (response.IsSuccessStatusCode)
            {
                log.LogInformation($"Request sent for {filePath}.");
            }
            else
            {
                log.LogError("Message not sent successfully.");
            }
        }
    }
}
