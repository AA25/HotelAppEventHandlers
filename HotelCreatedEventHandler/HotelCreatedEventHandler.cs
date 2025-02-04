using System.Reflection;
using System.Text.Json;
using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.DocumentModel;
using Amazon.Lambda.Core;
using Amazon.Lambda.Serialization.SystemTextJson;
using Amazon.Lambda.SNSEvents;
using HotelCreatedEventHandler.Models;
using Nest;

[assembly: LambdaSerializer(typeof(DefaultLambdaJsonSerializer))]

namespace HotelCreatedEventHandler;

public class HotelCreatedEventHandler
{
    public async Task Handler(SNSEvent snsEvent)
    {
        // note: document model instead of object persistence model (latter is the "orm" way)
        var dbClient = new AmazonDynamoDBClient();
        var table = Table.LoadTable(dbClient, "HotelCreatedEventIds");
        
        var host = Environment.GetEnvironmentVariable("host");
        var userName = Environment.GetEnvironmentVariable("userName");
        var password = Environment.GetEnvironmentVariable("password");
        // elasticsearch namespace
        var indexName = Environment.GetEnvironmentVariable("indexName");
                
        var connSettings = new ConnectionSettings(new Uri(host));
        connSettings.BasicAuthentication(userName, password);
        connSettings.DefaultIndex(indexName);
        // ensures hotel.id is used as the id of the document in elasticsearch (instead of it gen'ing a random one)
        connSettings.DefaultMappingFor<Hotel>(m => m.IdProperty(p => p.Id));
                
        var esClient = new Nest.ElasticClient(connSettings);
        
        // AWS may send more than 1 event at a time
        foreach (var eventRecord in snsEvent.Records)
        {
            var eventId = eventRecord.Sns.MessageId;
            var foundItem = await table.GetItemAsync(eventId);
            if (foundItem == null)
            {
                await table.PutItemAsync(new Document()
                {
                    ["eventId"] = eventId
                });
                
                // Ensuring the index exists in elasticsearch
                if (!(await esClient.Indices.ExistsAsync(indexName)).Exists)
                {
                    await esClient.Indices.CreateAsync(indexName);
                }

                var hotel = JsonSerializer.Deserialize<Hotel>(eventRecord.Sns.Message);
                await esClient.IndexDocumentAsync<Hotel>(hotel);
            }
        }
    }
}