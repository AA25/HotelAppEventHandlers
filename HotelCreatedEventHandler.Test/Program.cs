// See https://aka.ms/new-console-template for more information

using System.Text.Json;
using Amazon.Lambda.SNSEvents;
using HotelCreatedEventHandler.Models;

Environment.SetEnvironmentVariable("host", "https://search-hotels-25xqscxxtzr5irwawuexmodvbq.aos.eu-west-2.on.aws/_dashboards");
Environment.SetEnvironmentVariable("userName", "elastic");
Environment.SetEnvironmentVariable("password", "");
Environment.SetEnvironmentVariable("indexName", "event");

var hotel = new Hotel()
{
    Name = "Hotel",
    CityName = "Seattle",
    Price = 1,
    Rating = 5,
    Id = "123",
    UserId = "ABC",
    CreationDateTime = DateTime.Now
};

var snsEvent = new SNSEvent()
{
    Records = new List<SNSEvent.SNSRecord>()
    {
        new SNSEvent.SNSRecord()
        {
            Sns = new SNSEvent.SNSMessage()
            {
                MessageId = "100",
                Message = JsonSerializer.Serialize(hotel)
            }
        }
    }
};

var handler = new HotelCreatedEventHandler.HotelCreatedEventHandler();
await handler.Handler(snsEvent);