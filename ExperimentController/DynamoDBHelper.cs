using System;
using Amazon.DynamoDBv2;
using System.Threading.Tasks;
using Amazon.DynamoDBv2.Model;
using System.Collections.Generic;

namespace ExperimentConductor
{
    class DynamoDBHelper
    {
        AmazonDynamoDBClient client;
        string grainType;
        byte[] grainKey;
        string logName;
        const string ATT_KEY_1 = "EXPERIMENT";
        const string ATT_KEY_2 = "NUMBER";
        const string ATT_VALUE = "VALUE";
        const string DYNAMODB_ACCESS_KEY_ID = "";
        const string DYNAMODB_ACCESS_KEY_VALUE = "";
        const int READ_CAPACITY_UNITS = 10;
        const int WRITE_CAPACITY_UNITS = 10;

        bool singleTable = true;
        bool tableExists = false;

        public DynamoDBHelper()
        {
            client = new AmazonDynamoDBClient(DYNAMODB_ACCESS_KEY_ID, DYNAMODB_ACCESS_KEY_VALUE, Amazon.RegionEndpoint.USWest2);
            Console.WriteLine("Initialized dynamodb client");


        }

        async Task createTableIfNotExists(string experiment)
        {
            var name = experiment;
            var describeTableRequest = new DescribeTableRequest()
            {
                TableName = name
            };
            try
            {
                DescribeTableResponse response;
                do
                {
                    response = await client.DescribeTableAsync(describeTableRequest);
                    Console.WriteLine("Current table status = {0}", response.Table.TableStatus);
                    await Task.Delay(TimeSpan.FromSeconds(5));
                } while (response.Table.TableStatus != TableStatus.ACTIVE);
                tableExists = true;
            }
            catch (ResourceNotFoundException)
            {
                tableExists = false;
            }
            if (!tableExists)
            {
                var request = new CreateTableRequest()
                {
                    TableName = name,
                    KeySchema = new List<KeySchemaElement>()
                    {
                        new KeySchemaElement()
                        {
                            AttributeName = ATT_KEY_1,
                            KeyType = "HASH"
                        },
                        new KeySchemaElement()
                        {
                            AttributeName = ATT_KEY_2,
                            KeyType = "RANGE"
                        }
                    },
                    AttributeDefinitions = new List<AttributeDefinition>()
                    {
                        new AttributeDefinition()
                        {
                            AttributeName = ATT_KEY_1,
                            AttributeType = "B"
                        },
                        new AttributeDefinition()
                        {
                            AttributeName = ATT_KEY_2,
                            AttributeType = "B"
                        }
                    },
                    ProvisionedThroughput = new ProvisionedThroughput
                    {
                        ReadCapacityUnits = READ_CAPACITY_UNITS,
                        WriteCapacityUnits = WRITE_CAPACITY_UNITS
                    }
                };
                await client.CreateTableAsync(request);
                while (true)
                {
                    var response = await client.DescribeTableAsync(describeTableRequest);
                    if (response.Table.TableStatus == TableStatus.ACTIVE)
                        break;
                    else
                        await Task.Delay(TimeSpan.FromSeconds(5));
                }
            }
        }
    }
}
