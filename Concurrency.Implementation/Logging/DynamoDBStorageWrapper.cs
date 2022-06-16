using System;
using System.IO;
using Utilities;
using Amazon.DynamoDBv2;
using System.Threading.Tasks;
using Amazon.DynamoDBv2.Model;
using System.Collections.Generic;
using Concurrency.Interface.Logging;
using Amazon.DynamoDBv2.DocumentModel;

namespace Concurrency.Implementation.Logging
{
    class DynamoDBStorageWrapper : IKeyValueStorageWrapper
    {
        AmazonDynamoDBClient client;        
        string grainType;
        byte[] grainKey;
        string logName;
        const string ATT_KEY_1 = "GRAIN_REFERENCE";
        const string ATT_KEY_2 = "SEQUENCE_NUMBER";
        const string ATT_VALUE = "VALUE";
        const int READ_CAPACITY_UNITS = 10;
        const int WRITE_CAPACITY_UNITS = 10;
        
        bool singleTable = true;
        bool tableExists = false;


        public DynamoDBStorageWrapper(string grainType, int grainID)
        {
            string ServiceRegion;
            string AccessKey;
            string SecretKey;

            using (var file = new StreamReader(Constants.credentialFile))
            {
                ServiceRegion = file.ReadLine();
                AccessKey = file.ReadLine();
                SecretKey = file.ReadLine();
            }

            client = new AmazonDynamoDBClient(AccessKey, SecretKey, Amazon.RegionEndpoint.USEast2);
            //Console.WriteLine("Initialized dynamodb client");
            this.grainType = grainType;
            this.grainKey = BitConverter.GetBytes(grainID);
            if (singleTable) logName = Constants.ServiceID;
            else logName = grainType + grainKey;

            
        }
                
        async Task createTableIfNotExists()
        {
            var describeTableRequest = new DescribeTableRequest()
            {
                TableName = logName
            };        
            try
            {
                DescribeTableResponse response;                
                do
                {
                    response = await client.DescribeTableAsync(describeTableRequest);
                    //Console.WriteLine($"Current table status = {response.Table.TableStatus}");
                    await Task.Delay(TimeSpan.FromSeconds(5));
                } while (response.Table.TableStatus != TableStatus.ACTIVE);
                tableExists = true;
            } 
            catch(ResourceNotFoundException)
            {
                tableExists = false;
            }
            if(!tableExists)
            {
                var request = new CreateTableRequest()
                {
                    TableName = logName,
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
                while(true)
                {                    
                    var response = await client.DescribeTableAsync(describeTableRequest);
                    if (response.Table.TableStatus == TableStatus.ACTIVE) break;
                    else await Task.Delay(TimeSpan.FromSeconds(5));
                }                    
            }
        }

        Task<byte[]> IKeyValueStorageWrapper.Read(byte[] key)
        {
            throw new NotImplementedException();
        }

        async Task insertItemUsingLowLevelAPI(byte[] key, byte[] value)
        {
            var request = new PutItemRequest()
            {
                TableName = logName,
                Item = new Dictionary<string, AttributeValue>() {
                    {ATT_KEY_1, new AttributeValue() { B = new MemoryStream(grainKey)}},
                    {ATT_KEY_2, new AttributeValue() { B = new MemoryStream(key)}},
                    {ATT_VALUE, new AttributeValue() { B = new MemoryStream(value)}}
                }
            };
            await client.PutItemAsync(request);
        }

        async Task insertItemUsingDocumentModel(byte[] key, byte[] value)
        {
            var table = Table.LoadTable(client, logName);
            var item = new Document();
            item[ATT_KEY_1] = grainKey;
            item[ATT_KEY_2] = key;
            item[ATT_VALUE] = value;
            await table.PutItemAsync(item);
        }

        async Task IKeyValueStorageWrapper.Write(byte[] key, byte[] value)
        {
            if(!tableExists)
            {
                await createTableIfNotExists();
            }
            //Use one of the following
            await insertItemUsingLowLevelAPI(key, value);
            //await insertItemUsingDocumentModel(key, value);
        }
    }
}
