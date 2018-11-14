using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;
using Concurrency.Interface.Logging;
using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.Model;
using Amazon.DynamoDBv2.DocumentModel;
using System.IO;


namespace Concurrency.Implementation.Logging
{
    class DynamoDBStorageWrapper : IKeyValueStorageWrapper
    {
        AmazonDynamoDBClient client;        
        String grainType;
        String grainKey;
        String logName;
        const string ATT_KEY = "key";
        const string ATT_VALUE = "value";
        bool tableExists = false;

        public DynamoDBStorageWrapper(String grainType, String grainKey)
        {
            client = new AmazonDynamoDBClient();
            this.grainType = grainType;
            this.grainKey = grainKey;
            this.logName = grainType + grainKey;            
        }

        async Task createTableIfNotExists()
        {
            var describeTableRequest = new DescribeTableRequest()
            {
                TableName = logName
            };        
            try
            {
                var response = await client.DescribeTableAsync(describeTableRequest);
            } catch(ResourceNotFoundException)
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
                            AttributeName = ATT_KEY,
                            KeyType = "HASH"
                        }
                    },
                    AttributeDefinitions = new List<AttributeDefinition>()
                    {
                        new AttributeDefinition()
                        {
                            AttributeName = ATT_KEY,
                            AttributeType = "B"
                        },
                        new AttributeDefinition()
                        {
                            AttributeName = ATT_VALUE,
                            AttributeType = "B"
                        }
                    }
                };
                await client.CreateTableAsync(request);
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
                    {ATT_KEY, new AttributeValue() { B = new MemoryStream(key) }},
                    {ATT_VALUE, new AttributeValue() { B = new MemoryStream(value)}}
                }
            };
            await client.PutItemAsync(request);
        }

        async Task insertItemUsingDocumentModel(byte[] key, byte[] value)
        {
            var table = Table.LoadTable(client, logName);
            var item = new Document();
            item[ATT_KEY] = key;
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
