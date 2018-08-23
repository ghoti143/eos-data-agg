using System;
using System.Linq;
using MongoDB.Driver;
using MongoDB.Bson;
using MongoDB.Bson.Serialization.Attributes;
using MongoDB.Driver.Linq;
using System.Collections.Generic;


namespace EosDataAgg
{
    [BsonIgnoreExtraElements]
    class Transaction {
        [BsonElement("block_num")]
        public int BlockNum {get; set;}
        
        [BsonElement("actions")]
        public IEnumerable<Action> Actions {get; set;}
        
        [BsonElement("trx_id")]
        public string TrxId {get; set;}

    }

    [BsonIgnoreExtraElements]
    class Action {
        [BsonElement("account")]
        public string Account {get; set;}
        [BsonElement("name")]
        public string Name {get; set;}
    }

    [BsonIgnoreExtraElements]
    class TransactionTrace {
        [BsonElement("receipt")]
        public Receipt Receipt {get; set;}
        
        [BsonElement("actions")]
        public IEnumerable<Action> Actions {get; set;}

        [BsonElement("id")]
        public string Id {get; set;}
    }

    [BsonIgnoreExtraElements]
    class Receipt {
        [BsonElement("status")]
        public string status {get; set;}
        
        [BsonElement("cpu_usage_us")]
        public int cpuUsage {get; set;}

        [BsonElement("net_usage_words")]
        public int netUsage {get; set;}
    }

    class Program
    {
        static void Main(string[] args)
        {
            //TODO: use appsettings json

            /* 
            what's in config file?  
                * connection string
                * array of account;name

            https://ballardsoftware.com/adding-appsettings-json-configuration-to-a-net-core-console-application/
            https://blog.bitscry.com/2017/05/30/appsettings-json-in-net-core-console-app/
            
            */

            var client = new MongoClient("mongodb://localhost:27018");
            var database = client.GetDatabase("EOS");
            var transColl = database.GetCollection<Transaction>("transactions");

            var transTraceColl = database.GetCollection<TransactionTrace>("transaction_traces");

            // first get max block number.

            var blockColl = database.GetCollection<BsonDocument>("blocks");
            var max = blockColl.Aggregate()
                .Group(new BsonDocument { 
                    { "_id", "max" }, 
                    { "max", new BsonDocument("$min", "$block_num") }
                });
            var res = max.Single();
            var maxBlock = res["max"].AsInt32;
            Console.WriteLine(maxBlock);


            // now we will perf counts for the last 1,000,000 blocks (config param)
            var blockLimit = 1000000;
            var minBlock = maxBlock - blockLimit;


            var query = 
                from trans in transColl.AsQueryable()
                join trace in transTraceColl on trans.TrxId equals trace.Id into foo
                where 
                    trans.BlockNum >= minBlock && 
                    trans.Actions.Any(a => a.Account == "blocktwitter") &&
                    trans.Actions.Any(a => a.Name == "tweet")
                select new { trans.BlockNum, trans.Actions, foo };

            var result = query.Average(f => f.foo.First().Receipt.cpuUsage);
/*
            var agg = collection.Aggregate()
                .Match(new BsonDocument {
                    {"block_num", new BsonDocument{{"$gte", minBlock}} },
                    {"actions.account", new BsonDocument {{"$eq", "blocktwitter"}} },
                    {"actions.name", new BsonDocument {{"$eq", "tweet"}} }
                })
                .GraphLookup(BsonDocument.Parse("{  from: 'transaction_traces',  startWith: '$trx_id',  connectFromField: 'trx_id',  connectToField: 'id',  as: 'traces', maxDepth: 0}"))
                .Group(new BsonDocument { 
                    { "_id", "avgCpu" }, 
                    { "avgCpu", new BsonDocument("$avg", "$block_num") }
                });

   */

            

            Console.WriteLine(result.ToString());

            /* 
            var aggregate = collection.Aggregate()
                .Sort(new BsonDocument {
                    {"createdAt", -1}
                })
                .Match(new BsonDocument { 
                    {"action_traces.act.account", new BsonDocument {{"$eq", "eosio.token"}} },
                    {"action_traces.act.name", new BsonDocument {{"$eq", "transfer"}} }
                }).Count();
                
                                      
                                      
            var result = aggregate.Single();
            
            Console.WriteLine(aggregate.ToString());

            Console.WriteLine(result.Count);
            */
        }
    }
}

/*
.Group(new BsonDocument { 
                    { "_id", "avgCpu" }, 
                    { "avgCpu", new BsonDocument("$avg", "$receipt.cpu_usage_us") },
                    { "avgNet", new BsonDocument("$avg", "$receipt.net_usage_words") }
                });

.Match(new BsonDocument { 
                    {"action_traces.act.account", new BsonDocument {{"$eq", "blocktwitter"}} },
                    {"action_traces.act.name", new BsonDocument {{"$eq", "tweet"}} }
                });
                
.Group(new BsonDocument { 
                                           { "_id", "avgCpu" }, 
                                           { "avgCpu", new BsonDocument("$avg", "$receipt.cpu_usage_us") },
                                           { "avgNet", new BsonDocument("$avg", "$receipt.net_usage_words") }
                                        });

.Group(BsonDocument.Parse("{  _id: 'avgCpu',    avgCpu: {        $avg: '$receipt.cpu_usage_us'    },    avgNet: {        $avg: '$receipt.net_usage_words'   }}"));

 */