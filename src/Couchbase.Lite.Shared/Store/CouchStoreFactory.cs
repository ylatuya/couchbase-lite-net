using System;
using System.Collections.Generic;
using Couchbase.Lite.Store;
using Couchbase.Lite.Util;

namespace Couchbase.Lite.Storage
{
    /// <summary>
    /// Factory for creating ICouchStore implementations
    /// </summary>
    internal static class CouchStoreFactory
    {
        private const string TAG = "CouchStoreFactory";
        private static readonly Dictionary<string, Func<ICouchStore>> Generators;

        static CouchStoreFactory()
        {
            Generators = new Dictionary<string, Func<ICouchStore>> {
                { "Sqlite", () => new SqliteStorage() }
            };
        }

        public static ICouchStore GetCouchStore(string type)
        {
            Func<ICouchStore> generator;
            if(!Generators.TryGetValue(type, out generator)) {
                return null;
            }

            return generator();
        }
    }
}

