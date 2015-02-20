using System;
using System.Collections.Generic;

namespace Couchbase.Lite.Store
{

    /// <summary>
    /// Storage for a view. Instances are created by ICouchStore implementations, and are owned by
    /// View instances.
    /// </summary>
    internal interface IViewStorage
    {

        /// <summary>
        /// The name of the view.
        /// </summary>
        string Name { get; }

        /// <summary>
        /// The delegate (in practice, the owning View itself.)
        /// </summary>
        /// <value>The delegate.</value>
        IViewStorageDelegate Delegate { get; }

        /// <summary>
        /// Closes the storage.
        /// </summary>
        void Close();

        /// <summary>
        /// Erases the view's index.
        /// </summary>
        void DeleteIndex();

        /// <summary>
        /// Deletes the view's storage (metadata and index), removing it from the database.
        /// </summary>
        void DeleteView();

        /// <summary>
        /// Updates the version of the view. A change in version means the delegate's map block has
        /// changed its semantics, so the index should be deleted.
        /// </summary>
        bool SetVersion(string version);

        /// <summary>
        /// The total number of rows in the index.
        /// </summary>
        uint TotalRows { get; }

        /// <summary>
        /// The last sequence number that has been indexed.
        /// </summary>
        Int64 LastSequenceIndexed { get; }

        /// <summary>
        /// The last sequence number that caused an actual change in the index.
        /// </summary>
        Int64 LastSequenceChangedAt { get; }

        /// <summary>
        /// Updates the indexes of one or more views in parallel.
        /// </summary>
        /// <returns>The success/error status.</returns>
        /// <param name="views">An array of IViewStorage instances, always including the receiver.</param>
        Status UpdateIndexes(IEnumerable<IViewStorage> views);

        /// <summary>
        /// Queries the view without performing any reducing or grouping.
        /// </summary>
        QueryEnumerator RegularQuery(QueryOptions options);

        /// <summary>
        /// Queries the view, with reducing or grouping as per the options.
        /// </summary>
        QueryEnumerator ReducedQuery(QueryOptions options);

        /// <summary>
        /// Performs a full-text query as per the options.
        /// </summary>
        QueryEnumerator FullTextQuery(QueryOptions options);

        IQueryRowStorage StorageForQueryRow(QueryRow row);

        #if DEBUG

        IEnumerable<IDictionary<string, object>> Dump();

        #endif
    }
}

