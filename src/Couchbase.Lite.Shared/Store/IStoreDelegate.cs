using System;
using System.Collections.Generic;

namespace Couchbase.Lite.Store
{
    /// <summary>
    /// Delegate of a ICouchStorage instance. Database implements this.
    /// </summary>
    public interface IStoreDelegate
    {

        /// <summary>
        /// Optional encryption key registered with this database.
        /// </summary>
        /// <value>The encryption key.</value>
        SymmetricKey EncryptionKey { get; }

        /// <summary>
        /// Called whenever the outermost transaction completes.
        /// </summary>
        /// <param name="committed"><c>true</c> on commit, <c>false</c> if the transaction was aborted.</param>
        void StorageExitedTransaction(bool committed);

        /// <summary>
        /// Called whenever a revision is added to the database (but not for local docs or for purges.) 
        /// </summary>
        void DatabaseStorageChanged(DocumentChange change);

        /// <summary>
        /// Generates a revision ID for a new revision.
        /// </summary>
        /// <param name="json">The canonical JSON of the revision (with metadata properties removed.)</param>
        /// <param name="deleted"><c>true</c> if this revision is a deletion</param>
        /// <param name="prevRevId">The parent's revision ID, or nil if this is a new document.</param>
        string GenerateRevID(IEnumerable<byte> json, bool deleted, string prevRevId);

    }
}

