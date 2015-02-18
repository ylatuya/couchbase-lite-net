using System;
using System.Collections.Generic;

namespace Couchbase.Lite.Store
{
    public interface ICouchStore
    {
        #region Initialization and Configuration 

        /// <summary>
        /// Preflight to see if a database file exists in this directory. Called _before_ Open()!
        /// </summary>
        /// <returns><c>true</c>, if a database exists in the directory, <c>false</c> otherwise.</returns>
        /// <param name="directory">The directory to check</param>
        bool DatabaseExistsIn(string directory);

        /// <summary>
        /// Opens storage. Files will be created in the directory, which must already exist.
        /// </summary>
        /// <param name="directory">The existing directory to put data files into. The implementation may
        /// create as many files as it wants here. There will be a subdirectory called "attachments"
        /// which contains attachments; don't mess with that..</param>
        /// <param name="readOnly">If this is <c>true</c>, the database is opened read-only and any attempt to modify
        /// it must return an error.</param>
        /// <param name="manager">The owning Manager; this is provided so the storage can examine its
        ///properties.</param>
        bool Open(string directory, bool readOnly, Manager manager);

        /// <summary>
        /// Closes storage before it's deallocated. 
        /// </summary>
        void Close();

        /// <summary>
        /// The delegate object, which in practice is the Database.
        /// </summary>
        IStoreDelegate Delegate { get; set; }

        /// <summary>
        /// The maximum depth a document's revision tree should grow to; beyond that, it should be pruned.
        /// This will be set soon after the Open() call.
        /// </summary>
        int MaxRevTreeDepth { get; set; }

        /// <summary>
        /// Whether the database storage should automatically (periodically) be compacted.
        /// This will be set soon after the Open() call.
        /// </summary>
        bool AutoCompact { get; set; }

        #endregion

        #region Database Attributes & Operations

        /// <summary>
        /// Stores an arbitrary string under an arbitrary key, persistently.
        /// </summary>
        Status SetInfo(string info, string key);

        /// <summary>
        /// Returns the value assigned to the given key by SetInfo().
        /// </summary>
        string GetInfo(string key);

        /// <summary>
        /// The number of (undeleted) documents in the database.
        /// </summary>
        uint DocumentCount { get; }

        /// <summary>
        /// The last sequence number allocated to a revision.
        /// </summary>
        Int64 LastSequence { get; }

        /// <summary>
        /// Is a transaction active?
        /// </summary>
        bool InTransaction { get; }

        /// <summary>
        /// Explicitly compacts document storage.
        /// </summary>
        bool Compact();

        /// <summary>
        /// Executes the block within a database transaction.
        /// If the block returns a non-OK status, the transaction is aborted/rolled back.
        /// If the block returns DbBusy, the block will also be retried after a short delay;
        /// if 10 retries all fail, the DbBusy will be returned to the caller.
        /// Any exception raised by the block will be caught and treated as Exception.
        /// </summary>
        Status RunInTransaction(Func<Status> block);

        #endregion

        #region Documents

        /// <summary>
        /// Retrieves a document revision by ID.
        /// </summary>
        /// <returns>The revision, or null if not found.</returns>
        /// <param name="docId">The document ID</param>
        /// <param name="revId">The revision ID; may be nil, meaning "the current revision".</param>
        /// <param name="options">Specifies which data to include in the JSON.</param>
        Revision GetDocument(string docId, string revId, DocumentContentOptions options);

        /// <summary>
        /// Loads the body of a revision.
        /// On entry, rev.docID and rev.revID will be valid.
        /// On success, rev.body will be valid.
        /// </summary>
        Status LoadRevisionBody(Revision rev, DocumentContentOptions options);

        /// <summary>
        /// Looks up the sequence number of a revision.
        /// Will only be called on revisions whose .sequence property is not already set.
        /// Does not need to set the revision's .sequence property; the caller will take care of that.
        /// </summary>
        Int64 GetRevisionSequence(Revision rev);

        /// <summary>
        /// Retrieves the parent revision of a revision, or returns null if there is no parent.
        /// </summary>
        Revision GetParentRevision(Revision rev);

        /// <summary>
        /// Returns the given revision's list of direct ancestors (as CBL_Revision objects) in _reverse_
        /// chronological order, starting with the revision itself.
        /// </summary>
        IEnumerable<string> GetRevisionHistory(Revision rev);

        /// <summary>
        /// Returns the revision history as a _revisions dictionary, as returned by the REST API's ?revs=true option. 
        /// If 'ancestorRevIDs' is present, the revision history will only go back as far as any of the revision ID 
        /// strings in that array.
        /// </summary>
        IDictionary<string, object> GetRevisionHistory(Revision rev, IEnumerable<string> ancestorRevIds);

        /// <summary>
        /// Returns all the known revisions (or all current/conflicting revisions) of a document.
        /// </summary>
        /// <returns>An array of all available revisions of the document.</returns>
        /// <param name="docId">The document ID</param>
        /// <param name="onlyCurrent">If <c>true</c>, only leaf revisions (whether or not deleted) should be returned.</param>
        RevisionList GetAllDocumentRevisions(string docId, bool onlyCurrent);

        /// <summary>
        /// Returns IDs of local revisions of the same document, that have a lower generation number.
        /// Does not return revisions whose bodies have been compacted away, or deletion markers.
        /// If 'onlyAttachments' is true, only revisions with attachments will be returned.
        /// </summary>
        IEnumerable<string> GetPossibleAncestors(Revision rev, uint limit, bool onlyAttachments);

        /// <summary>
        /// Returns the most recent member of revIDs that appears in rev's ancestry.
        /// In other words: Look at the revID properties of rev, its parent, grandparent, etc.
        /// As soon as you find a revID that's in the revIDs array, stop and return that revID.
        /// If no match is found, return null.
        /// </summary>
        string FindCommonAncestor(Revision rev, IEnumerable<string> revIds);

        /// <summary>
        /// Looks for each given revision in the local database, and removes each one found from the list.
        /// On return, therefore, `revs` will contain only the revisions that don't exist locally.
        /// </summary>
        bool FindMissingRevisions(RevisionList revs);

        /// <summary>
        /// Returns the keys (unique IDs) of all attachments referred to by existing un-compacted
        /// Each revision key is a BlobKey (raw SHA-1 digest) derived from the "digest" property 
        /// of the attachment's metadata.
        /// </summary>
        HashSet<BlobKey> FindAllAttachmentKeys();

        /// <summary>
        /// Iterates over all documents in the database, according to the given query options.
        /// </summary>
        QueryEnumerator GetAllDocs(QueryOptions options);

        /// <summary>
        /// Returns all database changes with sequences greater than `lastSequence`.
        /// </summary>
        /// <returns>The since.</returns>
        /// <param name="lastSequence">The sequence number to start _after_</param>
        /// <param name="options">Options for ordering, document content, etc.</param>
        /// <param name="filter">If non-null, will be called on every revision, and those for which it returns <c>false</c>
        /// will be skipped.</param>
        RevisionList ChangesSince(Int64 lastSequence, ChangesOptions options, FilterDelegate filter);

        #endregion

        #region Insertion / Deletion

        /// <summary>
        /// On success, before returning the new SavedRevision, the implementation will also call the
        /// Delegate's DatabaseStorageChanged() method to give it more details about the change.
        /// </summary>
        /// <returns>The new revision, with its revID and sequence filled in, or null on error.</returns>
        /// <param name="docId">The document ID, or nil if an ID should be generated at random.</param>
        /// <param name="prevRevId">The parent revision ID, or nil if creating a new document.</param>
        /// <param name="properties">The new revision's properties. (Metadata other than "_attachments" ignored.)</param>
        /// <param name="deleting"><c>true</c> if this revision is a deletion</param>
        /// <param name="allowConflict"><c>true</c> if this operation is allowed to create a conflict; otherwise a 409
        /// status will be returned if the parent revision is not a leaf.</param>
        /// <param name="validationBlock">If non-null, this block will be called before the revision is added.
        /// It's given the parent revision, with its properties if available, and can reject
        /// the operation by returning an error status.</param>
        SavedRevision PutDocument(string docId, string prevRevId, IDictionary<string, object> properties,
                                  bool deleting, bool allowConflict, ValidateDelegate validationBlock);
            
        /// <summary>
        /// Inserts an already-existing revision (with its revID), plus its ancestry, into a document.
        /// This is called by the pull replicator to add the revisions received from the server.
        /// On success, the implementation will also call the
        /// delegate's DatabaseStorageChanged() method to give it more details about the change.
        /// </summary>
        /// <returns>Status code; 200 on success, otherwise an error.</returns>
        /// <param name="rev">The revision to insert. Its revID will be non-null.</param>
        /// <param name="history">The revIDs of the revision and its ancestors, in reverse chronological order.
        /// The first item will be equal to inRev.revID.</param>
        /// <param name="validationBlock">If non-null, this block will be called before the revision is added.
        /// It's given the parent revision, with its properties if available, and can reject
        /// the operation by returning an error status.</param>
        /// <param name="source">The URL of the remote database this was pulled from, or null if it's local.
        /// (This will be used to create the DatabaseChange object sent to the delegate.</param>
        Status ForceInsert(Revision rev, IEnumerable<string> history, ValidationDelegate validationBlock, Uri source);

        #endregion
    }
}

