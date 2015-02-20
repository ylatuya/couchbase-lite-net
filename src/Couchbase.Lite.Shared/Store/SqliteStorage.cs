using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Couchbase.Lite.Internal;
using Couchbase.Lite.Storage;
using Couchbase.Lite.Util;
using Sharpen;
using SQLitePCL;

namespace Couchbase.Lite.Store
{
    #region Delegates

    internal delegate Status StoreValidateDelegate(RevisionInternal newRevision, RevisionInternal prevRevision, string parentRevId);

    #endregion

    internal sealed class SqliteStorage : ICouchStore
    {

        #region Constants

        private const string DB_FILENAME = "db.sqlite3";
        private const int DOC_ID_CACHE_SIZE = 1000;
        private const double SQLITE_BUSY_TIMEOUT = 5.0;
        private const int TRANSACTION_MAX_RETRIES = 10;
        private const double TRANSACTION_RETRY_DELAY = 0.050;
        private const string LOCAL_CHECKPOINT_ID = "CBL_LocalCheckpoint";
        private const string TAG = "SqliteStorage";

        #endregion

        #region Private Members

        private static readonly ICollection<String> KnownSpecialKeys;
        private string _directory;
        private Manager _manager;
        private bool _readOnly;
        private LruCache<string, object> _docIds;
        private TaskFactory _scheduler;
        private int _lastTransactionCount;

        #endregion

        #region Properties

        public ISQLiteStorageEngine StorageEngine { get; private set; }

        #endregion

        #region Constructors

        static SqliteStorage()
        {
            Log.I(TAG, "Couchbase Lite using SQLite version {0} ({1})", raw.sqlite3_libversion(), raw.sqlite3_sourceid());
            Debug.Assert(raw.sqlite3_libversion_number() >= 3007000,
                "SQLite library is too old ({0}); needs to be at least 3.7", raw.sqlite3_libversion());
            Debug.Assert(raw.sqlite3_compileoption_used("SQLITE_ENABLE_FTS3") != 0
                || raw.sqlite3_compileoption_used("SQLITE_ENABLE_FTS4") != 0,
                "SQLite isn't built with full-text indexing (FTS3 or FTS4)");
            Debug.Assert(raw.sqlite3_compileoption_used("SQLITE_ENABLE_RTREE") != 0,
                @"SQLite isn't built with geo-indexing (R-tree)");

            KnownSpecialKeys = new List<String>();
            KnownSpecialKeys.Add("_id");
            KnownSpecialKeys.Add("_rev");
            KnownSpecialKeys.Add("_attachments");
            KnownSpecialKeys.Add("_deleted");
            KnownSpecialKeys.Add("_revisions");
            KnownSpecialKeys.Add("_revs_info");
            KnownSpecialKeys.Add("_conflicts");
            KnownSpecialKeys.Add("_deleted_conflicts");
            KnownSpecialKeys.Add("_local_seq");
            KnownSpecialKeys.Add("_removed");
        }

        public SqliteStorage()
        {
            _scheduler = new TaskFactory(new SingleThreadScheduler());
        }

        #endregion

        public Status LastDbStatus { get; private set; }
        public Status LastDbError { get; private set; }

        public bool Initialize(string statements)
        {
            if(RunStatements(statements)) {
                return true;
            }

            Log.W(TAG, @"Could not initialize schema of {0} -- May be an old/incompatible format. ", 
                _directory);
            Close();

            return false;
        }

        public int SchemaVersion() {
            return StorageEngine.GetVersion();
        }

        /*

        public Revision GetDocument(string docId, Int64 sequenceNumber)
        {
            throw new NotSupportedException();
        }*/

        #region Private Methods

        private static void ComputeFTSRank(sqlite3_context ctx, int val, sqlite3_value sqlVal)
        {
            throw new NotSupportedException();
        }

        private static IDictionary<string, object> MakeRevisionHistoryDict(IList<RevisionInternal> history)
        {
            if(history == null) {
                return null;
            }

            // Try to extract descending numeric prefixes:
            var suffixes = new List<string>();
            var start = -1;
            var lastRevNo = -1;

            foreach(var rev in history) {
                var revNo = ParseRevIDNumber(rev.GetRevId());
                var suffix = ParseRevIDSuffix(rev.GetRevId());
                if(revNo > 0 && suffix.Length > 0) {
                    if(start < 0) {
                        start = revNo;
                    }
                    else {
                        if(revNo != lastRevNo - 1) {
                            start = -1;
                            break;
                        }
                    }
                    lastRevNo = revNo;
                    suffixes.Add(suffix);
                } else {
                    start = -1;
                    break;
                }
            }

            var result = new Dictionary<String, Object>();
            if(start == -1) {
                // we failed to build sequence, just stuff all the revs in list
                suffixes = new List<string>();
                foreach(RevisionInternal rev_1 in history) {
                    suffixes.Add(rev_1.GetRevId());
                }
            } else {
                result["start"] = start;
            }

            result["ids"] = suffixes;
            return result;
        }

        // Splits a revision ID into its generation number and opaque suffix string
        private static int ParseRevIDNumber(string rev)
        {
            var result = -1;
            var dashPos = rev.IndexOf("-", StringComparison.InvariantCultureIgnoreCase);

            if(dashPos >= 0) {
                try {
                    var revIdStr = rev.Substring(0, dashPos);

                    // Should return -1 when the string has WC at the beginning or at the end.
                    if(revIdStr.Length > 0 &&
                        (char.IsWhiteSpace(revIdStr[0]) ||
                        char.IsWhiteSpace(revIdStr[revIdStr.Length - 1]))) {
                        return result;
                    }

                    result = Int32.Parse(revIdStr);
                } catch(FormatException) {

                }
            }
            // ignore, let it return -1
            return result;
        }

        // Splits a revision ID into its generation number and opaque suffix string
        private static string ParseRevIDSuffix(string rev)
        {
            var result = String.Empty;
            int dashPos = rev.IndexOf("-", StringComparison.InvariantCultureIgnoreCase);
            if(dashPos >= 0) {
                result = rev.Substring(dashPos + 1);
            }

            return result;
        }

        private static String JoinQuoted(IEnumerable<String> strings)
        {
            if(!strings.Any()) {
                return String.Empty;
            }

            var result = "'";
            var first = true;

            foreach(string str in strings) {
                if(first) {
                    first = false;
                } else {
                    result = result + "','";
                }

                result = result + str.Replace("'", "''");
            }

            result = result + "'";
            return result;
        }

        private static String JoinQuotedObjects(IEnumerable<Object> objects)
        {
            var strings = new List<String>();
            foreach(var obj in objects) {
                strings.AddItem(obj != null ? obj.ToString() : null);
            }

            return JoinQuoted(strings);
        }

        private static Status PurgeRevisionsTask(SqliteStorage enclosingDatabase, IDictionary<String, IList<String>> docsToRevs, IDictionary<String, Object> result)
        {
            foreach(string docID in docsToRevs.Keys) {
                long docNumericID = enclosingDatabase.GetDocNumericID(docID);
                if(docNumericID == -1) {
                    continue;
                }

                var revsPurged = new List<string>();
                var revIDs = docsToRevs[docID];
                if(revIDs == null) {
                    return new Status(StatusCode.BadRequest);
                } else {
                    if(revIDs.Count == 0) {
                        revsPurged = new List<string>();
                    } else {
                        if(revIDs.Contains("*")) {
                            try {
                                var args = new[] { Convert.ToString(docNumericID) };
                                enclosingDatabase.StorageEngine.ExecSQL("DELETE FROM revs WHERE doc_id=?", args);
                            } catch(SQLException e) {
                                Log.E(TAG, "Error deleting revisions", e);
                                return new Status(StatusCode.DbError);
                            }
                            revsPurged = new List<string>();
                            revsPurged.AddItem("*");
                        } else {
                            Cursor cursor = null;
                            try {
                                var args = new [] { Convert.ToString(docNumericID) };
                                const string queryString = "SELECT revid, sequence, parent FROM revs WHERE doc_id=? ORDER BY sequence DESC";
                                cursor = enclosingDatabase.StorageEngine.RawQuery(queryString, args);
                                if(!cursor.MoveToNext()) {
                                    Log.W(TAG, "No results for query: " + queryString);
                                    return new Status(StatusCode.BadRequest);
                                }
                                var seqsToPurge = new HashSet<long>();
                                var seqsToKeep = new HashSet<long>();
                                var revsToPurge = new HashSet<string>();
                                while(!cursor.IsAfterLast()) {
                                    string revID = cursor.GetString(0);
                                    long sequence = cursor.GetLong(1);
                                    long parent = cursor.GetLong(2);
                                    if(seqsToPurge.Contains(sequence) || revIDs.Contains(revID) && !seqsToKeep.Contains
                                        (sequence)) {
                                        seqsToPurge.AddItem(sequence);
                                        revsToPurge.AddItem(revID);
                                        if(parent > 0) {
                                            seqsToPurge.AddItem(parent);
                                        }
                                    } else {
                                        seqsToPurge.Remove(sequence);
                                        revsToPurge.Remove(revID);
                                        seqsToKeep.AddItem(parent);
                                    }
                                    cursor.MoveToNext();
                                }
                                seqsToPurge.RemoveAll(seqsToKeep);
                                Log.I(TAG, String.Format("Purging doc '{0}' revs ({1}); asked for ({2})", docID, revsToPurge, revIDs));
                                if(seqsToPurge.Count > 0) {
                                    string seqsToPurgeList = String.Join(",", seqsToPurge);
                                    string sql = string.Format("DELETE FROM revs WHERE sequence in ({0})", seqsToPurgeList);
                                    try {
                                        enclosingDatabase.StorageEngine.ExecSQL(sql);
                                    } catch(SQLException e) {
                                        Log.E(TAG, "Error deleting revisions via: " + sql, e);
                                        return new Status(StatusCode.DbError);
                                    }
                                }
                                Collections.AddAll(revsPurged, revsToPurge);
                            } catch(SQLException e) {
                                Log.E(TAG, "Error getting revisions", e);
                                return new Status(StatusCode.DbError);
                            } finally {
                                if(cursor != null) {
                                    cursor.Close();
                                }
                            }
                        }
                    }
                }
                result[docID] = revsPurged;
            }

            return new Status(StatusCode.Ok);
        }

        private IDictionary<string, object> DocumentPropertiesFromJSON(byte[] json, string docId, string revId,
            bool deleted, Int64 sequence, DocumentContentOptions options)
        {
            var rev = new RevisionInternal(docId, revId, deleted);
            rev.SetSequence(sequence);

            IDictionary<String, Object> extra = ExtraPropertiesForRevision(rev, options);
            if(json == null) {
                return extra;
            }

            IDictionary<String, Object> docProperties = null;
            try {
                docProperties = Manager.GetObjectMapper().ReadValue<IDictionary<string, object>>(json);
                docProperties.PutAll(extra);
            } catch (Exception e) {
                Log.E(TAG, "Error serializing properties to JSON", e);
            }
            return docProperties;
        }

        private bool RunStatements(string statements)
        {
            foreach(var statement in statements.Split(new[] { ';' }, StringSplitOptions.RemoveEmptyEntries)) {
                var unescapedStatement = statement.Replace('|', ';');
                try {
                    StorageEngine.ExecSQL(unescapedStatement);
                } catch(SQLException e) {
                    Log.E(TAG, "Error running statments", e);
                    return false;
                }
            }

            return true;
        }

        private long GetDocNumericID(string docId)
        {
            object cached = _docIds.Get(docId);
            if(cached != null) {
                return (long)cached;
            }

            var result = LongForQuery("SELECT doc_id FROM docs WHERE docid=?", docId);
            _docIds.Put(docId, result);
            return result;
        }

        /// <summary>Inserts the _id, _rev and _attachments properties into the JSON data and stores it in rev.
        ///     </summary>
        /// <remarks>
        /// Inserts the _id, _rev and _attachments properties into the JSON data and stores it in rev.
        /// Rev must already have its revID and sequence properties set.
        /// </remarks>
        private void ExpandStoredJSONIntoRevision(IEnumerable<Byte> json, RevisionInternal rev, DocumentContentOptions contentOptions)
        {
            var extra = ExtraPropertiesForRevision(rev, contentOptions);

            if(json != null && json.Any()) {
                rev.SetJson(AppendDictToJSON(json, extra));
            }
            else {
                rev.SetProperties(extra);
                if(json == null) {
                    rev.SetMissing(true);
                }
            }
        }

        /// <summary>Inserts the _id, _rev and _attachments properties into the JSON data and stores it in rev.
        ///     </summary>
        /// <remarks>
        /// Inserts the _id, _rev and _attachments properties into the JSON data and stores it in rev.
        /// Rev must already have its revID and sequence properties set.
        /// </remarks>
        private IDictionary<String, Object> ExtraPropertiesForRevision(RevisionInternal rev, DocumentContentOptions contentOptions)
        {
            var docId = rev.GetDocId();
            var revId = rev.GetRevId();

            // Get more optional stuff to put in the properties:
            //OPT: This probably ends up making redundant SQL queries if multiple options are enabled.
            var localSeq = -1L;
            if(contentOptions.HasFlag(DocumentContentOptions.IncludeLocalSeq)) {
                localSeq = rev.GetSequence();
            }

            IDictionary<string, object> revHistory = null;
            if(contentOptions.HasFlag(DocumentContentOptions.IncludeRevs)) {
                revHistory = GetRevisionHistoryDict(rev);
            }
            IList<object> revsInfo = null;
            if(contentOptions.HasFlag(DocumentContentOptions.IncludeRevsInfo)) {
                revsInfo = new List<object>();
                var revHistoryFull = GetRevisionHistory(rev);
                foreach(RevisionInternal historicalRev in revHistoryFull) {
                    var revHistoryItem = new Dictionary<string, object>();
                    var status = "available";
                    if (historicalRev.IsDeleted()) {
                        status = "deleted";
                    } else if (historicalRev.IsMissing()) {
                        status = "missing";
                    }

                    revHistoryItem["rev"] = historicalRev.GetRevId();
                    revHistoryItem["status"] = status;
                    revsInfo.Add(revHistoryItem);
                }
            }

            IList<string> conflicts = null;
            if(contentOptions.HasFlag(DocumentContentOptions.IncludeConflicts)) {
                var revs = GetAllRevisionsOfDocumentID(docId, true);
                if(revs.Count > 1) {
                    conflicts = new List<string>();
                    foreach(RevisionInternal savedRev in revs) {
                        if (!(savedRev.Equals(rev) || savedRev.IsDeleted())) {
                            conflicts.Add(savedRev.GetRevId());
                        }
                    }
                }
            }

            var result = new Dictionary<string, object>();
            result["_id"] = docId;
            result["_rev"] = revId;

            if(rev.IsDeleted()) {
                result["_deleted"] = true;
            }
            if(localSeq > -1) {
                result["_local_seq"] = localSeq;
            }
            if(revHistory != null) {
                result["_revisions"] = revHistory;
            }
            if(revsInfo != null) {
                result["_revs_info"] = revsInfo;
            }
            if(conflicts != null) {
                result["_conflicts"] = conflicts;
            }

            return result;
        }

        /// <summary>Splices the contents of an NSDictionary into JSON data (that already represents a dict), without parsing the JSON.</summary>
        private IEnumerable<Byte> AppendDictToJSON(IEnumerable<Byte> json, IDictionary<String, Object> dict)
        {
            if(dict.Count == 0) {
                return json;
            }

            IEnumerable<Byte> extraJSON;
            try {
                extraJSON = Manager.GetObjectMapper().WriteValueAsBytes(dict);
            } catch (Exception e) {
                Log.E(TAG, "Error convert extra JSON to bytes", e);
                return null;
            }

            var jsonArray = json.ToArray();
            int jsonLength = jsonArray.Length;
            int extraLength = extraJSON.Count();

            if(jsonLength == 2) {
                // Original JSON was empty
                return extraJSON;
            }

            var newJson = new byte[jsonLength + extraLength - 1];
            Array.Copy(jsonArray, 0, newJson, 0, jsonLength - 1);

            // Copy json w/o trailing '}'
            newJson[jsonLength - 1] = (byte)(',');

            // Add a ','
            Array.Copy(extraJSON.ToArray(), 1, newJson, jsonLength, extraLength - 1);

            return newJson;
        }

        private RevisionList GetAllRevisionsOfDocumentID (string id, bool onlyCurrent)
        {
            var docNumericId = GetDocNumericID(id);
            if (docNumericId < 0) {
                return null;
            } else {
                if (docNumericId == 0) {
                    return new RevisionList();
                }
                else {
                    return GetAllRevisionsOfDocumentID(id, docNumericId, onlyCurrent);
                }
            }
        }

        private RevisionList GetAllRevisionsOfDocumentID(string docId, long docNumericID, bool onlyCurrent)
        {
            var sql = onlyCurrent 
                ? "SELECT sequence, revid, deleted FROM revs WHERE doc_id=? AND current ORDER BY sequence DESC"
                : "SELECT sequence, revid, deleted FROM revs WHERE doc_id=? ORDER BY sequence DESC";
                
            var cursor = StorageEngine.RawQuery(sql, docNumericID);

            RevisionList result = null;
            try {
                result = new RevisionList();
                while(cursor.MoveToNext()) {
                    var rev = new RevisionInternal(docId, cursor.GetString(1), (cursor.GetInt(2) > 0));
                    rev.SetSequence(cursor.GetLong(0));
                    result.Add(rev);
                }
            } catch (SQLException e) {
                Log.E(TAG, "Error getting all revisions of document", e);
            } finally {
                if (cursor != null) {
                    cursor.Close();
                }
            }

            return result;
        }

        /// <summary>Returns the revision history as a _revisions dictionary, as returned by the REST API's ?revs=true option.
        ///     </summary>
        private IDictionary<String, Object> GetRevisionHistoryDict(RevisionInternal rev)
        {
            return MakeRevisionHistoryDict(GetRevisionHistory(rev));
        }

        private Int64 LongForQuery(string sqlQuery, params object[] args)
        {
            return ResultForQuery(sqlQuery, c => c.GetLong(0), args);
        }

        private int IntForQuery(string sqlQuery, params object[] args)
        {
            return ResultForQuery(sqlQuery, c => c.GetInt(0), args);
        }

        private string StringForQuery(string sqlQuery, params object[] args)
        {
            return ResultForQuery(sqlQuery, c => c.GetString(0), args);
        }

        private T ResultForQuery<T>(string sqlQuery, Func<Cursor, T> generator, params object[] args)
        {
            Cursor cursor = null;
            var result = default(T);
            try {
                cursor = StorageEngine.RawQuery(sqlQuery, args.ToArray());
                if (cursor.MoveToNext()) {
                    result = generator(cursor);
                }
            } catch(SQLException e) {
                Log.E(TAG, "Error executing query: {0} ({1})".Fmt(sqlQuery, args), e);
            } finally {
                if (cursor != null) {
                    cursor.Close();
                }
            }

            return result;
        }

        private void IterateResults(string sqlQuery, Action<Cursor> block, params object[] args)
        {
            IterateResults(sqlQuery, (c) =>
            {
                block(c);
                return true;
            }, args);

        }

        private void IterateResults(string sqlQuery, Func<Cursor, bool> block, params object[] args)
        {
            Cursor cursor = null;
            try {
                cursor = StorageEngine.RawQuery(sqlQuery, args);
                bool keepGoing = true;
                while(keepGoing && cursor.MoveToNext()) {
                    keepGoing = block(cursor);
                }
            } catch (SQLException e) {
                Log.E(TAG, "Error executing query: {0} ({1})".Fmt(sqlQuery, args), e);
            } finally {
                if (cursor != null) {
                    cursor.Close();
                }
            }

        }

        private bool SequenceHasAttachments(Int64 sequence)
        {
            Cursor cursor = null;
            try {
                cursor = StorageEngine.RawQuery("SELECT 1 FROM attachments WHERE sequence=? LIMIT 1", sequence);
                return cursor.MoveToNext();
            } catch (SQLException e) {
                Log.E(Database.Tag, "Error getting attachments for sequence", e);
                return false;
            } finally {
                if (cursor != null) {
                    cursor.Close();
                }
            }
        }

        private Int32 GetDeletedColumnIndex(QueryOptions options)
        {
            Debug.Assert(options != null);

            return options.IsIncludeDocs() ? 5 : 4;
        }

        /// <summary>Returns the rev ID of the 'winning' revision of this document, and whether it's deleted.</summary>
        /// <exception cref="Couchbase.Lite.CouchbaseLiteException"></exception>
        private String WinningRevIDOfDoc(Int64 docNumericId, out bool outIsDeleted, out bool outIsConflict, Boolean readOnly = false)
        {
            Cursor cursor = null;
            outIsDeleted = false;
            outIsConflict = false;
            var args = new [] { Convert.ToString(docNumericId) };
            String revId = null;
            const string sql = "SELECT revid, deleted FROM revs WHERE doc_id=? and current=1 " +
                "ORDER BY deleted asc, revid desc LIMIT 2";

            try {
                cursor = readOnly 
                    ? StorageEngine.RawQuery(sql, args)
                    : StorageEngine.IntransactionRawQuery(sql, args);
                cursor.MoveToNext();

                if(!cursor.IsAfterLast()) {
                    revId = cursor.GetString(0);
                    outIsDeleted = cursor.GetInt(1) > 0;

                    // The document is in conflict if there are two+ result rows that are not deletions.
                    var hasNextResult = cursor.MoveToNext();
                    if(hasNextResult) {
                        var isNextDeleted = cursor.GetInt(1) > 0;
                        outIsConflict = !outIsDeleted && hasNextResult && !isNextDeleted;
                    }
                }
            } catch (SQLException e) {
                Log.E(TAG, "Error getting winning revision", e);
                throw new CouchbaseLiteException("Error", e, new Status(StatusCode.InternalServerError));
            } finally {
                if (cursor != null) {
                    cursor.Close();
                }
            }

            return revId;
        }

        private Int64 GetSequenceOfDocument(Int64 docNumericId, String revId, Boolean onlyCurrent)
        {
            var result = -1L;
            Cursor cursor = null;
            try {
                var extraSql = (onlyCurrent ? "AND current=1" : string.Empty);
                var sql = string.Format("SELECT sequence FROM revs WHERE doc_id=? AND revid=? {0} LIMIT 1", extraSql);
                cursor = StorageEngine.IntransactionRawQuery(sql, docNumericId, revId);
                result = cursor.MoveToNext()
                    ? cursor.GetLong(0)
                    : 0;
            } catch(Exception e) {
                Log.E(TAG, "Error getting getSequenceOfDocument", e);
            } finally {
                if (cursor != null) {
                    cursor.Close();
                }
            }

            return result;
        }

        private Boolean ExistsDocumentWithIDAndRev(String docId, String revId)
        {
            return GetDocument(docId, revId, DocumentContentOptions.NoBody) != null;
        }

        private Int64 InsertDocumentID(String docId)
        {
            var rowId = -1L;
            try {
                ContentValues args = new ContentValues();
                args["docid"] = docId;
                rowId = StorageEngine.InsertWithOnConflict("docs", null, args, ConflictResolutionStrategy.Ignore);
            } catch (Exception e) {
                Log.E(TAG, "Error inserting document id", e);
            }

            if(rowId != -1) {
                _docIds.Put(docId, rowId);
            }
            return rowId;
        }

        private void BeginTransaction()
        {
            _lastTransactionCount = StorageEngine.BeginTransaction();
            Log.D(TAG, "Begin transaction (level %d)...", _lastTransactionCount);
        }

        private void EndTransaction()
        {
            var transactionCount = StorageEngine.EndTransaction();
            if(transactionCount > 0) {
                return;
            }

            bool committed = transactionCount == 0;
            Delegate.StorageExitedTransaction(committed);
            _lastTransactionCount = transactionCount;
        }

        private Int64 InsertRevision(RevisionInternal rev, long docNumericID, long parentSequence, bool current, bool hasAttachments, IEnumerable<byte> data)
        {
            var rowId = 0L;
            try {
                if (docNumericID == -1L) {
                    throw new CouchbaseLiteException(StatusCode.BadRequest);
                }

                var args = new ContentValues();
                args["doc_id"] = docNumericID;
                args.Put("revid", rev.GetRevId());
                if (parentSequence != 0) {
                    args["parent"] = parentSequence;
                }

                args["current"] = current;
                args["deleted"] = rev.IsDeleted();
                args["no_attachments"] = !hasAttachments;
                if (data != null) {
                    args["json"] = data.ToArray();
                }

                rowId = StorageEngine.Insert("revs", null, args);
                rev.SetSequence(rowId);
            } catch (Exception e) {
                Log.E(TAG, "Error inserting revision", e);
            }

            return rowId;
        }

        private RevisionInternal Winner(Int64 docNumericID, String oldWinningRevID, Boolean oldWinnerWasDeletion, RevisionInternal newRev)
        {
            if (oldWinningRevID == null) {
                return newRev;
            }

            var newRevID = newRev.GetRevId();
            if(!newRev.IsDeleted()) {
                if(oldWinnerWasDeletion || RevisionInternal.CBLCompareRevIDs(newRevID, oldWinningRevID) > 0) {
                    return newRev;
                }
            } else {
                // this is now the winning live revision
                if(oldWinnerWasDeletion) {
                    if(RevisionInternal.CBLCompareRevIDs(newRevID, oldWinningRevID) > 0) {
                        return newRev;
                    }
                } else {
                    // doc still deleted, but this beats previous deletion rev
                    // Doc was alive. How does this deletion affect the winning rev ID?
                    bool outIsDeleted;
                    bool outIsConflict;
                    var winningRevID = WinningRevIDOfDoc(docNumericID, out outIsDeleted, out outIsConflict);

                    if(!winningRevID.Equals(oldWinningRevID)) {
                        if(winningRevID.Equals(newRev.GetRevId())) {
                            return newRev;
                        } else {
                            var winningRev = new RevisionInternal(newRev.GetDocId(), winningRevID, false);
                            return winningRev;
                        }
                    }
                }
            }
            return null;
        }

        private Int64 GetOrInsertDocNumericID(String docId)
        {
            Int64 docNumericId = -1L;
            RunInTransaction(() =>
            {
                docNumericId = GetDocNumericID(docId);
                if (docNumericId == 0)
                {
                    docNumericId = InsertDocumentID(docId);
                }

                return new Status(StatusCode.Ok);
            });

            return docNumericId;
        }

        private Boolean IsValidDocumentId(string id)
        {
            // http://wiki.apache.org/couchdb/HTTP_Document_API#Documents
            if(String.IsNullOrEmpty (id)) {
                return false;
            }

            return id [0] != '_' || id.StartsWith("_design/", StringComparison.InvariantCultureIgnoreCase);
        }

        private IEnumerable<Byte> EncodeDocumentJSON(RevisionInternal rev)
        {
            var origProps = rev.GetProperties();
            if(origProps == null) {
                return null;
            }
            var specialKeysToLeave = new[] {
                "_removed",
                "_replication_id",
                "_replication_state",
                "_replication_state_time"
            };

            // Don't allow any "_"-prefixed keys. Known ones we'll ignore, unknown ones are an error.
            var properties = new Dictionary<String, Object>(origProps.Count);
            foreach(var key in origProps.Keys) {
                var shouldAdd = false;
                if(key.StartsWith("_", StringComparison.InvariantCultureIgnoreCase)) {
                    if(!KnownSpecialKeys.Contains(key)) {
                        Log.E(TAG, "Database: Invalid top-level key '" + key + "' in document to be inserted");
                        return null;
                    }
                    if(specialKeysToLeave.Contains(key)) {
                        shouldAdd = true;
                    }
                } else {
                    shouldAdd = true;
                }

                if(shouldAdd) {
                    properties.Put(key, origProps.Get(key));
                }
            }
            IEnumerable<byte> json = null;
            try {
                json = Manager.GetObjectMapper().WriteValueAsBytes(properties);
            } catch(Exception e) {
                Log.E(TAG, "Error serializing " + rev + " to JSON", e);
            }
            return json;
        }

        private void DeleteLocalDocument(string docID, string revID)
        {
            if(docID == null) {
                throw new CouchbaseLiteException(StatusCode.BadRequest);
            }

            if(revID == null) {
                // Didn't specify a revision to delete: 404 or a 409, depending
                if(GetLocalDocument(docID, null) != null) {
                    throw new CouchbaseLiteException(StatusCode.Conflict);
                } else {
                    throw new CouchbaseLiteException(StatusCode.NotFound);
                }
            }

            var whereArgs = new [] { docID, revID };
            try {
                int rowsDeleted = StorageEngine.Delete("localdocs", "docid=? AND revid=?", whereArgs);
                if(rowsDeleted == 0) {
                    if(GetLocalDocument(docID, null) != null) {
                        throw new CouchbaseLiteException(StatusCode.Conflict);
                    } else {
                        throw new CouchbaseLiteException(StatusCode.NotFound);
                    }
                }
            } catch (SQLException e) {
                throw new CouchbaseLiteException(e, StatusCode.InternalServerError);
            }
        }

        #endregion

        #region ICouchStore

        public int DocumentCount {
            get {
                const string sql = "SELECT COUNT(DISTINCT doc_id) FROM revs WHERE current=1 AND deleted=0";
                return IntForQuery(sql);
            }
        }

        public Int64 LastSequence {
            get {
                const string sql = "SELECT MAX(sequence) FROM revs";
                return Math.Max(0, LongForQuery(sql));
            }
        }

        public bool InTransaction
        {
            get {
                return StorageEngine.InTransaction;
            }
        }

        public IStoreDelegate Delegate { get; set; }

        public int MaxRevTreeDepth { get; set; }

        public bool AutoCompact { get; set; }

        public bool DatabaseExistsIn(string directory)
        {
            string fullPath = Path.Combine(directory, DB_FILENAME);
            return File.Exists(fullPath);
        }

        public bool Open(string directory, bool readOnly, Manager manager)
        {
            _directory = directory;
            _readOnly = readOnly;
            _manager = manager;
            _docIds = new LruCache<string, object>(DOC_ID_CACHE_SIZE);

            if(StorageEngine != null && StorageEngine.IsOpen) {
                StorageEngine.Close();
            }

            // Create the storage engine.
            StorageEngine = SQLiteStorageEngineFactory.CreateStorageEngine();

            // Try to open the storage engine and stop if we fail.
            if (StorageEngine == null || !StorageEngine.Open(directory))
            {
                var msg = "Unable to create a storage engine, fatal error";
                Log.E(TAG, msg);
                throw new CouchbaseLiteException(msg);
            }

            // Stuff we need to initialize every time the sqliteDb opens:
            if (!Initialize("PRAGMA foreign_keys = ON; PRAGMA journal_mode=WAL;"))
            {
                Log.E(TAG, "Error turning on foreign keys");
                return false;
            }

            // Check the user_version number we last stored in the sqliteDb:
            var dbVersion = StorageEngine.GetVersion();

            // Incompatible version changes increment the hundreds' place:
            if(dbVersion >= 100) {
                Log.E(TAG, "Database: Database version (" + dbVersion + ") is newer than I know how to work with");
                StorageEngine.Close();
                return false;
            }

            if(dbVersion < 17) {
                const string schema = @"CREATE TABLE docs (
                doc_id INTEGER PRIMARY KEY,
                docid TEXT UNIQUE NOT NULL);
            CREATE INDEX docs_docid ON docs(docid);
            
            CREATE TABLE revs (
                sequence INTEGER PRIMARY KEY AUTOINCREMENT,
                doc_id INTEGER NOT NULL REFERENCES docs(doc_id) ON DELETE CASCADE,
                revid TEXT NOT NULL COLLATE REVID,
                parent INTEGER REFERENCES revs(sequence) ON DELETE SET NULL,
                current BOOLEAN,
                deleted BOOLEAN DEFAULT 0,
                json BLOB,
                no_attachments BOOLEAN,
                UNIQUE (doc_id, revid));
            CREATE INDEX revs_parent ON revs(parent);
            CREATE INDEX revs_by_docid_revid ON revs(doc_id, revid desc, current, deleted);
            CREATE INDEX revs_current ON revs(doc_id, current desc, deleted, revid desc);
            
            CREATE TABLE localdocs (
                docid TEXT UNIQUE NOT NULL,
                revid TEXT NOT NULL COLLATE REVID,
                json BLOB);
            CREATE INDEX localdocs_by_docid ON localdocs(docid);
            
            CREATE TABLE views (
                view_id INTEGER PRIMARY KEY,
                name TEXT UNIQUE NOT NULL,
                version TEXT,
                lastsequence INTEGER DEFAULT 0,
                total_docs INTEGER DEFAULT -1);
            CREATE INDEX views_by_name ON views(name);
            
            CREATE TABLE info (
                key TEXT PRIMARY KEY,
                value TEXT);
            
            CREATE VIRTUAL TABLE fulltext USING fts4(content);
            CREATE VIRTUAL TABLE bboxes USING rtree(rowid, x0, x1, y0, y1);
            PRAGMA user_version = 17"; //FIXME.JHB: unicodesn unavailable (iOS uses it)

                if(!Initialize(schema)) {
                    StorageEngine.Close();
                    return false;
                }
            }

            return true;
        }

        public void Close()
        {
            if(StorageEngine != null) {
                StorageEngine.Close();
                StorageEngine = null;
            }
        }

        public Status RunInTransaction(Func<Status> block)
        {
            return _scheduler.StartNew(() =>
            {
                var status = new Status();
                int retries = 0;
                do {
                    try {
                        BeginTransaction();
                        status = block();

                    } catch(Exception e) {
                        Log.E(TAG, "RunInTransaction threw exception", e);
                        status.SetCode(StatusCode.Exception);
                        return status;
                    } finally {
                        if(!status.IsError) {
                            StorageEngine.SetTransactionSuccessful();
                        }

                        EndTransaction();
                    }

                    if(status.GetCode() == StatusCode.DbBusy) {
                        if(_lastTransactionCount > 1) {
                            break;
                        }

                        if(++retries > TRANSACTION_MAX_RETRIES) {
                            Log.W(TAG, "Db busy, too many retries, giving up");
                            break;
                        }

                        Log.D(TAG, "Db busy, retrying transaction (#{0})...", retries);
                        Thread.Sleep(TimeSpan.FromSeconds(TRANSACTION_RETRY_DELAY));
                    }
                } while(status.GetCode() == StatusCode.DbBusy);

                return status;
            }).Result;
        }

        public RevisionInternal GetDocument(string docId, string revId, DocumentContentOptions options)
        {
            RevisionInternal result = null;
            Cursor cursor = null;
            try
            {
                Int64 docNumericId = GetDocNumericID(docId);
                if(docNumericId <= 0) {
                    throw new CouchbaseLiteException(StatusCode.NotFound);
                }

                StringBuilder sb = new StringBuilder("SELECT revid, deleted, sequence, no_attachments");
                if (!options.HasFlag(DocumentContentOptions.NoBody)) {
                    sb.Append(", json");
                }
                if (revId != null) {
                    sb.Append(" FROM revs WHERE revs.doc_id=? AND revid=? AND json notnull LIMIT 1");

                } else {
                    sb.Append(@" FROM revs WHERE revs.doc_id=? and current=1 and deleted=0 
                            ORDER BY revid DESC LIMIT 1");
                }

                cursor = StorageEngine.IntransactionRawQuery(sb.ToString(), docId, revId);
                if(!cursor.MoveToNext()) {
                    StatusCode err = revId == null ? StatusCode.NotFound : StatusCode.Deleted;
                    throw new CouchbaseLiteException(err);
                } else {
                    if (revId == null) {
                        revId = cursor.GetString(0);
                    }

                    var deleted = cursor.GetInt(1) > 0;
                    result = new RevisionInternal(docId, revId, deleted);
                    result.SetSequence(cursor.GetLong(2));
                    if (options != DocumentContentOptions.NoBody)
                    {
                        byte[] json = null;
                        if (!options.HasFlag(DocumentContentOptions.NoBody))
                        {
                            json = cursor.GetBlob(4);
                        }
                        if (cursor.GetInt(3) > 0)
                        {
                            // no_attachments == true
                            options |= DocumentContentOptions.NoAttachments;
                        }
                        ExpandStoredJSONIntoRevision(json, result, options);
                    }
                }
            } catch (Exception e) {
                Log.E(TAG, "Error getting document with id and rev", e);
            } finally {
                if(cursor != null) {
                    cursor.Close();
                }
            }
            return result;
        }

        public IList<RevisionInternal> GetRevisionHistory(RevisionInternal rev)
        {
            string docId = rev.GetDocId();
            string revId = rev.GetRevId();

            Debug.Assert(((docId != null) && (revId != null)));

            long docNumericId = GetDocNumericID(docId);
            if (docNumericId < 0) {
                return null;
            } else if (docNumericId == 0){
                return new List<RevisionInternal>();
            }
                
            IList<RevisionInternal> result = new List<RevisionInternal>();;
            const string sql = "SELECT sequence, parent, revid, deleted, json isnull FROM revs WHERE doc_id=? ORDER BY sequence DESC";
            long lastSequence = 0;
            IterateResults(sql, (cursor) =>
            {
                var sequence = cursor.GetLong(0);
                bool matches = false;
                if(lastSequence == 0) {
                    matches = revId.Equals(cursor.GetString(2));
                } else {
                    matches = (sequence == lastSequence);
                }

                if(matches){
                    revId = cursor.GetString(2);
                    var deleted = (cursor.GetInt(3) > 0);
                    var missing = (cursor.GetInt(4) > 0);

                    var aRev = new RevisionInternal(docId, revId, deleted);
                    aRev.SetSequence(sequence);
                    aRev.SetMissing(missing);
                    result.Add(aRev);

                    lastSequence = cursor.GetLong(1);
                    if(lastSequence == 0) {
                        return false;
                    }
                }

                return true;
            });

            return result;
        }

        public IDictionary<string, object> GetRevisionHistory(RevisionInternal rev, IList<string>ancestorRevIDs)
        {
            var history = GetRevisionHistory(rev); // This is in reverse order, newest ... oldest
            if(ancestorRevIDs != null && ancestorRevIDs.Any()) {
                for(var i = 0; i < history.Count; i++) {
                    if(ancestorRevIDs.Contains(history[i].GetRevId())) {
                        var newHistory = new List<RevisionInternal>();
                        for(var index = 0; index < i + 1; index++) {
                            newHistory.Add(history[index]);
                        }
                        history = newHistory;
                        break;
                    }
                }
            }

            return MakeRevisionHistoryDict(history);
        }
            
        public Status SetInfo(string key, string info)
        {
            try {
                StorageEngine.ExecSQL("INSERT OR REPLACE INTO info (key, value) VALUES (?, ?)", key, info);
            } catch(CouchbaseLiteException e) {
                return e.GetCBLStatus();
            }

            return new Status(StatusCode.Ok);
        }

        public string GetInfo(string key)
        {
            const string sql = "SELECT value FROM info WHERE key=?";
            return StringForQuery(sql, key);
        }

        public void Compact()
        {
            // Can't delete any rows because that would lose revision tree history.
            // But we can remove the JSON of non-current revisions, which is most of the space.
            try {
                Log.V(TAG, "Deleting JSON of old revisions...");
                //PruneRevsToMaxDepth(0);
                //Log.V(Tag, "Deleting JSON of old revisions...");

                var args = new ContentValues();
                args["json"] = null;
                args["no_attachments"] = 1;
                StorageEngine.Update("revs", args, "current=0", null);
            } catch (SQLException e) {
                Log.E(TAG, "Error compacting", e);
                throw new CouchbaseLiteException(StatusCode.InternalServerError);
            }

            /*Log.V(TAG, "Deleting old attachments...");
            var result = GarbageCollectAttachments();
            if (!result.IsSuccessful)
            {
                throw new CouchbaseLiteException(result.GetCode());
            }*/

            try {
                Log.V(TAG, "Flushing SQLite WAL...");
                StorageEngine.ExecSQL("PRAGMA wal_checkpoint(RESTART)");
                Log.V(TAG, "Vacuuming SQLite sqliteDb...");
                StorageEngine.ExecSQL("VACUUM");
            }
            catch (SQLException e) {
                Log.E(TAG, "Error compacting sqliteDb", e);
                throw new CouchbaseLiteException(StatusCode.InternalServerError);
            }
        }

        public RevisionInternal LoadRevisionBody(RevisionInternal rev, DocumentContentOptions options)
        {
            if(rev.GetBody() != null && options == DocumentContentOptions.None && rev.GetSequence() != 0) {
                return rev;
            }

            if((rev.GetDocId() == null) || (rev.GetRevId() == null)) {
                Log.E(Database.Tag, "Error loading revision body");
                throw new CouchbaseLiteException(StatusCode.PreconditionFailed);
            }

            Int64 docNumericId = GetDocNumericID(rev.GetDocId());
            if(docNumericId <= 0) {
                throw new CouchbaseLiteException(StatusCode.NotFound);
            }

            const string sql = "SELECT sequence, json FROM revs WHERE revid=? AND doc_id=? LIMIT 1";
            var result = new Status(StatusCode.NotFound);
            IterateResults(sql, (cursor) =>
            {
                result.SetCode(StatusCode.Ok);
                rev.SetSequence(cursor.GetLong(0));
                ExpandStoredJSONIntoRevision(cursor.GetBlob(1), rev, options);
            }, rev.GetRevId(), docNumericId);

            return rev;
        }

        public Int64 GetRevisionSequence(RevisionInternal rev)
        {
            Int64 docNumericId = GetDocNumericID(rev.GetDocId());
            if(docNumericId <= 0) {
                return 0;
            }

            const string sql = "SELECT sequence FROM revs WHERE doc_id=? AND revid=? LIMIT 1";
            return LongForQuery(sql, docNumericId, rev.GetRevId());
        }

        public RevisionInternal GetParentRevision(RevisionInternal rev)
        {
            // First get the parent's sequence:
            var seq = rev.GetSequence();
            if(seq > 0) {
                seq = LongForQuery("SELECT parent FROM revs WHERE sequence=?", seq);
            } else {
                var docNumericID = GetDocNumericID(rev.GetDocId());
                if(docNumericID <= 0) {
                    return null;
                }

                seq = LongForQuery("SELECT parent FROM revs WHERE doc_id=? and revid=?", docNumericID, rev.GetRevId());
            }

            if(seq == 0) {
                return null;
            }

            // Now get its revID and deletion status:
            RevisionInternal result = null;
            const string queryString = "SELECT revid, deleted FROM revs WHERE sequence=?";

            Tuple<string, bool> info = ResultForQuery(queryString, c => Tuple.Create(c.GetString(0), c.GetInt(1) > 0), seq);
            result = new RevisionInternal(rev.GetDocId(), info.Item1, info.Item2);
            result.SetSequence(seq);
            return result;
        }

        public RevisionList GetAllDocumentRevisions(string docId, bool onlyCurrent)
        {
            var docNumericId = GetDocNumericID(docId);
            if(docNumericId < 0) {
                return null;
            } else {
                if(docNumericId == 0) {
                    return new RevisionList();
                }
                else {
                    return GetAllRevisionsOfDocumentID(docId, docNumericId, onlyCurrent);
                }
            }
        }

        public IEnumerable<string> GetPossibleAncestors(RevisionInternal rev, int limit, bool onlyAttachments)
        {
            var matchingRevs = new List<String>();
            var generation = rev.GetGeneration();
            if(generation <= 1) {
                return null;
            }

            var docNumericID = GetDocNumericID(rev.GetDocId());
            if(docNumericID <= 0) {
                return null;
            }

            var sqlLimit = limit > 0 ? limit : -1;

            // SQL uses -1, not 0, to denote 'no limit'
            const string sql = @"SELECT revid, sequence FROM revs WHERE doc_id=? and revid < ? and deleted=0 and json not null 
                                ORDER BY sequence DESC LIMIT ?";

            IterateResults(sql, (cursor) =>
            {
                if (onlyAttachments && !SequenceHasAttachments(cursor.GetLong(1))) {
                    return;
                }

                matchingRevs.AddItem(cursor.GetString(0));
            }, new object[] { docNumericID, string.Format("{0}-", generation), sqlLimit });

            return matchingRevs;
        }

        public string FindCommonAncestor(RevisionInternal rev, IEnumerable<string> revIds)
        {
            if(revIds == null || !revIds.Any()) {
                return String.Empty;
            }

            Int64 docNumericId = GetDocNumericID(rev.GetDocId());
            if(docNumericId <= 0) {
                return null;
            }

            var sql = String.Format(@"SELECT revid FROM revs 
                                               WHERE doc_id=? and revid in ({0}) and revid <= ? 
                                               ORDER BY revid DESC LIMIT 1", JoinQuoted(revIds));

            return StringForQuery(sql);
        }

        public int FindMissingRevisions(RevisionList revs)
        {
            var numRevisionsRemoved = 0;
            if(revs.Count == 0) {
                return numRevisionsRemoved;
            }

            var quotedDocIds = JoinQuoted(revs.GetAllDocIds());
            var quotedRevIds = JoinQuoted(revs.GetAllRevIds());
            var sql = String.Format(@"SELECT docid, revid FROM revs, docs WHERE docid IN ({0})
                                               AND revid in ({1}) AND revs.doc_id == docs.doc_id", 
                                               quotedDocIds, quotedRevIds);

            IterateResults(sql, (cursor) =>
            {
                var rev = revs.RevWithDocIdAndRevId(cursor.GetString(0), cursor.GetString(1));
                if (rev != null) {
                    revs.Remove(rev);
                    numRevisionsRemoved += 1;
                }
            });

            return numRevisionsRemoved;
        }

        public HashSet<BlobKey> FindAllAttachmentKeys()
        {
            var retVal = new HashSet<BlobKey>();
            const string sql = "SELECT json FROM revs WHERE no_attachments != 1";
            IterateResults(sql, (cursor) =>
            {
                var blob = cursor.GetBlob(0);
                var json = Manager.GetObjectMapper().ReadValue<IDictionary<string, object>>(blob);
                if(!json.ContainsKey("digest")) {
                    return;
                }

                try {
                    BlobKey key = new BlobKey((string)json["digest"]);
                    retVal.Add(key);
                } catch(ArgumentException) {}
            });

            return retVal;
        }

        public IDictionary<string, object> GetAllDocs(QueryOptions options)
        {
            var result = new Dictionary<String, Object>();
            var rows = new List<QueryRow>();
            if(options == null) {
                options = new QueryOptions();
            }

            var includeDeletedDocs = (options.GetAllDocsMode() == AllDocsMode.IncludeDeleted);
            var updateSeq = 0L;
            if(options.IsUpdateSeq()) {
                updateSeq = LastSequence;
            }

            // TODO: needs to be atomic with the following SELECT
            var sql = new StringBuilder("SELECT revs.doc_id, docid, revid, sequence");
            if(options.IsIncludeDocs()) {
                sql.Append(", json");
            }
            if(includeDeletedDocs) {
                sql.Append(", deleted");
            }
            sql.Append(" FROM revs, docs WHERE");

            if(options.GetKeys() != null) {
                if(!options.GetKeys().Any()) {
                    return result;
                }

                var commaSeperatedIds = JoinQuotedObjects(options.GetKeys());
                sql.AppendFormat(" revs.doc_id IN (SELECT doc_id FROM docs WHERE docid IN ({0})) AND", commaSeperatedIds);
            }

            sql.Append(" docs.doc_id = revs.doc_id AND current=1");
            if(!includeDeletedDocs) {
                sql.Append(" AND deleted=0");
            }

            var args = new List<String>();
            var minKey = options.GetStartKey();
            var maxKey = options.GetEndKey();
            var inclusiveMin = true;
            var inclusiveMax = options.IsInclusiveEnd();

            if(options.IsDescending()) {
                minKey = maxKey;
                maxKey = options.GetStartKey();
                inclusiveMin = inclusiveMax;
                inclusiveMax = true;
            }
            if(minKey != null) {
                Debug.Assert((minKey is String));
                sql.Append((inclusiveMin ? " AND docid >= ?" : " AND docid > ?"));
                args.Add((string)minKey);
            }
            if(maxKey != null) {
                Debug.Assert((maxKey is string));
                sql.Append((inclusiveMax ? " AND docid <= ?" : " AND docid < ?"));
                args.Add((string)maxKey);
            }

            sql.AppendFormat(
                " ORDER BY docid {0}, {1} revid DESC LIMIT ? OFFSET ?", 
                    options.IsDescending() ? "DESC" : "ASC", 
                    includeDeletedDocs ? "deleted ASC," : String.Empty
            );

            args.Add(options.GetLimit().ToString());
            args.Add(options.GetSkip().ToString());

            Cursor cursor = null;
            var docs = new Dictionary<String, QueryRow>();
            try {
                cursor = StorageEngine.RawQuery(sql.ToString(), args.ToArray());
                var keepGoing = cursor.MoveToNext();
                while(keepGoing) {
                    var docNumericID = cursor.GetLong(0);
                    var includeDocs = options.IsIncludeDocs();
                    var docId = cursor.GetString(1);
                    var revId = cursor.GetString(2);
                    var sequenceNumber = cursor.GetLong(3);
                    var deleted = includeDeletedDocs && cursor.GetInt(GetDeletedColumnIndex(options)) > 0;
                    IDictionary<String, Object> docContents = null;
                    byte[] json = null;
                    if(includeDocs) {
                        json = cursor.GetBlob(4);
                        docContents = DocumentPropertiesFromJSON(json, docId, revId, deleted, sequenceNumber, options.GetContentOptions());
                    }

                    // Iterate over following rows with the same doc_id -- these are conflicts.
                    // Skip them, but collect their revIDs if the 'conflicts' option is set:
                    var conflicts = new List<string>();
                    while((keepGoing = cursor.MoveToNext()) && cursor.GetLong(0) == docNumericID) {
                        if (options.GetAllDocsMode() == AllDocsMode.ShowConflicts || options.GetAllDocsMode() == AllDocsMode.OnlyConflicts) {
                            if (conflicts.Count == 0) {
                                conflicts.Add(revId);
                            }

                            conflicts.AddItem(cursor.GetString(2));
                        }
                    }
                    if(options.GetAllDocsMode() == AllDocsMode.OnlyConflicts && conflicts.IsEmpty()) {
                        continue;
                    }

                    var value = new Dictionary<string, object>();
                    value["rev"] = revId;
                    value["_conflicts"] = conflicts;
                    if(includeDeletedDocs) {
                        value["deleted"] = deleted;
                    }
                    var change = new QueryRow(docId, sequenceNumber, docId, value, docContents);
                    //change.Database = this;

                    if(options.GetKeys() != null) {
                        docs[docId] = change;
                    } else {
                        rows.Add(change);
                    }
                }

                if(options.GetKeys() != null) {
                    foreach(var docIdObject in options.GetKeys())
                    {
                        var docId = docIdObject as string;
                        if(docId == null) {
                            continue;
                        }

                        var change = docs.Get(docId);
                        if(change == null) {
                            var value = new Dictionary<string, object>();
                            var docNumericID = GetDocNumericID(docId);
                            if(docNumericID > 0) {
                                bool outIsDeleted;
                                bool outIsConflict;
                                var revId = WinningRevIDOfDoc(docNumericID, out outIsDeleted, out outIsConflict, true);

                                if(revId != null) {
                                    value["rev"] = revId;
                                    value["deleted"] = outIsDeleted;
                                }
                            }
                            change = new QueryRow((value != null ? docId : null), 0, docId, value, null);
                            //change.Database = this;
                        }
                        rows.Add(change);
                    }
                }
            } catch (SQLException e) {
                Log.E(TAG, "Error getting all docs", e);
                throw new CouchbaseLiteException("Error getting all docs", e, new Status(StatusCode.InternalServerError));
            } finally {
                if (cursor != null) {
                    cursor.Close ();
                }
            }

            result["rows"] = rows;
            result["total_rows"] = rows.Count;
            result.Put("offset", options.GetSkip());
            if (updateSeq != 0) {
                result["update_seq"] = updateSeq;
            }

            return result;
        }

        public RevisionList ChangesSince(Int64 lastSequence, ChangesOptions options, RevisionFilterDelegate filter)
        {
            // http://wiki.apache.org/couchdb/HTTP_database_API#Changes
            if(options == null) {
                options = new ChangesOptions();
            }

            var includeDocs = options.IsIncludeDocs() || (filter != null);
            var additionalSelectColumns = string.Empty;
            if(includeDocs) {
                additionalSelectColumns = ", json";
            }

           var sql = "SELECT sequence, revs.doc_id, docid, revid, deleted" + additionalSelectColumns
                + " FROM revs, docs " + "WHERE sequence > ? AND current=1 " + "AND revs.doc_id = docs.doc_id "
                + "ORDER BY revs.doc_id, revid DESC";
            long lastDocId = 0L;
            RevisionList changes = new RevisionList();
            IterateResults(sql, (cursor) =>
            {
                if(!options.IsIncludeConflicts()) {
                    // Only count the first rev for a given doc (the rest will be losing conflicts):
                    var docNumericId = cursor.GetLong(1);
                    if(docNumericId == lastDocId) {
                        cursor.MoveToNext();
                        return;
                    }

                    lastDocId = docNumericId;
                }

                var sequence = cursor.GetLong(0);
                var rev = new RevisionInternal(cursor.GetString(2), cursor.GetString(3), (cursor.GetInt(4) > 0));
                rev.SetSequence(sequence);
                if(includeDocs) {
                    ExpandStoredJSONIntoRevision(cursor.GetBlob(5), rev, options.GetContentOptions());
                }
                    
                if (filter == null || filter(rev)) {
                    changes.AddItem(rev);
                }
            }, lastSequence);


            if(options.IsSortBySequence()) {
                changes.SortBySequence();
            }

            changes.Limit(options.GetLimit());
            return changes;
        }

        public RevisionInternal PutRevision(string docId, string prevRevId, IDictionary<string, object> properties,
            bool deleting, bool allowConflict, StoreValidateDelegate validationBlock)
        {
            IEnumerable<byte> json = null;
            if(properties != null) {
                json = Manager.GetObjectMapper().WriteValueAsBytes(properties);
                if(json == null) {
                    throw new CouchbaseLiteException(StatusCode.BadJson);
                }
            } else {
                json = Encoding.UTF8.GetBytes("{}");
            }

            RevisionInternal newRev = null;
            RevisionInternal winningRev = null;
            bool inConflict = false;

            var transactionStatus = RunInTransaction(() =>
            {
                //Remember this block may be called multiple times if I have to retry the transaction;
                newRev = null;
                winningRev = null;
                inConflict = false;

                // PART I: In which are performed lookups and validations prior to the insert...

                // Get the doc's numeric ID (doc_id) and its current winning revision:
                Int64 docNumericId = docId != null ? GetDocNumericID(docId) : 0;
                bool oldWinnerWasDeletion = false;
                bool wasInConflict = false;
                string oldWinningRevId = null;
                if(docNumericId > 0) {
                    // Look up which rev is the winner, before this insertion
                    //OPT: This rev ID could be cached in the 'docs' row
                    oldWinningRevId = WinningRevIDOfDoc(docNumericId, out oldWinnerWasDeletion, out wasInConflict);
                }

                Int64 parentSequence = 0;
                if(prevRevId != null) {
                    // Replacing: make sure given prevRevID is current & find its sequence number:
                    if(docNumericId <= 0) {
                        throw new CouchbaseLiteException(StatusCode.NotFound);
                    }

                    parentSequence = GetSequenceOfDocument(docNumericId, prevRevId, !allowConflict);
                    if(parentSequence == 0) {
                        if(!allowConflict && ExistsDocumentWithIDAndRev(docId, null)) {
                            throw new CouchbaseLiteException(StatusCode.Conflict);
                        } else {
                            throw new CouchbaseLiteException(StatusCode.NotFound);
                        }
                    }
                } else {
                    // Inserting first revision.
                    if(deleting && docId != null) {
                        // Didn't specify a revision to delete: NotFound or a Conflict, depending
                        if(ExistsDocumentWithIDAndRev(docId, null)) {
                            throw new CouchbaseLiteException(StatusCode.Conflict);
                        } else {
                            throw new CouchbaseLiteException(StatusCode.NotFound);
                        }
                    }

                    if(docId != null) {
                        // Inserting first revision, with docID given (PUT):
                        if(docNumericId <= 0) {
                            // Doc ID doesn't exist at all; create it:
                            docNumericId = InsertDocumentID(docId);
                            if(docNumericId <= 0) {
                                throw new CouchbaseLiteException(StatusCode.DbError);
                            }
                        } else {
                            // Doc ID exists; check whether current winning revision is deleted:
                            if(oldWinnerWasDeletion) {
                                prevRevId = oldWinningRevId;
                                parentSequence = GetSequenceOfDocument(docNumericId, prevRevId, false);
                            } else if(oldWinningRevId != null) {
                                throw new CouchbaseLiteException(StatusCode.Conflict);
                            }
                        }
                    } else {
                        // Inserting first revision, with no docID given (POST): generate a unique docID:
                        docId = Misc.CreateGUID();
                        docNumericId = InsertDocumentID(docId);
                        if(docNumericId <= 0) {
                            throw new CouchbaseLiteException(StatusCode.DbError);
                        }
                    }
                }

                // There may be a conflict if (a) the document was already in conflict, or
                // (b) a conflict is created by adding a non-deletion child of a non-winning rev.
                inConflict = wasInConflict || (!deleting && prevRevId.Equals(oldWinningRevId));

                // PART II: In which we prepare for insertion...

                // Bump the revID and update the JSON:
                string newRevId = Delegate.GenerateRevID(json, deleting, prevRevId);
                if(newRevId == null) {
                    // invalid previous revID (no numeric prefix)
                    throw new CouchbaseLiteException(StatusCode.BadId);
                }

                Debug.Assert(docId != null);
                newRev = new RevisionInternal(docId, newRevId, deleting);
                if(properties != null) {
                    properties["_id"] = docId;
                    properties["_rev"] = newRevId;
                    newRev.SetProperties(properties);
                }

                // Validate:
                if(validationBlock != null) {
                    // Fetch the previous revision and validate the new one against it:
                    RevisionInternal prevRev = null;
                    if(prevRevId != null) {
                        prevRev = new RevisionInternal(docId, prevRevId, false);
                    }

                    var status = validationBlock(newRev, prevRev, prevRevId);
                    if(status.IsError) {
                        throw new CouchbaseLiteException(status.GetCode());
                    }
                }

                // Don't store a SQL null in the 'json' column -- I reserve it to mean that the revision data
                // is missing due to compaction or replication.
                // Instead, store an empty zero-length blob.
                if(json == null) {
                    json = new byte[0];
                }

                // PART III: In which the actual insertion finally takes place:

                Int64 sequence = InsertRevision(newRev, docNumericId, parentSequence, true, 
                    properties.ContainsKey("_attachments"), json);

                if (sequence <= 0) {
                    return new Status(StatusCode.DbError);
                }

                // Make replaced rev non-current:
                try {
                    var args = new ContentValues();
                    args["current"] = 0;
                    StorageEngine.Update("revs", args, "sequence=?", new[] { parentSequence.ToString() });
                } catch (SQLException e) {
                    Log.E(Database.Tag, "Error setting parent rev non-current", e);
                    throw new CouchbaseLiteException(StatusCode.InternalServerError);
                }

                winningRev = Winner(docNumericId, oldWinningRevId, oldWinnerWasDeletion, newRev);
                return new Status(deleting ? StatusCode.Ok : StatusCode.Created);
            });

            if(transactionStatus.IsError) {
                return null;
            }

            // EPILOGUE: A change notification is sent...
            if(Delegate != null) {
                Delegate.DatabaseStorageChanged(new DocumentChange(newRev, winningRev, inConflict, null));
            }

            return newRev;
        }

        public void ForceInsert(RevisionInternal rev, IList<string> revHistory, StoreValidateDelegate validationBlock, Uri source)
        {
            var inConflict = false;
            var docId = rev.GetDocId();
            var revId = rev.GetRevId();

            if(!IsValidDocumentId(docId) || (revId == null)) {
                throw new CouchbaseLiteException(StatusCode.BadRequest);
            }

            int historyCount = 0;
            if(revHistory != null) {
                historyCount = revHistory.Count;
            }

            if(historyCount == 0) {
                revHistory = new List<string>();
                revHistory.Add(revId);
                historyCount = 1;
            } else {
                if(!revHistory[0].Equals(rev.GetRevId())) {
                    throw new CouchbaseLiteException(StatusCode.BadRequest);
                }
            }

            RevisionInternal winningRev = null;
            var transactionStatus = RunInTransaction(() =>
            {
                try {
                    // First look up all locally-known revisions of this document:
                    long docNumericID = GetOrInsertDocNumericID(docId);
                    RevisionList localRevs = GetAllRevisionsOfDocumentID(docId, docNumericID, false);
                    if(localRevs == null){
                        throw new CouchbaseLiteException(StatusCode.InternalServerError);
                    }

                    bool oldWinnerWasDeletion = false;
                    string oldWinningRevID = WinningRevIDOfDoc(docNumericID, out oldWinnerWasDeletion, out inConflict);

                    if(validationBlock != null) {
                        RevisionInternal oldRev = null;
                        for(int i = 1; i < historyCount; i++) {
                            oldRev = localRevs.RevWithDocIdAndRevId(docId, revHistory[i]);
                            if(oldRev != null) {
                                break;
                            }
                        }

                        string parentId = historyCount > 1 ? revHistory[1] : null;
                        var status = validationBlock(rev, oldRev, parentId);
                        if(status.IsError) {
                            throw new CouchbaseLiteException(status.GetCode());
                        }
                    }

                    // Walk through the remote history in chronological order, matching each revision ID to
                    // a local revision. When the list diverges, start creating blank local revisions to fill
                    // in the local history:
                    long sequence = 0;
                    long localParentSequence = 0;

                    for(int i = revHistory.Count - 1; i >= 0; --i) {
                        revId = revHistory[i];
                        RevisionInternal localRev = localRevs.RevWithDocIdAndRevId(docId, revId);

                        if(localRev != null) {
                            // This revision is known locally. Remember its sequence as the parent of the next one:
                            sequence = localRev.GetSequence();
                            Debug.Assert((sequence > 0));
                            localParentSequence = sequence;
                        } else {
                            // This revision isn't known, so add it:
                            RevisionInternal newRev;
                            IEnumerable<Byte> data = null;
                            bool current = false; 
                            if (i == 0) {
                                // Hey, this is the leaf revision we're inserting:
                                newRev = rev;
                                if (!rev.IsDeleted()) {
                                    data = EncodeDocumentJSON(rev);
                                    if (data == null) {
                                        throw new CouchbaseLiteException(StatusCode.BadJson);
                                    }
                                }
                                current = true;
                            } else {
                                // It's an intermediate parent, so insert a stub:
                                newRev = new RevisionInternal(docId, revId, false);
                            }

                            // Insert it:
                            sequence = InsertRevision(newRev, docNumericID, sequence, current, newRev.GetProperties().ContainsKey("_attachments"), data);
                            if (sequence <= 0) {
                                throw new CouchbaseLiteException(StatusCode.InternalServerError);
                            }
                        }
                    }

                    if(localParentSequence == sequence) {
                        return new Status(StatusCode.Ok); // No-op: No new revisions were inserted.
                    }

                    // Mark the latest local rev as no longer current:
                    if(localParentSequence > 0) {
                        ContentValues args = new ContentValues();
                        args["current"] = 0;
                        string[] whereArgs = new string[] { Convert.ToString(localParentSequence) };

                        try {
                            var numRowsChanged = StorageEngine.Update("revs", args, "sequence=?", whereArgs);
                            if(numRowsChanged == 0) {
                                inConflict = true;
                            }
                        } catch(Exception) {
                            throw new CouchbaseLiteException(StatusCode.InternalServerError);
                        }
                    }

                    winningRev = Winner(docNumericID, oldWinningRevID, oldWinnerWasDeletion, rev);
                    return new Status(StatusCode.Created);
                }
                catch (SQLException)
                {
                    throw new CouchbaseLiteException(StatusCode.InternalServerError);
                }
            });

            if(Delegate != null && transactionStatus.IsSuccessful) {
                Delegate.DatabaseStorageChanged(new DocumentChange(rev, winningRev, inConflict, null));
            }
        }

        public IDictionary<string, object> PurgeRevisions(IDictionary<string, IList<string>> docsToRev)
        {
            var result = new Dictionary<String, Object>();
            RunInTransaction(() => PurgeRevisionsTask(this, docsToRev, result));
            // no such document, skip it
            // Delete all revisions if magic "*" revision ID is given:
            // Iterate over all the revisions of the doc, in reverse sequence order.
            // Keep track of all the sequences to delete, i.e. the given revs and ancestors,
            // but not any non-given leaf revs or their ancestors.
            // Purge it and maybe its parent:
            // Keep it and its parent:
            // Now delete the sequences to be purged.
            return result;
        }

        public IViewStorage GetViewStorage(string name, bool create)
        {
            throw new NotImplementedException();
        }

        public IEnumerable<string> GetAllViews()
        {
            var retVal = new List<string>();
            IterateResults("SELECT name FROM views", (cursor) =>
            {
                retVal.Add(cursor.GetString(0));
            });

            return retVal;
        }

        public RevisionInternal GetLocalDocument(string docId, string revId)
        {
            // docID already should contain "_local/" prefix
            RevisionInternal result = null;
            const string sql = "SELECT revid, json FROM localdocs WHERE docid=?";
            IterateResults(sql, (cursor) =>
            {
                var gotRevID = cursor.GetString(0);
                if (revId != null && (!revId.Equals(gotRevID))) {
                    return false;
                }

                var json = cursor.GetBlob(1);
                IDictionary<string, object> properties = null;
                try {
                    properties = Manager.GetObjectMapper().ReadValue<IDictionary<String, Object>>(json);
                    properties["_id"] = docId;
                    properties["_rev"] = gotRevID;

                    result = new RevisionInternal(docId, gotRevID, false);
                    result.SetProperties(properties);
                } catch (Exception e) {
                    Log.W(TAG, "Error parsing local doc JSON", e);
                }

                return false;
            }, docId);

            return result;
        }

        public RevisionInternal PutLocalRevision(RevisionInternal revision, string prevRevID, bool obeyMVCC)
        {
            var docID = revision.GetDocId();
            if(!docID.StartsWith("_local/", StringComparison.InvariantCultureIgnoreCase)) {
                throw new CouchbaseLiteException(StatusCode.BadRequest);
            }

            if(!revision.IsDeleted()) {
                // PUT:
                string newRevID;
                var json = EncodeDocumentJSON(revision);

                if(prevRevID != null) {
                    var generation = RevisionInternal.GenerationFromRevID(prevRevID);
                    if(generation == 0) {
                        throw new CouchbaseLiteException(StatusCode.BadRequest);
                    }
                    newRevID = String.Format("{0}-local", ++generation);

                    var values = new ContentValues();
                    values["revid"] = newRevID;
                    values["json"] = json;

                    var whereArgs = new [] { docID, prevRevID };
                    try {
                        var rowsUpdated = StorageEngine.Update("localdocs", values, "docid=? AND revid=?", whereArgs);
                        if(rowsUpdated == 0) {
                            throw new CouchbaseLiteException(StatusCode.Conflict);
                        }
                    } catch(SQLException e) {
                        throw new CouchbaseLiteException(e, StatusCode.InternalServerError);
                    }
                } else {
                    newRevID = "1-local";

                    var values = new ContentValues();
                    values["docid"] = docID;
                    values["revid"] = newRevID;
                    values["json"] = json;

                    try {
                        StorageEngine.InsertWithOnConflict("localdocs", null, values, ConflictResolutionStrategy.Ignore);
                    } catch(SQLException e) {
                        throw new CouchbaseLiteException(e, StatusCode.InternalServerError);
                    }
                }

                return revision.CopyWithDocID(docID, newRevID);
            } else {
                // DELETE:
                DeleteLocalDocument(docID, prevRevID);
                return revision;
            }
        }
            
        #endregion
    }
}

