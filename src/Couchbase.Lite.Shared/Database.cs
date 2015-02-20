//
// Database.cs
//
// Author:
//     Zachary Gramana  <zack@xamarin.com>
//
// Copyright (c) 2014 Xamarin Inc
// Copyright (c) 2014 .NET Foundation
//
// Permission is hereby granted, free of charge, to any person obtaining
// a copy of this software and associated documentation files (the
// "Software"), to deal in the Software without restriction, including
// without limitation the rights to use, copy, modify, merge, publish,
// distribute, sublicense, and/or sell copies of the Software, and to
// permit persons to whom the Software is furnished to do so, subject to
// the following conditions:
// 
// The above copyright notice and this permission notice shall be
// included in all copies or substantial portions of the Software.
// 
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
// EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
// MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
// NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE
// LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
// OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION
// WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
//
//
// Copyright (c) 2014 Couchbase, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
// except in compliance with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the
// License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
// either express or implied. See the License for the specific language governing permissions
// and limitations under the License.
//

using System;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Newtonsoft.Json.Linq;
using Couchbase.Lite.Internal;
using Couchbase.Lite.Replicator;
using Couchbase.Lite.Storage;
using Couchbase.Lite.Store;
using Couchbase.Lite.Util;
using Sharpen;
using System.Collections.Concurrent;
using System.Collections;
using System.Collections.ObjectModel;
using System.Net;
using System.Threading;

namespace Couchbase.Lite 
{

    /// <summary>
    /// A Couchbase Lite Database.
    /// </summary>
    public sealed class Database : IStoreDelegate
    {
    #region Constructors

        /// <summary>
        /// Initializes a new instance of the <see cref="Couchbase.Lite.Database"/> class.
        /// </summary>
        /// <param name="path">Path.</param>
        /// <param name="manager">Manager.</param>
        internal Database(String path, Manager manager, bool readOnly = false)
        {
            Debug.Assert(System.IO.Path.IsPathRooted(path));

            //path must be absolute
            Path = path;
            Name = FileDirUtils.GetDatabaseNameFromPath(path);
            _readOnly = readOnly;
            Manager = manager;
            DocumentCache = new LruCache<string, Document>(MaxDocCacheSize);
            UnsavedRevisionDocumentCache = new Dictionary<string, WeakReference>();
 
            // FIXME: Not portable to WinRT/WP8.
            ActiveReplicators = new List<Replication>();
            AllReplicators = new List<Replication> ();

            _changesToNotify = new List<DocumentChange>();

            Scheduler = new TaskFactory(new SingleTaskThreadpoolScheduler());

            StartTime = DateTime.UtcNow.ToMillisecondsSinceEpoch ();

            MaxRevTreeDepth = DefaultMaxRevs;
        }

    #endregion

    #region Static Members

        //Properties

        /// <summary>
        /// Gets or sets an object that can compile source code into <see cref="FilterDelegate"/>.
        /// </summary>
        /// <value>The filter compiler object.</value>
        public static CompileFilterDelegate FilterCompiler { get; set; }

        // "_local/*" is not a valid document ID. Local docs have their own API and shouldn't get here.
        internal static String GenerateDocumentId()
        {
            return Misc.CreateGUID();
        }

        static readonly ICollection<String> KnownSpecialKeys;

        static Database()
        {
            // Length that constitutes a 'big' attachment
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

    #endregion
    
    #region Instance Members
        //Properties

        private TaskFactory Scheduler { get; set; }
        private bool _readOnly;

        public CookieContainer PersistentCookieStore
        {
            get
            {
                if (_persistentCookieStore == null)
                {
                    _persistentCookieStore = new CookieStore(System.IO.Path.GetDirectoryName(Path));
                }
                return _persistentCookieStore;
            }
        }

        internal ICouchStore Store { get; private set; }

        /// <summary>
        /// Gets the <see cref="Couchbase.Lite.Database"/> name.
        /// </summary>
        /// <value>The database name.</value>
        public String Name { get; internal set; }

        /// <summary>
        /// Gets the <see cref="Couchbase.Lite.Manager"> that owns this <see cref="Couchbase.Lite.Database"/>.
        /// </summary>
        /// <value>The manager object.</value>
        public Manager Manager { get; private set; }

        /// <summary>
        /// Gets the number of <see cref="Couchbase.Lite.Document"> in the <see cref="Couchbase.Lite.Database"/>.
        /// </summary>
        /// <value>The document count.</value>
        /// TODO: Convert this to a standard method call.
        public Int32 DocumentCount 
        {
            get  {
                return Store.DocumentCount;
            }
        }
            
        /// <summary>
        /// Gets the latest sequence number used by the <see cref="Couchbase.Lite.Database" />.  Every new <see cref="Couchbase.Lite.Revision" /> is assigned a new sequence 
        /// number, so this property increases monotonically as changes are made to the <see cref="Couchbase.Lite.Database" />. This can be used to 
        /// check whether the <see cref="Couchbase.Lite.Database" /> has changed between two points in time.
        /// </summary>
        /// <value>The last sequence number.</value>
        public Int64 LastSequenceNumber 
        {
            get {
                return Store.LastSequence;
            }
        }

        /// <summary>
        /// Gets all the running <see cref="Couchbase.Lite.Replication" />s 
        /// for this <see cref="Couchbase.Lite.Database" />.  
        /// This includes all continuous <see cref="Couchbase.Lite.Replication" />s and 
        /// any non-continuous <see cref="Couchbase.Lite.Replication" />s that has been started 
        /// and are still running.
        /// </summary>
        /// <value>All replications.</value>
        public IEnumerable<Replication> AllReplications { get { return AllReplicators; } }

        //Methods

        /// <summary>
        /// Compacts the <see cref="Couchbase.Lite.Database" /> file by purging non-current 
        /// <see cref="Couchbase.Lite.Revision" />s and deleting unused <see cref="Couchbase.Lite.Attachment" />s.
        /// </summary>
        /// <exception cref="Couchbase.Lite.CouchbaseLiteException">thrown if an issue occurs while 
        /// compacting the <see cref="Couchbase.Lite.Database" /></exception>
        public void Compact()
        {
            Store.Compact();
            GarbageCollectAttachments();
        }

        /// <summary>
        /// Deletes the <see cref="Couchbase.Lite.Database" />.
        /// </summary>
        /// <exception cref="Couchbase.Lite.CouchbaseLiteException">
        /// Thrown if an issue occurs while deleting the <see cref="Couchbase.Lite.Database" /></exception>
        public void Delete()
        {
            if(_isOpen && !Close()) {
                throw new CouchbaseLiteException("The database was open, and could not be closed", StatusCode.InternalServerError);
            }

            Manager.ForgetDatabase(this);
            if(!Exists()) {
                return;
            }

            var file = new FilePath(Path);
            var fileJournal = new FilePath(AttachmentStorePath + "-journal");

            var deleteStatus = file.Delete();
            
            if(fileJournal.Exists()) {
                deleteStatus &= fileJournal.Delete();
            }

            //recursively delete attachments path
            var attachmentsFile = new FilePath(AttachmentStorePath);
            var deleteAttachmentStatus = FileDirUtils.DeleteRecursive(attachmentsFile);

            //recursively delete path where attachments stored( see getAttachmentStorePath())
            var lastDotPosition = Path.LastIndexOf('.');
            if(lastDotPosition > 0) {
                var attachmentsFileUpFolder = new FilePath(Path.Substring(0, lastDotPosition));
                FileDirUtils.DeleteRecursive(attachmentsFileUpFolder);
            }

            if(!deleteStatus) {
                Log.V(Tag, String.Format("Error deleting the SQLite database file at {0}", file.GetAbsolutePath()));
            }

            if(!deleteStatus) {
                throw new CouchbaseLiteException("Was not able to delete the database file", StatusCode.InternalServerError);
            }

            if(!deleteAttachmentStatus) {
                throw new CouchbaseLiteException("Was not able to delete the attachments files", StatusCode.InternalServerError);
            }
        }

        /// <summary>
        /// Gets or creates the <see cref="Couchbase.Lite.Document" /> with the given id.
        /// </summary>
        /// <returns>The <see cref="Couchbase.Lite.Document" />.</returns>
        /// <param name="id">The id of the Document to get or create.</param>
        public Document GetDocument(String id) 
        { 
            if (String.IsNullOrWhiteSpace (id)) {
                return null;
            }

            var unsavedDoc = UnsavedRevisionDocumentCache.Get(id);
            var doc = unsavedDoc != null 
                ? (Document)unsavedDoc.Target 
                : DocumentCache.Get(id);

            if (doc == null)
            {
                doc = new Document(this, id);
                DocumentCache[id] = doc;
                UnsavedRevisionDocumentCache[id] = new WeakReference(doc);
            }

            return doc;
        }

        /// <summary>
        /// Gets the <see cref="Couchbase.Lite.Document" /> with the given id, or null if it does not exist.
        /// </summary>
        /// <returns>The <see cref="Couchbase.Lite.Document" /> with the given id, or null if it does not exist.</returns>
        /// <param name="id">The id of the Document to get.</param>
        public Document GetExistingDocument(String id) 
        { 
            if (String.IsNullOrWhiteSpace (id)) {
                return null;
            }
            var revisionInternal = GetDocumentWithIDAndRev(id, null, DocumentContentOptions.None);
            return revisionInternal == null ? null : GetDocument (id);
        }

        /// <summary>
        /// Creates a <see cref="Couchbase.Lite.Document" /> with a unique id.
        /// </summary>
        /// <returns>A document with a unique id.</returns>
        public Document CreateDocument()
        { 
            return GetDocument(Misc.CreateGUID());
        }

        /// <summary>
        /// Gets the local document with the given id, or null if it does not exist.
        /// </summary>
        /// <returns>The existing local document.</returns>
        /// <param name="id">Identifier.</param>
        public IDictionary<String, Object> GetExistingLocalDocument(String id) 
        {
            var revInt = Store.GetLocalDocument(MakeLocalDocumentId(id), null);
            if(revInt == null) {
                return null;
            }

            return revInt.GetProperties();
        }

        /// <summary>
        /// Sets the contents of the local <see cref="Couchbase.Lite.Document" /> with the given id.  If <param name="properties"/> is null, the 
        /// <see cref="Couchbase.Lite.Document" /> is deleted.
        /// </summary>
        /// <param name="id">The id of the local document whos contents to set.</param>
        /// <param name="properties">The contents to set for the local document.</param>
        /// <exception cref="Couchbase.Lite.CouchbaseLiteException">Thrown if an issue occurs 
        /// while setting the contents of the local document.</exception>
        public void PutLocalDocument(String id, IDictionary<String, Object> properties) 
        { 
            // TODO: the iOS implementation wraps this in a transaction, this should do the same.
            id = MakeLocalDocumentId(id);
            RevisionInternal rev = new RevisionInternal(id, null, properties == null);
            if(properties != null) {
                rev.SetProperties(properties);
            }

            Store.PutLocalRevision(rev, null, false);
        }

        /// <summary>
        /// Deletes the local <see cref="Couchbase.Lite.Document" /> with the given id.
        /// </summary>
        /// <returns><c>true</c>, if local <see cref="Couchbase.Lite.Document" /> was deleted, <c>false</c> otherwise.</returns>
        /// <param name="id">Identifier.</param>
        /// <exception cref="Couchbase.Lite.CouchbaseLiteException">Thrown if there is an issue occurs while deleting the local document.</exception>
        public Boolean DeleteLocalDocument(String id) 
        {
            PutLocalDocument(id, null);
            return true;
        } 

        /// <summary>
        /// Creates a <see cref="Couchbase.Lite.Query" /> that matches all <see cref="Couchbase.Lite.Document" />s in the <see cref="Couchbase.Lite.Database" />.
        /// </summary>
        /// <returns>Returns a <see cref="Couchbase.Lite.Query" /> that matches all <see cref="Couchbase.Lite.Document" />s in the <see cref="Couchbase.Lite.Database" />s.</returns>
        public Query CreateAllDocumentsQuery() 
        {
            return new Query(this, (View)null);
        }

        internal View MakeAnonymousView()
        {
            for (var i = 0; true; ++i)
            {
                var name = String.Format("anon{0}", i);
                var existing = GetExistingView(name);
                if (existing == null)
                {
                    // this name has not been used yet, so let's use it
                    return GetView(name);
                }
            }
        }

        internal View RegisterView(View view)
        {
            if(view == null) {
                return null;
            }

            if(_views == null) {
                _views = new Dictionary<string, View>();
            }

            _views[view.Name] = view;
            return view;
        }

        /// <summary>
        /// Gets or creates the <see cref="Couchbase.Lite.View" /> with the given name.  
        /// New <see cref="Couchbase.Lite.View" />s won't be added to the <see cref="Couchbase.Lite.Database" /> 
        /// until a map function is assigned.
        /// </summary>
        /// <returns>The <see cref="Couchbase.Lite.View" /> with the given name.</returns>
        /// <param name="name">The name of the <see cref="Couchbase.Lite.View" /> to get or create.</param>
        public View GetView(String name) 
        {
            View view = null;

            if (_views != null)
            {
                view = _views.Get(name);
            }

            if (view != null)
            {
                return view;
            }

            return RegisterView(new View(this, name));
        }

        /// <summary>
        /// Gets the <see cref="Couchbase.Lite.View" /> with the given name, or null if it does not exist.
        /// </summary>
        /// <returns>The <see cref="Couchbase.Lite.View" /> with the given name, or null if it does not exist.</returns>
        /// <param name="name">The name of the View to get.</param>
        public View GetExistingView(String name) 
        {
            View view = null;
            if (_views != null)
            {
                _views.TryGetValue(name, out view);
            }
            if (view != null)
            {
                return view;
            }
            view = new View(this, name);

            return view.Id == 0 ? null : RegisterView(view);
        }

        /// <exception cref="Couchbase.Lite.CouchbaseLiteException"></exception>
        internal IEnumerable<QueryRow> QueryViewNamed(String viewName, QueryOptions options, IList<Int64> outLastSequence)
        {
            Log.D(Tag, "Starting QueryViewNamed");
            var before = Runtime.CurrentTimeMillis();
            var lastSequence = 0L;
            IEnumerable<QueryRow> rows;

            if (!String.IsNullOrEmpty (viewName)) {
                var view = GetView (viewName);
                if (view == null)
                {
                    throw new CouchbaseLiteException (StatusCode.NotFound);
                }

                lastSequence = view.LastSequenceIndexed;
                if (options.GetStale () == IndexUpdateMode.Before || lastSequence <= 0) {
                    Log.D(Tag, "Updating index on view '{0}' before generating query results.", view.Name);
                    view.UpdateIndex ();
                    lastSequence = view.LastSequenceIndexed;
                } else {
                    if (options.GetStale () == IndexUpdateMode.After 
                        && lastSequence < this.LastSequenceNumber)
                    {
                        Log.D(Tag, "Deferring index update on view '{0}'.", view.Name);
                        RunAsync((db)=>
                        {
                            try
                            {
                                Log.D(Tag, "Updating index on view '{0}'", view.Name);
                                view.UpdateIndex();
                            }
                            catch (CouchbaseLiteException e)
                            {
                                Log.E(Tag, "Error updating view index on background thread", e);
                            }
                        });
                    }
                }
                rows = view.QueryWithOptions (options);
            } else {
                // nil view means query _all_docs
                // note: this is a little kludgy, but we have to pull out the "rows" field from the
                // result dictionary because that's what we want.  should be refactored, but
                // it's a little tricky, so postponing.
                Log.D(Tag, "Returning an all docs query.");
                var allDocsResult = GetAllDocs (options);
                rows = (IList<QueryRow>)allDocsResult.Get ("rows");
                lastSequence = this.LastSequenceNumber;
            }
            outLastSequence.AddItem(lastSequence);

            var delta = Runtime.CurrentTimeMillis() - before;
            Log.D(Tag, String.Format("Query view {0} completed in {1} milliseconds", viewName, delta));

            return rows;
        }

        internal void SetLastSequence(string lastSequence, string checkpointID)
        {
            this.Store.SetInfo("checkpoint/" + checkpointID, lastSequence);
        }

        /// <summary>
        /// Gets the <see cref="ValidateDelegate" /> for the given name, or null if it does not exist.
        /// </summary>
        /// <returns>the <see cref="ValidateDelegate" /> for the given name, or null if it does not exist.</returns>
        /// <param name="name">The name of the validation delegate to get.</param>
        public ValidateDelegate GetValidation(String name) 
        {
            ValidateDelegate result = null;
            if (_validations != null)
            {
                result = _validations.Get(name);
            }
            return result;
        }

        /// <summary>
        /// Sets the validation delegate for the given name. If delegate is null, 
        /// the validation with the given name is deleted. Before any change 
        /// to the <see cref="Couchbase.Lite.Database"/> is committed, including incoming changes from a pull 
        /// <see cref="Couchbase.Lite.Replication"/>, all of its validation delegates are called and given 
        /// a chance to reject it.
        /// </summary>
        /// <param name="name">The name of the validation delegate to set.</param>
        /// <param name="validationDelegate">The validation delegate to set.</param>
        public void SetValidation(String name, ValidateDelegate validationDelegate)
        {
            if (_validations == null)
                _validations = new Dictionary<string, ValidateDelegate>();

            if (validationDelegate != null)
                _validations[name] = validationDelegate;
            else
                _validations.Remove(name);
        }

        /// <summary>
        /// Returns the <see cref="ValidateDelegate" /> for the given name, or null if it does not exist.
        /// </summary>
        /// <returns>The <see cref="ValidateDelegate" /> for the given name, or null if it does not exist.</returns>
        /// <param name="name">The name of the validation delegate to get.</param>
        public FilterDelegate GetFilter(String name) 
        { 
            FilterDelegate result = null;
            if (Filters != null)
            {
                result = Filters.Get(name);
            }
            if (result == null)
            {
                var filterCompiler = FilterCompiler;
                if (filterCompiler == null)
                {
                    return null;
                }

                var outLanguageList = new List<string>();
                var sourceCode = GetDesignDocFunction(name, "filters", outLanguageList);

                if (sourceCode == null)
                {
                    return null;
                }

                var language = outLanguageList[0];

                var filter = filterCompiler(sourceCode, language);
                if (filter == null)
                {
                    Log.W(Tag, string.Format("Filter {0} failed to compile", name));
                    return null;
                }

                SetFilter(name, filter);
                return filter;
            }
            return result;
        }

        /// <summary>
        /// Sets the <see cref="ValidateDelegate" /> for the given name. If delegate is null, the filter 
        /// with the given name is deleted. Before a <see cref="Couchbase.Lite.Revision" /> is replicated via a 
        /// push <see cref="Couchbase.Lite.Replication" />, its filter delegate is called and 
        /// given a chance to exclude it from the <see cref="Couchbase.Lite.Replication" />.
        /// </summary>
        /// <param name="name">The name of the filter delegate to set.</param>
        /// <param name="filterDelegate">The filter delegate to set.</param>
        public void SetFilter(String name, FilterDelegate filterDelegate) 
        { 
            if (Filters == null)
            {
                Filters = new Dictionary<String, FilterDelegate>();
            }
            if (filterDelegate != null)
            {
                Filters[name] = filterDelegate;
            }
            else
            {
                Collections.Remove(Filters, name);
            }
        }

        /// <summary>
        /// Runs the <see cref="Couchbase.Lite.RunAsyncDelegate"/> asynchronously.
        /// </summary>
        /// <returns>The async task.</returns>
        /// <param name="runAsyncDelegate">The delegate to run asynchronously.</param>
        public Task RunAsync(RunAsyncDelegate runAsyncDelegate) 
        {
            return Manager.RunAsync(runAsyncDelegate, this);
        }

        /// <summary>
        /// Runs the delegate within a transaction. If the delegate returns false, 
        /// the transaction is rolled back.
        /// </summary>
        /// <returns>True if the transaction was committed, otherwise false.</returns>
        /// <param name="transactionDelegate">The delegate to run within a transaction.</param>
        public Boolean RunInTransaction(RunInTransactionDelegate transactionDelegate)
        {
            Status status = Store.RunInTransaction(() =>
            {
                return transactionDelegate() ? new Status(StatusCode.Ok) : new Status(StatusCode.DbError);
            });

            return status.IsSuccessful;
        }

            
        /// <summary>
        /// Creates a new <see cref="Couchbase.Lite.Replication"/> that will push to the target <see cref="Couchbase.Lite.Database"/> at the given url.
        /// </summary>
        /// <returns>A new <see cref="Couchbase.Lite.Replication"/> that will push to the target <see cref="Couchbase.Lite.Database"/> at the given url.</returns>
        /// <param name="url">The url of the target Database.</param>
        public Replication CreatePushReplication(Uri url)
        {
            var scheduler = new SingleTaskThreadpoolScheduler(); //TaskScheduler.FromCurrentSynchronizationContext();
            return new Pusher(this, url, false, new TaskFactory(scheduler));
        }

        /// <summary>
        /// Creates a new <see cref="Couchbase.Lite.Replication"/> that will pull from the source <see cref="Couchbase.Lite.Database"/> at the given url.
        /// </summary>
        /// <returns>A new <see cref="Couchbase.Lite.Replication"/> that will pull from the source Database at the given url.</returns>
        /// <param name="url">The url of the source Database.</param>
        public Replication CreatePullReplication(Uri url)
        {
            var scheduler = new SingleTaskThreadpoolScheduler();
            return new Puller(this, url, false, new TaskFactory(scheduler));
        }

        public override string ToString()
        {
            return GetType().FullName + "[" + Path + "]";
        }

        /// <summary>
        /// Event handler delegate that will be called whenever a <see cref="Couchbase.Lite.Document"/> within the <see cref="Couchbase.Lite.Database"/> changes.
        /// </summary>
        public event EventHandler<DatabaseChangeEventArgs> Changed;

    #endregion
       
    #region Constants
        internal const String Tag = "Database";

        internal const String TagSql = "CBLSQL";
       
        internal const Int32 BigAttachmentLength = 16384;

        const Int32 MaxDocCacheSize = 50;

        const Int32 DefaultMaxRevs = Int32.MaxValue;

        internal readonly String Schema = @"
CREATE TABLE docs ( 
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
  json BLOB); 
CREATE INDEX revs_by_id ON revs(revid, doc_id); 
CREATE INDEX revs_current ON revs(doc_id, current); 
CREATE INDEX revs_parent ON revs(parent); 
CREATE TABLE localdocs ( 
  docid TEXT UNIQUE NOT NULL, 
  revid TEXT NOT NULL COLLATE REVID, 
  json BLOB); 
CREATE INDEX localdocs_by_docid ON localdocs(docid); 
CREATE TABLE views ( 
  view_id INTEGER PRIMARY KEY, 
  name TEXT UNIQUE NOT NULL,
  version TEXT, 
  lastsequence INTEGER DEFAULT 0); 
CREATE INDEX views_by_name ON views(name); 
CREATE TABLE maps ( 
  view_id INTEGER NOT NULL REFERENCES views(view_id) ON DELETE CASCADE, 
  sequence INTEGER NOT NULL REFERENCES revs(sequence) ON DELETE CASCADE, 
  key TEXT NOT NULL COLLATE JSON, 
  value TEXT); 
CREATE INDEX maps_keys on maps(view_id, key COLLATE JSON); 
CREATE TABLE attachments ( 
  sequence INTEGER NOT NULL REFERENCES revs(sequence) ON DELETE CASCADE, 
  filename TEXT NOT NULL, 
  key BLOB NOT NULL, 
  type TEXT, 
  length INTEGER NOT NULL, 
  revpos INTEGER DEFAULT 0); 
CREATE INDEX attachments_by_sequence on attachments(sequence, filename); 
CREATE TABLE replicators ( 
  remote TEXT NOT NULL, 
  push BOOLEAN, 
  last_sequence TEXT, 
  UNIQUE (remote, push)); 
PRAGMA user_version = 3;";

    #endregion

    #region Non-Public Instance Members

        /// <summary>
        /// Each database can have an associated PersistentCookieStore,
        /// where the persistent cookie store uses the database to store
        /// its cookies.
        /// </summary>
        /// <remarks>
        /// Each database can have an associated PersistentCookieStore,
        /// where the persistent cookie store uses the database to store
        /// its cookies.
        /// There are two reasons this has been made an instance variable
        /// of the Database, rather than of the Replication:
        /// - The PersistentCookieStore needs to span multiple replications.
        /// For example, if there is a "push" and a "pull" replication for
        /// the same DB, they should share a cookie store.
        /// - PersistentCookieStore lifecycle should be tied to the Database
        /// lifecycle, since it needs to cease to exist if the underlying
        /// Database ceases to exist.
        /// REF: https://github.com/couchbase/couchbase-lite-android/issues/269
        /// </remarks>
        private CookieStore                             _persistentCookieStore; // Not used yet.

        private Boolean                                 _isOpen;
        private IDictionary<String, ValidateDelegate>   _validations;
        private IDictionary<String, BlobStoreWriter>    _pendingAttachmentsByDigest;
        private IDictionary<String, View>               _views;
        private Int32                                   _transactionLevel;
        private IList<DocumentChange>                   _changesToNotify;
        private Boolean                                 _isPostingChangeNotifications;
        private Object                                  _allReplicatorsLocker = new Object();

        internal String                                 Path { get; private set; }
        internal IList<Replication>                     ActiveReplicators { get; set; }
        internal IList<Replication>                     AllReplicators { get; set; }
        internal LruCache<String, Document>             DocumentCache { get; set; }
        internal IDictionary<String, WeakReference>     UnsavedRevisionDocumentCache { get; set; }


        //TODO: Should thid be a public member?

        /// <summary>
        /// Maximum depth of a document's revision tree (or, max length of its revision history.)
        /// Revisions older than this limit will be deleted during a -compact: operation.
        /// </summary>
        /// <remarks>
        /// Maximum depth of a document's revision tree (or, max length of its revision history.)
        /// Revisions older than this limit will be deleted during a -compact: operation.
        /// Smaller values save space, at the expense of making document conflicts somewhat more likely.
        /// </remarks>
        internal Int32                                  MaxRevTreeDepth { get; set; }

        private Int64                                   StartTime { get; set; }
        private IDictionary<String, FilterDelegate>     Filters { get; set; }

        private String GetDesignDocFunction(String fnName, String key, ICollection<String> outLanguageList)
        {
            var path = fnName.Split('/');
            if (path.Length != 2)
            {
                return null;
            }

            var docId = string.Format("_design/{0}", path[0]);
            var rev = GetDocumentWithIDAndRev(docId, null, DocumentContentOptions.None);
            if (rev == null)
            {
                return null;
            }

            var outLanguage = (string)rev.GetPropertyForKey("language");
            if (outLanguage != null)
            {
                outLanguageList.AddItem(outLanguage);
            }
            else
            {
                outLanguageList.AddItem("javascript");
            }

            var container = (IDictionary<String, Object>)rev.GetPropertyForKey(key);
            return (string)container.Get(path[1]);
        }

        internal Boolean Exists()
        {
            return new FilePath(Path).Exists();
        }

        internal static string MakeLocalDocumentId(string documentId)
        {
            return string.Format("_local/{0}", documentId);
        }

        /// <summary>Deletes obsolete attachments from the sqliteDb and blob store.</summary>
        private bool GarbageCollectAttachments()
        {
            var keys = Store.FindAllAttachmentKeys();
            if(keys == null) {
                return false;
            }

            Log.D(Tag, "Found {0} attachments...", keys.Count);
            var deleted = Attachments.DeleteBlobsExceptWithKeys(keys);
            Log.D(Tag, "...deleted {0} obsolete attachment files.", deleted);

            return deleted >= 0;
        }

        internal String LastSequenceWithCheckpointId(string checkpointId)
        {
            return Store.GetInfo(String.Format("checkpoint/{0}", checkpointId));
        }

        private IDictionary<String, BlobStoreWriter> PendingAttachmentsByDigest
        {
            get {
                return _pendingAttachmentsByDigest ?? (_pendingAttachmentsByDigest = new Dictionary<String, BlobStoreWriter>());
            }
            set {
                _pendingAttachmentsByDigest = value;
            }
        }
            
        internal RevisionList ChangesSince(long lastSeq, ChangesOptions options, FilterDelegate filter)
        {
            RevisionFilterDelegate revFilter = null;
            if(filter != null) {
                revFilter = (rev) => RunFilter(filter, null, rev);
            }

            return Store.ChangesSince(lastSeq, options, revFilter);
        }

        internal bool RunFilter(FilterDelegate filter, IDictionary<string, object> paramsIgnored, RevisionInternal rev)
        {
            if (filter == null)
            {
                return true;
            }
            var publicRev = new SavedRevision(this, rev);
            return filter(publicRev, null);
        }

        /// <exception cref="Couchbase.Lite.CouchbaseLiteException"></exception>
        internal IDictionary<String, Object> GetAllDocs(QueryOptions options)
        {
            return Store.GetAllDocs(options);
        }

        /// <summary>
        /// Creates a one-shot query with the given map function. This is equivalent to creating an
        /// anonymous View and then deleting it immediately after querying it. It may be useful during
        /// development, but in general this is inefficient if this map will be used more than once,
        /// because the entire view has to be regenerated from scratch every time.
        /// </summary>
        /// <returns>The query.</returns>
        /// <param name="map">Map.</param>
        internal Query SlowQuery(MapDelegate map) 
        {
            return new Query(this, map);
        }
          

        internal void RemoveDocumentFromCache(Document document)
        {
            DocumentCache.Remove(document.Id);
            UnsavedRevisionDocumentCache.Remove(document.Id);
        }

        internal BlobStoreWriter AttachmentWriter { get { return new BlobStoreWriter(Attachments); } }

        internal BlobStore Attachments { get; set; }

        internal String PrivateUUID()
        {
            return Store.GetInfo("privateUUID");
        }

        internal String PublicUUID()
        {
            return Store.GetInfo("publicUUID");
        }

        internal BlobStoreWriter GetAttachmentWriter()
        {
            return new BlobStoreWriter(Attachments);
        }

        internal Boolean ReplaceUUIDs()
        {
            Store.SetInfo("publicUUID", Misc.CreateGUID());
            Store.SetInfo("privateUUID", Misc.CreateGUID());

            return true;
        }

        internal void RememberAttachmentWritersForDigests(IDictionary<String, BlobStoreWriter> blobsByDigest)
        {
            PendingAttachmentsByDigest.PutAll(blobsByDigest);
        }

        internal void RememberAttachmentWriter (BlobStoreWriter writer)
        {
            var digest = writer.MD5DigestString();
            PendingAttachmentsByDigest[digest] = writer;
        }


        internal RevisionInternal GetDocumentWithIDAndRev(String id, String revId, DocumentContentOptions contentOptions)
        {
            var rev = Store.GetDocument(id, revId, contentOptions);
            if(rev != null && contentOptions.HasFlag(DocumentContentOptions.IncludeAttachments)) {
                if(!ExpandAttachments(rev, contentOptions)) {
                    return null;
                }
            }

            return rev;
        }

        internal bool ExpandAttachments(RevisionInternal rev, DocumentContentOptions options)
        {
            bool success = true;
            rev.MutateAttachments((name, attachment) =>
            {
                if(attachment.ContainsKey("stub") || attachment.ContainsKey("follows")) {
                    attachment.Remove("stub");
                    attachment.Remove("follows");

                    Attachment attachObj = GetAttachmentForDict(attachment, name);
                    if(attachObj == null) {
                        Log.W(Tag, "Can't get attachment '{0}' of {1}", name, rev);
                        success = false;
                        return attachment;
                    }

                    var data = attachObj.Content;
                    if(data != null) {
                        attachment["data"] = Convert.ToBase64String(data.ToArray());
                        success &= true;
                    } else {
                        Log.W(Tag, "Can't get binary data of attachment '{0}' of {1}", name, rev);
                        success = false;
                    }
                }

                return attachment;
            });

            return success;
        }

        internal Attachment GetAttachmentForDict(IDictionary<string, object> info, string filename)
        {
            if(info == null) {
                return null;
            }

            var retVal = new Attachment(null, filename, info);
            retVal.Database = this;
            return retVal;
        }

        /// <summary>Parses the _revisions dict from a document into an array of revision ID strings.</summary>
        internal static IList<string> ParseCouchDBRevisionHistory(IDictionary<String, Object> docProperties)
        {
            var revisions = docProperties.Get ("_revisions").AsDictionary<string,object> ();
            if (revisions == null)
            {
                return new List<string>();
            }

            var ids = revisions ["ids"].AsList<string> ();
            if (ids == null || ids.Count == 0)
            {
                return new List<string>();
            }

            var revIDs = new List<string>(ids);
            var start = Convert.ToInt64(revisions.Get("start"));
            for (var i = 0; i < revIDs.Count; i++)
            {
                var revID = revIDs[i];
                revIDs.Set(i, Sharpen.Extensions.ToString(start--) + "-" + revID);
            }

            return revIDs;
        }

        // Splits a revision ID into its generation number and opaque suffix string
        internal static int ParseRevIDNumber(string rev)
        {
            var result = -1;
            var dashPos = rev.IndexOf("-", StringComparison.InvariantCultureIgnoreCase);

            if (dashPos >= 0)
            {
                try
                {
                    var revIdStr = rev.Substring(0, dashPos);

                    // Should return -1 when the string has WC at the beginning or at the end.
                    if (revIdStr.Length > 0 && 
                        (char.IsWhiteSpace(revIdStr[0]) || 
                            char.IsWhiteSpace(revIdStr[revIdStr.Length - 1])))
                    {
                        return result;
                    }

                    result = Int32.Parse(revIdStr);
                }
                catch (FormatException)
                {

                }
            }
            // ignore, let it return -1
            return result;
        }

        // Splits a revision ID into its generation number and opaque suffix string
        internal static string ParseRevIDSuffix(string rev)
        {
            var result = String.Empty;
            int dashPos = rev.IndexOf("-", StringComparison.InvariantCultureIgnoreCase);
            if (dashPos >= 0)
            {
                result = Runtime.Substring(rev, dashPos + 1);
            }
            return result;
        }
            
        /// <summary>Splices the contents of an NSDictionary into JSON data (that already represents a dict), without parsing the JSON.</summary>
        internal IEnumerable<Byte> AppendDictToJSON(IEnumerable<Byte> json, IDictionary<String, Object> dict)
        {
            if (dict.Count == 0)
                return json;

            Byte[] extraJSON;
            try
            {
                extraJSON = Manager.GetObjectMapper().WriteValueAsBytes(dict).ToArray();
            }
            catch (Exception e)
            {
                Log.E(Tag, "Error convert extra JSON to bytes", e);
                return null;
            }

            var jsonArray = json.ToArray ();
            int jsonLength = jsonArray.Length;
            int extraLength = extraJSON.Length;

            if (jsonLength == 2)
            {
                // Original JSON was empty
                return extraJSON;
            }

            var newJson = new byte[jsonLength + extraLength - 1];
            Array.Copy(jsonArray.ToArray(), 0, newJson, 0, jsonLength - 1);

            // Copy json w/o trailing '}'
            newJson[jsonLength - 1] = (byte)(',');

            // Add a ','
            Array.Copy(extraJSON, 1, newJson, jsonLength, extraLength - 1);

            return newJson;
        }

        /// <exception cref="Couchbase.Lite.CouchbaseLiteException"></exception>
        internal RevisionInternal PutRevision(RevisionInternal rev, String prevRevId, Status resultStatus)
        {
            return PutRevision(rev, prevRevId, false, resultStatus);
        }

        /// <exception cref="Couchbase.Lite.CouchbaseLiteException"></exception>
        internal RevisionInternal PutRevision(RevisionInternal rev, String prevRevId, Boolean allowConflict)
        {
            Status ignoredStatus = new Status();
            return PutRevision(rev, prevRevId, allowConflict, ignoredStatus);
        }

        /// <summary>Stores a new (or initial) revision of a document.</summary>
        /// <remarks>
        /// Stores a new (or initial) revision of a document.
        /// This is what's invoked by a PUT or POST. As with those, the previous revision ID must be supplied when necessary and the call will fail if it doesn't match.
        /// </remarks>
        /// <param name="oldRev">The revision to add. If the docID is null, a new UUID will be assigned. Its revID must be null. It must have a JSON body.
        ///     </param>
        /// <param name="prevRevId">The ID of the revision to replace (same as the "?rev=" parameter to a PUT), or null if this is a new document.
        ///     </param>
        /// <param name="allowConflict">If false, an error status 409 will be returned if the insertion would create a conflict, i.e. if the previous revision already has a child.
        ///     </param>
        /// <param name="resultStatus">On return, an HTTP status code indicating success or failure.
        ///     </param>
        /// <returns>A new RevisionInternal with the docID, revID and sequence filled in (but no body).
        ///     </returns>
        /// <exception cref="Couchbase.Lite.CouchbaseLiteException"></exception>
        internal RevisionInternal PutRevision(RevisionInternal oldRev, String prevRevId, Boolean allowConflict, Status resultStatus)
        {
            // prevRevId is the rev ID being replaced, or nil if an insert
            var docId = oldRev.GetDocId();
            var deleted = oldRev.IsDeleted();

            if((oldRev == null) || ((prevRevId != null) && (docId == null)) || (deleted && (docId == null)) || ((docId != null) && !IsValidDocumentId(docId))) {
                throw new CouchbaseLiteException(StatusCode.BadId);
            }

            if(oldRev.GetProperties().ContainsKey("_attachments")) {
                var tmpRev = new RevisionInternal(docId, prevRevId, deleted);
                tmpRev.SetProperties(oldRev.GetProperties());
                if(!ProcessAttachmentsForRevision(oldRev, prevRevId)) {
                    return null;
                }

                oldRev.SetProperties(tmpRev.GetProperties());
            }

            StoreValidateDelegate validationBlock = (next, prev, prevId) =>
            {
                try {
                    ValidateRevision(next, prev, prevId);
                } catch(CouchbaseLiteException e) {
                    return e.GetCBLStatus();
                }

                return new Status(StatusCode.Ok);
            };

            var putRev = Store.PutRevision(docId, prevRevId, oldRev.GetProperties(), deleted, allowConflict, validationBlock);
            if(putRev != null) {
                Log.D(Tag, "Put revision --> ", putRev);
            }

            return putRev;
        }

        internal void PostChangeNotifications()
        {
            // This is a 'while' instead of an 'if' because when we finish posting notifications, there
            // might be new ones that have arrived as a result of notification handlers making document
            // changes of their own (the replicator manager will do this.) So we need to check again.
            while(_transactionLevel == 0 && _isOpen && !_isPostingChangeNotifications && _changesToNotify.Count > 0) {
                try {
                    _isPostingChangeNotifications = true;

                    IList<DocumentChange> outgoingChanges = new List<DocumentChange>();
                    foreach(var change in _changesToNotify) {
                        outgoingChanges.Add(change);
                    }
                    _changesToNotify.Clear();
                    // TODO: change this to match iOS and call cachedDocumentWithID
                    var isExternal = false;
                    foreach(var change in outgoingChanges) {
                        var document = GetDocument(change.DocumentId);
                        document.RevisionAdded(change);
                        if(change.SourceUrl != null) {
                            isExternal = true;
                        }
                    }

                    var args = new DatabaseChangeEventArgs { 
                        Changes = outgoingChanges,
                        IsExternal = isExternal,
                        Source = this
                    };

                    var changeEvent = Changed;
                    if(changeEvent != null)
                        changeEvent(this, args);
                } catch(Exception e) {
                    Log.E(Tag, " got exception posting change notifications", e);
                } finally {
                    _isPostingChangeNotifications = false;
                }
            }
        }

        internal bool ProcessAttachmentsForRevision(RevisionInternal rev, string prevRevId)
        {
            throw new NotImplementedException();
            // If there are no attachments in the new rev, there's nothing to do:
            /*IDictionary<string, object> revAttachments = null;
            var properties = rev.GetProperties ();
            if(properties != null) {
                revAttachments = properties.Get("_attachments").AsDictionary<string, object>();
            }

            if(revAttachments == null) {
                return true;
            }

            if(revAttachments.Count == 0 || rev.IsDeleted()) {
                properties.Remove("_attachments");
                rev.SetProperties(properties);
                return true;
            }

            var generation = RevisionInternal.GenerationFromRevID(prevRevId) + 1;
            IDictionary<string, object> parentAttachments = null;
            return rev.MutateAttachments((name, attachment) =>
            {
                var attachObj = new AttachmentInternal(name, attachment);
                if(attachObj == null) {
                    return null;
                }

                if(attachment.ContainsKey("follows") && (bool)attachment["follows"]) {
                    try {
                        InstallAttachment(attachObj, attachment);
                    } catch(Exception) {
                        return null;
                    }
                } else if(attachment.ContainsKey("stub") && (bool)attachment["stub"]) {
                    if(parentAttachments == null && prevRevId != null) {
                        parentAttachments = AttachmentsForDocId(rev.GetDocId(), prevRevId);
                        if(parentAttachments == null) {
                            return null;
                        }
                    }

                    return parentAttachments.Get(name).AsDictionary<string, object>();
                }

                if(attachObj.GetRevpos() == 0) {
                    attachObj.SetRevpos(generation);
                } else if(attachObj.GetRevpos() >= generation) {
                    return null;
                }
                Debug.Assert(attachObj.IsValid());
                return new Dictionary<string, object>
                {
                    {"stub", true},
                    {AttachmentMetadataKeys.Digest, attachObj.GetBlobKey().Base64Digest()},
                    {AttachmentMetadataKeys.ContentType, attachObj.GetContentType()},
                    {"revpos", attachObj.GetRevpos()},
                    {AttachmentMetadataKeys.Length, attachObj.GetLength()}
                };
            });*/
        }

        internal IDictionary<string, object> AttachmentsForDocId(string docId, string revId)
        {
            var rev = new RevisionInternal(docId, revId, false);
            LoadRevisionBody(rev, DocumentContentOptions.None);

            return rev.GetPropertyForKey("_attachments").AsDictionary<string, object>();
        }

        /// <exception cref="Couchbase.Lite.CouchbaseLiteException"></exception>
        internal void InstallAttachment(AttachmentInternal attachment, IDictionary<String, Object> attachInfo)
        {
            var digest = (string)attachInfo.Get("digest");
            if(digest == null) {
                throw new CouchbaseLiteException(StatusCode.BadAttachment);
            }

            if(PendingAttachmentsByDigest != null && PendingAttachmentsByDigest.ContainsKey(digest)) {
                var writer = PendingAttachmentsByDigest.Get(digest);
                try {
                    var blobStoreWriter = writer;
                    blobStoreWriter.Install();
                    attachment.SetBlobKey(blobStoreWriter.GetBlobKey());
                    attachment.SetLength(blobStoreWriter.GetLength());
                } catch(Exception e) {
                    throw new CouchbaseLiteException(e, StatusCode.StatusAttachmentError);
                }
            }
        }

        internal void StubOutAttachmentsInRevision(IDictionary<String, AttachmentInternal> attachments, RevisionInternal rev)
        {
            var properties = rev.GetProperties();
            var attachmentProps = properties.Get("_attachments");
            if (attachmentProps != null)
            {
                var nuAttachments = new Dictionary<string, object>();
                foreach (var kvp in attachmentProps.AsDictionary<string,object>())
                {
                    var attachmentValue = kvp.Value.AsDictionary<string,object>();
                    if (attachmentValue.ContainsKey("follows") || attachmentValue.ContainsKey("data"))
                    {
                        attachmentValue.Remove("follows");
                        attachmentValue.Remove("data");

                        attachmentValue["stub"] = true;
                        if (attachmentValue.Get("revpos") == null)
                        {
                            attachmentValue.Put("revpos", rev.GetGeneration());
                        }

                        var attachmentObject = attachments.Get(kvp.Key);
                        if (attachmentObject != null)
                        {
                            attachmentValue.Put("length", attachmentObject.GetLength());
                            if (attachmentObject.GetBlobKey() != null)
                            {
                                attachmentValue.Put("digest", attachmentObject.GetBlobKey().Base64Digest());
                            }
                        }
                    }
                    nuAttachments[kvp.Key] = attachmentValue;
                }

                properties["_attachments"] = nuAttachments;  
            }
        }

        internal Uri FileForAttachmentDict(IDictionary<String, Object> attachmentDict)
        {
            var digest = (string)attachmentDict.Get("digest");
            if (digest == null)
            {
                return null;
            }
            string path = null;
            var pending = PendingAttachmentsByDigest.Get(digest);
            if (pending != null)
            {
                path = pending.FilePath;
            }
            else
            {
                // If it's an installed attachment, ask the blob-store for it:
                var key = new BlobKey(digest);
                path = Attachments.PathForKey(key);
            }
            Uri retval = null;
            try
            {
                retval = new FilePath(path).ToURI().ToURL();
            }
            catch (UriFormatException)
            {
            }
            //NOOP: retval will be null
            return retval;
        }

        internal static void StubOutAttachmentsInRevBeforeRevPos(RevisionInternal rev, long minRevPos, bool attachmentsFollow)
        {
            if (minRevPos <= 1 && !attachmentsFollow)
            {
                return;
            }

            rev.MutateAttachments((name, attachment) =>
            {
                var revPos = 0L;
                if (attachment.ContainsKey("revpos"))
                {
                    revPos = Convert.ToInt64(attachment["revpos"]);
                }

                var includeAttachment = (revPos == 0 || revPos >= minRevPos);
                var stubItOut = !includeAttachment && (!attachment.ContainsKey("stub") || (bool)attachment["stub"] == false);
                var addFollows = includeAttachment && attachmentsFollow && (!attachment.ContainsKey("follows") || (bool)attachment["follows"] == false);

                if (!stubItOut && !addFollows)
                {
                    return attachment; // no change
                }

                // Need to modify attachment entry
                var editedAttachment = new Dictionary<string, object>(attachment);
                editedAttachment.Remove("data");

                if (stubItOut)
                {
                    // ...then remove the 'data' and 'follows' key:
                    editedAttachment.Remove("follows");
                    editedAttachment["stub"] = true;
                    Log.V(Tag, String.Format("Stubbed out attachment {0}: revpos {1} < {2}", rev, revPos, minRevPos));
                }
                else if (addFollows)
                {
                    editedAttachment.Remove("stub");
                    editedAttachment["follows"] = true;
                    Log.V(Tag, String.Format("Added 'follows' for attachment {0}: revpos {1} >= {2}", rev, revPos, minRevPos));
                }

                return editedAttachment;
            });
        }

        // Replaces the "follows" key with the real attachment data in all attachments to 'doc'.
        internal bool InlineFollowingAttachmentsIn(RevisionInternal rev)
        {
            return rev.MutateAttachments((s, attachment)=>
            {
                if (!attachment.ContainsKey("follows"))
                {
                    return attachment;
                }

                var fileURL = FileForAttachmentDict(attachment);
                byte[] fileData = null;
                try
                {
                    var inputStream = fileURL.OpenConnection().GetInputStream();
                    var os = new ByteArrayOutputStream();
                    inputStream.CopyTo(os);
                    fileData = os.ToByteArray();
                }
                catch (IOException e)
                {
                    Log.E(Tag, "could not retrieve attachment data: {0}".Fmt(fileURL.ToString()), e);
                    return null;
                }

                var editedAttachment = new Dictionary<string, object>(attachment);
                editedAttachment.Remove("follows");
                editedAttachment.Put("data", Convert.ToBase64String(fileData));

                return editedAttachment;
            });
        }

        /// <summary>INSERTION:</summary>
        internal IEnumerable<Byte> EncodeDocumentJSON(RevisionInternal rev)
        {
            var origProps = rev.GetProperties();
            if (origProps == null)
            {
                return null;
            }
            var specialKeysToLeave = new[] { "_removed", "_replication_id", "_replication_state", "_replication_state_time" };

            // Don't allow any "_"-prefixed keys. Known ones we'll ignore, unknown ones are an error.
            var properties = new Dictionary<String, Object>(origProps.Count);
            foreach (var key in origProps.Keys)
            {
                var shouldAdd = false;
                if (key.StartsWith("_", StringComparison.InvariantCultureIgnoreCase))
                {
                    if (!KnownSpecialKeys.Contains(key))
                    {
                        Log.E(Tag, "Database: Invalid top-level key '" + key + "' in document to be inserted");
                        return null;
                    }
                    if (specialKeysToLeave.Contains(key))
                    {
                        shouldAdd = true;
                    }
                }
                else
                {
                    shouldAdd = true;
                }
                if (shouldAdd)
                {
                    properties.Put(key, origProps.Get(key));
                }
            }
            IEnumerable<byte> json = null;
            try
            {
                json = Manager.GetObjectMapper().WriteValueAsBytes(properties);
            }
            catch (Exception e)
            {
                Log.E(Tag, "Error serializing " + rev + " to JSON", e);
            }
            return json;
        }

        /// <summary>
        /// Given a revision, read its _attachments dictionary (if any), convert each attachment to a
        /// AttachmentInternal object, and return a dictionary mapping names-&gt;CBL_Attachments.
        /// </summary>
        /// <remarks>
        /// Given a revision, read its _attachments dictionary (if any), convert each attachment to a
        /// AttachmentInternal object, and return a dictionary mapping names-&gt;CBL_Attachments.
        /// </remarks>
        /// <exception cref="Couchbase.Lite.CouchbaseLiteException"></exception>
        internal IDictionary<String, AttachmentInternal> GetAttachmentsFromRevision(RevisionInternal rev)
        {
            var revAttachments = rev.GetPropertyForKey("_attachments").AsDictionary<string, object>();
            if (revAttachments == null || revAttachments.Count == 0 || rev.IsDeleted())
            {
                return new Dictionary<string, AttachmentInternal>();
            }

            var attachments = new Dictionary<string, AttachmentInternal>();
            foreach (var name in revAttachments.Keys)
            {
                var attachInfo = revAttachments.Get(name).AsDictionary<string, object>();
                var contentType = (string)attachInfo.Get("content_type");
                var attachment = new AttachmentInternal(name, contentType);
                var newContentBase64 = (string)attachInfo.Get("data");
                if (newContentBase64 != null)
                {
                    // If there's inline attachment data, decode and store it:
                    byte[] newContents;
                    try
                    {
                        newContents = StringUtils.ConvertFromUnpaddedBase64String (newContentBase64);
                    }
                    catch (IOException e)
                    {
                        throw new CouchbaseLiteException(e, StatusCode.BadEncoding);
                    }
                    attachment.SetLength(newContents.Length);
                    var outBlobKey = new BlobKey();
                    var storedBlob = Attachments.StoreBlob(newContents, outBlobKey);
                    attachment.SetBlobKey(outBlobKey);
                    if (!storedBlob)
                    {
                        throw new CouchbaseLiteException(StatusCode.StatusAttachmentError);
                    }
                }
                else
                {
                    if (attachInfo.ContainsKey("follows") && ((bool)attachInfo.Get("follows")))
                    {
                        // "follows" means the uploader provided the attachment in a separate MIME part.
                        // This means it's already been registered in _pendingAttachmentsByDigest;
                        // I just need to look it up by its "digest" property and install it into the store:
                        InstallAttachment(attachment, attachInfo);
                    }
                    else
                    {
                        // This item is just a stub; validate and skip it
                        if (((bool)attachInfo.Get("stub")) == false)
                        {
                            throw new CouchbaseLiteException("Expected this attachment to be a stub", StatusCode.
                                                             BadAttachment);
                        }

                        var revPos = Convert.ToInt64(attachInfo.Get("revpos"));
                        if (revPos <= 0)
                        {
                            throw new CouchbaseLiteException("Invalid revpos: " + revPos, StatusCode.BadAttachment);
                        }

                        continue;
                    }
                }
                // Handle encoded attachment:
                string encodingStr = (string)attachInfo.Get("encoding");
                if (encodingStr != null && encodingStr.Length > 0)
                {
                    if (Runtime.EqualsIgnoreCase(encodingStr, "gzip"))
                    {
                        attachment.SetEncoding(AttachmentEncoding.AttachmentEncodingGZIP);
                    }
                    else
                    {
                        throw new CouchbaseLiteException("Unnkown encoding: " + encodingStr, StatusCode.BadEncoding
                                                        );
                    }
                    attachment.SetEncodedLength(attachment.GetLength());
                    if (attachInfo.ContainsKey("length"))
                    {
                        attachment.SetLength((long)attachInfo.Get("length"));
                    }
                }
                if (attachInfo.ContainsKey("revpos"))
                {
                    var revpos = Convert.ToInt32(attachInfo.Get("revpos"));
                    attachment.SetRevpos(revpos);
                }
                attachments[name] = attachment;
            }
            return attachments;
        }

        internal String GenerateIDForRevision(RevisionInternal rev, IEnumerable<byte> json, IDictionary<string, AttachmentInternal> attachments, string previousRevisionId)
        {
            MessageDigest md5Digest;

            // Revision IDs have a generation count, a hyphen, and a UUID.
            int generation = 0;
            if (previousRevisionId != null)
            {
                generation = RevisionInternal.GenerationFromRevID(previousRevisionId);
                if (generation == 0)
                {
                    return null;
                }
            }

            // Generate a digest for this revision based on the previous revision ID, document JSON,
            // and attachment digests. This doesn't need to be secure; we just need to ensure that this
            // code consistently generates the same ID given equivalent revisions.
            try
            {
                md5Digest = MessageDigest.GetInstance("MD5");
            }
            catch (NoSuchAlgorithmException e)
            {
                throw new RuntimeException(e);
            }

            var length = 0;
            if (previousRevisionId != null)
            {
                var prevIDUTF8 = Encoding.UTF8.GetBytes(previousRevisionId);
                length = prevIDUTF8.Length;
            }

            if (length > unchecked((0xFF)))
            {
                return null;
            }

            var lengthByte = unchecked((byte)(length & unchecked((0xFF))));
            var lengthBytes = new[] { lengthByte };
            md5Digest.Update(lengthBytes);

            var isDeleted = ((rev.IsDeleted()) ? 1 : 0);
            var deletedByte = new[] { unchecked((byte)isDeleted) };
            md5Digest.Update(deletedByte);

            var attachmentKeys = new List<String>(attachments.Keys);
            attachmentKeys.Sort();

            foreach (string key in attachmentKeys)
            {
                var attachment = attachments.Get(key);
                md5Digest.Update(attachment.GetBlobKey().GetBytes());
            }

            if (json != null)
            {
                md5Digest.Update(json != null ? json.ToArray() : null);
            }

            var md5DigestResult = md5Digest.Digest();
            var digestAsHex = BitConverter.ToString(md5DigestResult).Replace("-", String.Empty);
            int generationIncremented = generation + 1;
            return string.Format("{0}-{1}", generationIncremented, digestAsHex).ToLower();
        }

        /// <exception cref="Couchbase.Lite.CouchbaseLiteException"></exception>
        internal RevisionInternal LoadRevisionBody(RevisionInternal rev, DocumentContentOptions contentOptions)
        {
            if(rev.GetBody() != null && contentOptions == DocumentContentOptions.None && rev.GetSequence() != 0) {
                return rev;
            }

            if((rev.GetDocId() == null) || (rev.GetRevId() == null)) {
                Log.E(Database.Tag, "Error loading revision body");
                throw new CouchbaseLiteException(StatusCode.PreconditionFailed);
            }

            return Store.LoadRevisionBody(rev, contentOptions);
        }

        internal Boolean ExistsDocumentWithIDAndRev(String docId, String revId)
        {
            return GetDocumentWithIDAndRev(docId, revId, DocumentContentOptions.NoBody) != null;
        }

        /// <summary>DOCUMENT & REV IDS:</summary>
        internal Boolean IsValidDocumentId(string id)
        {
            // http://wiki.apache.org/couchdb/HTTP_Document_API#Documents
            if (String.IsNullOrEmpty (id)) {
                return false;
            }

            return id [0] != '_' || id.StartsWith ("_design/", StringComparison.InvariantCultureIgnoreCase);
        }

        /// <summary>Updates or deletes an attachment, creating a new document revision in the process.
        ///     </summary>
        /// <remarks>
        /// Updates or deletes an attachment, creating a new document revision in the process.
        /// Used by the PUT / DELETE methods called on attachment URLs.
        /// </remarks>
        /// <exclude></exclude>
        /// <exception cref="Couchbase.Lite.CouchbaseLiteException"></exception>
        internal RevisionInternal UpdateAttachment(string filename, BlobStoreWriter body, string contentType, AttachmentEncoding encoding, string docID, string oldRevID)
        {
            var isSuccessful = false;
            if (String.IsNullOrEmpty (filename) || (body != null && contentType == null) || (oldRevID != null && docID == null) || (body != null && docID == null))
            {
                throw new CouchbaseLiteException(StatusCode.BadRequest);
            }

            RevisionInternal newRev = null;

            var transactionSucceeded = RunInTransaction(() =>
            {
                try
                {
                    var oldRev = new RevisionInternal(docID, oldRevID, false);
                    if (oldRevID != null)
                    {
                        // Load existing revision if this is a replacement:
                        try
                        {
                            LoadRevisionBody(oldRev, DocumentContentOptions.None);
                        }
                        catch (CouchbaseLiteException e)
                        {
                            if (e.GetCBLStatus().GetCode() == StatusCode.NotFound && ExistsDocumentWithIDAndRev(docID, null))
                            {
                                throw new CouchbaseLiteException(StatusCode.Conflict);
                            }
                        }
                    }
                    else
                    {
                        // If this creates a new doc, it needs a body:
                        oldRev.SetBody(new Body(new Dictionary<string, object>()));
                    }

                    // Update the _attachments dictionary:
                    var oldRevProps = oldRev.GetProperties();
                    IDictionary<string, object> attachments = null;

                    if (oldRevProps != null)
                    {
                        attachments = oldRevProps.Get("_attachments").AsDictionary<string, object>();
                    }

                    if (attachments == null)
                    {
                        attachments = new Dictionary<string, object>();
                    }

                    if (body != null)
                    {
                        var key = body.GetBlobKey();
                        var digest = key.Base64Digest();

                        var blobsByDigest = new Dictionary<string, BlobStoreWriter>();
                        blobsByDigest.Put(digest, body);

                        RememberAttachmentWritersForDigests(blobsByDigest);

                        var encodingName = (encoding == AttachmentEncoding.AttachmentEncodingGZIP) ? "gzip" : null;
                        var dict = new Dictionary<string, object>();

                        dict.Put("digest", digest);
                        dict.Put("length", body.GetLength());
                        dict.Put("follows", true);
                        dict.Put("content_type", contentType);
                        dict.Put("encoding", encodingName);

                        attachments.Put(filename, dict);
                    }
                    else
                    {
                        if (oldRevID != null && !attachments.ContainsKey(filename))
                        {
                            throw new CouchbaseLiteException(StatusCode.NotFound);
                        }

                        attachments.Remove(filename);
                    }

                    var properties = oldRev.GetProperties();
                    properties.Put("_attachments", attachments);
                    oldRev.SetProperties(properties);

                    // Create a new revision:
                    var putStatus = new Status();
                    newRev = PutRevision(oldRev, oldRevID, false, putStatus);

                    isSuccessful = true;

                }
                catch (SQLException e)
                {
                    Log.E(Tag, "Error updating attachment", e);
                    throw new CouchbaseLiteException(StatusCode.InternalServerError);
                }

                return isSuccessful;
            });

            return newRev;
        }

        /// <summary>VALIDATION</summary>
        /// <exception cref="Couchbase.Lite.CouchbaseLiteException"></exception>
        internal void ValidateRevision(RevisionInternal newRev, RevisionInternal oldRev, String parentRevId)
        {
            if(_validations == null || _validations.Count == 0) {
                return;
            }

            var publicRev = new SavedRevision(this, newRev);
            publicRev.ParentRevisionID = parentRevId;
            var context = new ValidationContext(this, oldRev, newRev);
            foreach(var validationName in _validations.Keys) {
                var validation = GetValidation(validationName);
                validation(publicRev, context);
                if(context.RejectMessage != null) {
                    throw new CouchbaseLiteException(context.RejectMessage, StatusCode.Forbidden);
                }
            }
        }

        internal String AttachmentStorePath 
        {
            get 
            {
                var attachmentStorePath = Path;
                int lastDotPosition = attachmentStorePath.LastIndexOf(".", StringComparison.InvariantCultureIgnoreCase);
                if (lastDotPosition > 0)
                {
                    attachmentStorePath = attachmentStorePath.Substring(0, lastDotPosition);
                }
                attachmentStorePath = attachmentStorePath + FilePath.separator + "attachments";
                return attachmentStorePath;
            }
        }

        //TODO.JHB: After ForestDB implementation, check for existing Sqlite
        internal Boolean Open()
        {
            string storageType = Manager.StorageType ?? "Sqlite";
            Store = CouchStoreFactory.GetCouchStore(storageType); 
            Store.Delegate = this;
            if(!Store.Open(Path, _readOnly, Manager)) {
                return false;
            }

            try {
                Attachments = new BlobStore(AttachmentStorePath);
            } catch(ArgumentException e) {
                Log.E(Tag, "Could not initialize attachment store", e);
                Store.Close();
                return false;
            }
                
            return true;
        }

        internal Boolean Close()
        {
            if(!_isOpen) {
                return false;
            }

            Log.I(Tag, "Closing database {0}", Name);
            if(_views != null) {
                foreach(View view in _views.Values) {
                    view.DatabaseClosing();
                }
            }

            _views = null;
            if(ActiveReplicators != null) {
                // 
                var activeReplicators = new Replication[ActiveReplicators.Count];
                ActiveReplicators.CopyTo(activeReplicators, 0);
                foreach(Replication replicator in activeReplicators) {
                    replicator.DatabaseClosing();
                }
                ActiveReplicators = null;
            }

            _isOpen = false;
            _transactionLevel = 0;
            Store.Close();
            Store = null;
            return true;
        }

        internal void AddReplication(Replication replication)
        {
            lock (_allReplicatorsLocker) { AllReplicators.Add(replication); }
        }

        internal void ForgetReplication(Replication replication)
        {
            lock (_allReplicatorsLocker) { AllReplicators.Remove(replication); }
        }

        internal void AddActiveReplication(Replication replication)
        {
            ActiveReplicators.Add(replication);
            replication.Changed += (sender, e) => 
            {
                if (!e.Source.IsRunning && ActiveReplicators != null)
                {
                    ActiveReplicators.Remove(e.Source);
                }
            };
        }

    #endregion

        #region IStoreDelegate

        public SymmetricKey EncryptionKey {
            get {
                return null;
            }
        }

        public string GenerateRevID(IEnumerable<byte> json, bool deleted, string prevRevId)
        {
            // Revision IDs have a generation count, a hyphen, and a hex digest.
            int generation = 0;
            if(prevRevId != null) {
                generation = RevisionInternal.GenerationFromRevID(prevRevId);
                if(generation == 0) {
                    return null;
                }
            }

            // Generate a digest for this revision based on the previous revision ID, document JSON,
            // and attachment digests. This doesn't need to be secure; we just need to ensure that this
            // code consistently generates the same ID given equivalent revisions.
            MessageDigest md5digest = MessageDigest.GetInstance("MD5");

            var prevBytes = Encoding.UTF8.GetBytes(prevRevId);
            var length = prevBytes.Length;
            byte lengthByte = (byte)(prevBytes.Length & 0xFF);
            md5digest.Update(lengthByte);
            if(length > 0) {
                md5digest.Update(prevBytes);
            }

            byte deletedByte = deleted ? (byte)1 : (byte)0;
            md5digest.Update(deletedByte);
            md5digest.Update(json.ToArray());
            var digestBytes = md5digest.Digest();

            StringBuilder sb = new StringBuilder();
            sb.AppendFormat("{0}-", generation + 1);
            foreach(byte b in digestBytes) {
                sb.AppendFormat("{0:x2}", b);
            }

            return sb.ToString();
        }

        public void DatabaseStorageChanged(DocumentChange change)
        {

        }

        public void StorageExitedTransaction(bool committed)
        {

        }

        #endregion
    
    }

    #region Global Delegates

    /// <summary>
    /// A delegate that can validate a key/value change.
    /// </summary>
    public delegate Boolean ValidateChangeDelegate(String key, Object oldValue, Object newValue);

    /// <summary>
    /// A delegate that can be run asynchronously on a <see cref="Couchbase.Lite.Database"/>.
    /// </summary>
    public delegate void RunAsyncDelegate(Database database);

    /// <summary>
    /// A delegate that can be used to accept/reject new <see cref="Couchbase.Lite.Revision"/>s being added to a <see cref="Couchbase.Lite.Database"/>.
    /// </summary>
    public delegate Boolean ValidateDelegate(Revision newRevision, IValidationContext context);

    /// <summary>
    /// A delegate that can be used to include/exclude <see cref="Couchbase.Lite.Revision"/>s during push <see cref="Couchbase.Lite.Replication"/>.
    /// </summary>
    public delegate Boolean FilterDelegate(SavedRevision revision, Dictionary<String, Object> filterParams);

    /// <summary>
    /// A delegate that can be invoked to compile source code into a <see cref="FilterDelegate"/>.
    /// </summary>
    public delegate FilterDelegate CompileFilterDelegate(String source, String language);

    /// <summary>
    /// A delegate that can be run in a transaction on a <see cref="Couchbase.Lite.Database"/>.
    /// </summary>
    public delegate Boolean RunInTransactionDelegate();

    ///
    /// <summary>The event raised when a <see cref="Couchbase.Lite.Database"/> changes</summary>
    ///
    public class DatabaseChangeEventArgs : EventArgs 
    {
        /// <summary>
        /// Gets the <see cref="Couchbase.Lite.Database"/> that raised the event.
        /// </summary>
        /// <value>The <see cref="Couchbase.Lite.Database"/> that raised the event.</value>
            public Database Source { get; internal set; }

        /// <summary>
        /// Returns true if the change was not made by a Document belonging to this Database 
        /// (e.g. it came from another process or from a pull Replication), otherwise false.
        /// </summary>
        /// <value>true if the change was not made by a Document belonging to this Database 
        /// (e.g. it came from another process or from a pull Replication), otherwise false</value>
            public Boolean IsExternal { get; internal set; }

        /// <summary>
        /// Gets the DocumentChange details for the Documents that caused the Database change.
        /// </summary>
        /// <value>The DocumentChange details for the Documents that caused the Database change.</value>
            public IEnumerable<DocumentChange> Changes { get; internal set; }
    }

    #endregion
}

