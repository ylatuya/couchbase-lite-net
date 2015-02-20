using System;
using System.Collections.Generic;

namespace Couchbase.Lite.Store
{

    /// <summary>
    /// Storage for a QueryRow. Instantiated by a IViewStorage when it creates a QueryRow.
    /// </summary>
    internal interface IQueryRowStorage
    {

        /// <summary>
        /// Given the raw data of a row's value, returns <c>true</c> if this is a non-JSON placeholder representing
        /// the entire document. If so, the QueryRow will not parse this data but will instead fetch the
        /// document's body from the database and use that as its value.
        /// </summary>
        bool RowValueIsEntireDoc(byte[] valueData);

        /// <summary>
        /// Parses a "normal" (not entire-doc) row value into a JSON-compatible object.
        /// </summary>
        object ParseRowValue(byte[] valueData);

        /// <summary>
        /// Fetches a document's body; called when the row value represents the entire document.
        /// </summary>
        /// <returns>The document properties, or nil on error</returns>
        /// <param name="docId">The document ID</param>
        /// <param name="sequenceNumber">The sequence representing this revision</param>
        IDictionary<string, object> DocumentProperties(string docId, Int64 sequenceNumber);

        /// <summary>
        /// Fetches the full text that was emitted for the given document.
        /// </summary>
        /// <returns>The full text as UTF-8 data, or null on error.</returns>
        /// <param name="docId">The document ID</param>
        /// <param name="sequenceNumber">The sequence representing this revision</param>
        /// <param name="fullTextID">The opaque ID given when the QueryRow was created; this is used to
        /// disambiguate between multiple calls to emit() made for a single document.</param>
        byte[] FullTextForDocument(string docId, Int64 sequenceNumber, UInt64 fullTextID);

    }
}

