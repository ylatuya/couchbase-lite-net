using System;

namespace Couchbase.Lite.Store
{

    /// <summary>
    /// Delegate of an IViewStorage instance. View implements this.
    /// </summary>
    internal interface IViewStorageDelegate
    {

        /// <summary>
        /// The current map block. Never null.
        /// </summary>
        MapDelegate MapBlock { get; }

        /// <summary>
        /// The current reduce block, or null if there is none.
        /// </summary>
        ReduceDelegate ReduceBlock { get; }

        /// <summary>
        /// The current map version string. If this changes, the storage's SetVersion() method will be
        /// called to notify it, so it can invalidate the index.
        /// </summary>
        string MapVersion { get; }

    }
}

