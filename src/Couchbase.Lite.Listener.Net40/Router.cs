using System;
using Nancy;

namespace Couchbase.Lite.Listener
{
    public class Router : NancyModule
    {
        public Router()
        {
            RegisterServerMethods();
            RegisterDatabaseMethods();
            RegisterDocumentMethods();
            RegisterLocalDocumentMethods();
            RegisterAuthenticationMethods();
        }

        void RegisterServerMethods()
        {
            // HTTP GET requests.
            Get["/"] = ServerMethods.Greeting;
            Get["/_active_tasks"] = ServerMethods.GetActiveTasks;
            Get["/_all_dbs"] = ServerMethods.GetAllDatabases;
            Get["/_session"] = ServerMethods.GetSession;
            Get["/_uuids"] = ServerMethods.GetAllUniqueIds;

            // HTTP POST requests.
            Post["/_replicate"] = ServerMethods.ManageReplicationSession;
        }

        void RegisterDatabaseMethods()
        {
            // Manage configurations
            Get["/{db}"] = DatabaseMethods.GetConfiguration;
            Post["/{db}"] = DatabaseMethods.ExecuteTemporaryViewFunction;
            Put["/{db}"] = DatabaseMethods.UpdateConfiguration;
            Delete["/{db}"] = DatabaseMethods.DeleteConfiguration;

            // Bulk document operations
            Get["/{db}/_all_docs"] = DatabaseMethods.GetAllDocuments;
            Post["/{db}/_all_docs"] = DatabaseMethods.GetAllSpecifiedDocuments;
            Post["/{db}/_bulk_docs"] = DatabaseMethods.ProcessDocumentChangeOperationss;

            // Changes feed
            Get["/{db}/_changes"] = DatabaseMethods.GetChanges;

            // File management
            Post["/{db}/_compact"] = DatabaseMethods.Compact;
            Post["/{db}/_purge"] = DatabaseMethods.Purge;
            Post["/{db}/_temp_view"] = DatabaseMethods.ExecuteTemporaryViewFunction;
        }

        void RegisterDocumentMethods()
        {
            Get["/{db}/{id}"] = DocumentMethods.GetDocument;
            Delete["/{db}/{id}"] = DocumentMethods.DeleteDocument;
            Put["/{db}/{id}"] = DocumentMethods.UpdateDocument;
            Post["/{db}/"] = DocumentMethods.CreateDocument;

            // Attachments
            Get["/{db}/{id}/{attach}"] = DocumentMethods.GetAttachment;
            Delete["/{db}/{id}/{attach}"] = DocumentMethods.DeleteAttachment;
            Put["/{db}/{id}/{attach}"] = DocumentMethods.UpdateAttachment;
        }

        void RegisterLocalDocumentMethods()
        {
            Get["/{db}/_local/{id}"] = DocumentMethods.GetLocalDocument;
            Delete["/{db}/_local/{id}"] = DocumentMethods.DeleteLocalDocument;
            Put["/{db}/_local/{id}"] = DocumentMethods.UpdateLocalDocument;
        }

        void RegisterAuthenticationMethods()
        {
            Post["/_facebook_token"] = AuthenticationMethods.RegisterFacebookToken;
            Post["/_persona_assertion"] = AuthenticationMethods.RegisterPersonaToken;
        }
    }
}

