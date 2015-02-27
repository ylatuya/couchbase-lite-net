using System;
using Nancy.Hosting.Self;

namespace Couchbase.Lite.Listener
{
    public class HttpListener
    {
        public void Start()
        {
            using (var host = new NancyHost(new Uri("http://localhost:4984")))
            {
                host.Start();
            }
        }
    }
}

