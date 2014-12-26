using System;

namespace ScrapeNHLCareerStats
{
    public class FastTimeoutWebClient : System.Net.WebClient
    {
        protected override System.Net.WebRequest GetWebRequest(Uri uri)
        {
            var w = base.GetWebRequest(uri);
            w.Timeout = 1500;
            return w;
        }
    }
}