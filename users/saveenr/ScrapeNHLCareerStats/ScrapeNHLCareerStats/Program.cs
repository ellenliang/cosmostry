using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Security.Policy;
using System.Text;
using System.Threading.Tasks;
using HAP=HtmlAgilityPack;

namespace ScrapeNHLCareerStats
{

    public class MyWebClient : System.Net.WebClient
    {
        protected override System.Net.WebRequest GetWebRequest(Uri uri)
        {
            var w = base.GetWebRequest(uri);
            w.Timeout = 1500;
            return w;
        }
    }
    class Program
    {
        static void Main(string[] args)
        {

            Get_PlayerInfo();

        }

        static void Get_PlayerInfo()
        {
            var rans = new System.Random();

            var wc = new MyWebClient();

            var lines = System.IO.File.ReadLines(@"D:\github\CosmosSamples\users\saveenr\NHL\playerids\playerids_final.txt").ToList();
            lines = lines.Select(l => l.Trim()).ToList();

            System.Console.WriteLine("Total = {0}",lines.Count);

            int n = 1;
            foreach (string line in lines)
            {
                //System.Console.WriteLine("Current = {0} Total = {1}", n, lines.Count);

                var tokens = line.Split('|');
                string name = tokens[0].Trim();
                string playerid = tokens[1].Trim();

                
                string filename = @"D:\playerstats\" + playerid + ".htm";

                if (System.IO.File.Exists(filename))
                {
                    n++;
                    continue;
                }
                else
                {
                    Console.WriteLine("{0} - {1}", playerid, name);
                    string url = @"http://www.nhl.com/ice/player.htm?id=" + playerid;
                    try
                    {
                        wc.DownloadFile(url, filename);
                        n++;

                    }
                    catch (System.Net.WebException)
                    {
                        Console.WriteLine("WEB EXCEPTION");
                    }
                    finally
                    {
                        double wait = rans.NextDouble() * 30 * 1000.0;
                        System.Console.WriteLine("Sleeping {0}", wait / 1000.0);
                        //System.Threading.Thread.Sleep((int)wait);

                    }
                    
                }

            }
        }

        static void Main_ScrapeCareer(string[] args)
        {
            string sep = "\t";

            var fp = System.IO.File.CreateText(@"D:\careerstats.tsv");
            int max_page = 216;

            for (int i = 1; i <= max_page; i++)
            {
                Console.WriteLine(i);
                string url = get_career_page(i);

                var wc = new System.Net.WebClient();
                var doc = new HAP.HtmlDocument();


                doc.LoadHtml(wc.DownloadString(url));

                var tables = doc.DocumentNode.Descendants("table").ToList();

                var t4 = tables[4];

                var trs = t4.Descendants("tr").ToList();

                bool found_header = false;

                var texts = new List<string>();
                foreach (var tr in trs)
                {
                    var ths = tr.Descendants("th").ToList();

                    if (!found_header)
                    {
                        if (ths.Count > 0)
                        {
                            HandleRow2(texts, ths, fp, sep);
                        }
                        found_header = true;
                    }

                    var tds = tr.Descendants("td").ToList();
                    HandleRow2(texts, tds, fp, sep);

                    fp.WriteLine();
                }

                if (i > 3)
                {
                    //break;
                }

            }
            fp.Close();
        }

        private static void HandleRow(List<string> texts, List<HAP.HtmlNode> ths, StreamWriter fp, string sep)
        {
            texts.Clear();
            foreach (var th in ths)
            {
                string text = trim(th);
                texts.Add(text);
            }

            foreach (string text in texts)
            {
                fp.Write(text);
                fp.Write(sep);
            }


            if (texts.TrueForAll(s => s.Length < 1))
            {
                // do nothing
            }
            else
            {
            }
        }
        private static void HandleRow2(List<string> texts, List<HAP.HtmlNode> ths, StreamWriter fp, string sep)
        {
            texts.Clear();
            foreach (var th in ths.Take(2))
            {
                string text = trim(th);
 

                texts.Add(text);

                var as_ = th.Descendants("a").ToList();
                if (as_.Count > 0)
                {
                    var a = as_[0];
                    var h = a.GetAttributeValue("href", "");
                    texts.Add(h);
                }
                else
                {
                    texts.Add("nolink");
                }
            }

            foreach (string text in texts)
            {
                fp.Write(text);
                fp.Write(sep);
            }

            if (texts.TrueForAll(s => s.Length < 1))
            {
                // do nothing
            }
            else
            {
            }
        }

        private static string trim(HAP.HtmlNode th)
        {
            return th.InnerText.Replace("\t", " ").Replace("\r","").Replace("\n","").Trim();
        }

        static string get_career_page(int n)
        {
            
            string url =
                "http://www.nhl.com/ice/careerstats.htm?fetchKey=00002ALLSAHAll&viewName=careerLeadersAllSeasons&sort=goals&pg=" + n;
            return url;

        }


    }
}
