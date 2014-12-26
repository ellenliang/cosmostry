using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using HAP=HtmlAgilityPack;

namespace ScrapeNHLCareerStats
{
    class Program
    {
        static void Main(string[] args)
        {


            clean();
        }

        private static void clean()
        {
            var lines = System.IO.File.ReadLines(@"d:\playerdata_playoffs.tsv").ToList();

            var dp = System.IO.File.CreateText(@"d:\playerdata_playoffs_final.tsv");
            foreach (var line in lines)
            {
                if (line.Contains("SEASON") && line.Contains("TEAM"))
                {
                    continue;
                }
                if (line.Contains("NHL TOTALS"))
                {
                    continue;
                }


                string xline = line.Replace("\t", "|");

                dp.WriteLine(xline);
                dp.Flush();
            }
            dp.Close();
        }

        static void CollectPlayerData()
        {
            string sep = "\t";

            var fp = System.IO.File.CreateText(@"d:\playerdata_regseason.tsv");

            var files = System.IO.Directory.GetFiles(@"D:\playerpages", "*.htm");
            int i = 0;
            foreach (string file in files)
            {
                Console.WriteLine("{0} {1}", i, file);

                string playerid = System.IO.Path.GetFileNameWithoutExtension(file);
                string playerpage = System.IO.File.ReadAllText(file);

                var doc = new HAP.HtmlDocument();
                doc.Load(file);

                var tables = HAPUTIL.FindAllTables(doc);

                var target_table = tables[tables.Count-2];
                
                var row_nodes = HAPUTIL.FindRowsInTable(target_table);
                var texts = new List<string>();
                
                var context = new List<string>();
                foreach (var tr in row_nodes)
                {
                    context.Clear();
                    context.Add(playerid);
                    context.Add("REGSEASON");
                    
                    List<HAP.HtmlNode> nodes;
                    nodes = tr.Descendants("th").ToList();
                    if (nodes.Count < 1)
                    {
                        nodes = tr.Descendants("td").ToList();
                    }


                    HandleRow(texts, nodes, fp, sep,context);                       

                    fp.WriteLine();
                }

                i++;
            }

            fp.Close();

        }


        private static void HandleRow(List<string> texts, List<HAP.HtmlNode> ths, StreamWriter fp, string sep, IList<string> context_cells)
        {
            texts.Clear();
            if (context_cells!= null)
            {
                texts.AddRange(context_cells);
            }

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

    public static class HAPUTIL
    {
        public static List<HAP.HtmlNode> FindAllTables(HAP.HtmlDocument doc)
        {
            var tables = doc.DocumentNode.Descendants("table").ToList();
            return tables;
        }

        public static List<HAP.HtmlNode> FindRowsInTable(HAP.HtmlNode t4)
        {
            var trs = t4.Descendants("tr").ToList();
            return trs;
        }
    }
}
