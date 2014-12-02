using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading.Tasks;
using VcClient;
using HAP = HtmlAgilityPack;

// http://dumps.wikimedia.org/other/pagecounts-raw/

namespace UploadWikipediaPageCountsToCosmos
{
    public class Link
    {
        public string Text;
        public string Url;

        public Link(string text, string url)
        {
            this.Text = text;
            this.Url = url;
        }
    }
    class Program
    {
        static void Main(string[] args)
        {

            string temp_folder = System.IO.Path.Combine(System.IO.Path.GetTempPath(),"WikipediaPageCounts");

            if (System.IO.Directory.Exists(temp_folder))
            {
                System.IO.Directory.Delete(temp_folder, true);
            }

            System.IO.Directory.CreateDirectory(temp_folder);


            string main_index = @"http://dumps.wikimedia.org/other/pagecounts-raw/";

            var wc = new System.Net.WebClient();

            var year_hrefs = GetHrefs(wc, main_index).Where( i => i.Text == i.Url && i.Text.Length==4 ).ToList();
            year_hrefs = year_hrefs.Select(i => new Link(i.Text, main_index + i.Url)).ToList();

            var errlog = System.IO.File.CreateText("D:\\errlog.txt");
            foreach (var year_href in year_hrefs)
            {

                var month_hrefs = GetHrefs(wc, year_href.Url);
                month_hrefs = month_hrefs.Where(i => i.Text == i.Url && i.Text.Length == 4+1+2).ToList();
                month_hrefs = month_hrefs.Select(i => new Link(i.Text, year_href.Url + @"/" + i.Url)).ToList();

                foreach (var month_href in month_hrefs)
                {
                    var day_hrefs = GetHrefs(wc, month_href.Url);
                    day_hrefs = day_hrefs.Where(i => i.Url.EndsWith(".gz")).ToList();
                    day_hrefs = day_hrefs.Where(i => i.Url.Contains("pagecounts")).ToList();


                    foreach (var day_href in day_hrefs)
                    {

                        string local_gzip = System.IO.Path.Combine(temp_folder, day_href.Url);
                        string remote_gzip = month_href.Url + "/" +day_href.Url;

                        string fn_a = System.IO.Path.GetFileNameWithoutExtension(local_gzip);
                        System.Console.WriteLine(remote_gzip);
                        string fn_year = fn_a.Substring(11, 4);
                        string fn_month = fn_a.Substring(15, 2);
                        string fn_day = fn_a.Substring(17, 2);

                        string cosmos_path =
                            string.Format(@"vc://cosmos08/sandbox/my/WikipediaPageCounts/{0}/{1}/{2}/{3}.txt",
                                fn_year, fn_month, fn_day, fn_a);

                        var files = System.IO.Directory.GetFiles(temp_folder);
                        foreach (var file in files)
                        {
                            System.IO.File.Delete(file);
                        }


                        if (!VcClient.VC.StreamExists(cosmos_path))
                        {
                            try
                            {
                                System.Console.WriteLine("Downloading");
                                wc.DownloadFile(remote_gzip, local_gzip);
                            }
                            catch (Exception)
                            {
                                Console.WriteLine("Failed to download. Skipped {0}", fn_a);
                                errlog.WriteLine("Failed to download. Skipped {0}", fn_a);
                                errlog.Flush();
                                continue;
                            }

                            var fi = new System.IO.FileInfo(local_gzip);
                            System.Console.WriteLine("Decompressing");
                            string outfile = Decompress(fi);

                            try
                            {
                                System.Console.WriteLine("Uploading");
                                VcClient.VC.Upload(outfile, cosmos_path, true);
                            }
                            catch (VcClientExceptions.FileUploadException)
                            {
                                Console.WriteLine("Failed to download. Skipped {0}", fn_a);
                                errlog.WriteLine("Failed to download. Skipped {0}", fn_a);
                                errlog.Flush();
                                continue;
                            }                           
                        }
                        else
                        {
                            Console.WriteLine("Already exists");
                            
                        }
                    }
                }
            }
        }

        private static List<Link> GetHrefs(WebClient wc, string index)
        {
            var html = wc.DownloadString(index);
            var doc = new HAP.HtmlDocument();
            doc.LoadHtml(html);

            var ahrefs = doc.DocumentNode.Descendants("a").ToList();
            ahrefs = ahrefs.Where(el => el.Attributes["href"] != null).ToList();
            var a = ahrefs.Select(el => new Link(el.InnerText.Trim(), el.Attributes["href"].Value)).ToList();
            return a;
        }

        public static string Decompress(System.IO.FileInfo fi)
        {
            // Get the stream of the source file.
            using (var inFile = fi.OpenRead())
            {
                // Get original file extension, for example
                // "doc" from report.doc.gz.
                string curFile = fi.FullName;
                string origName = curFile.Remove(curFile.Length -
                        fi.Extension.Length);

                //Create the decompressed file.
                using (var outFile = System.IO.File.Create(origName))
                {
                    using (var Decompress = new System.IO.Compression.GZipStream(inFile,
                            System.IO.Compression.CompressionMode.Decompress))
                    {
                        // Copy the decompression stream 
                        // into the output file.
                        Decompress.CopyTo(outFile);
                        return origName;
                    }
                }
            }
        }
    }
}
