namespace GetLineInformation
{
    class Program
    {
        static void Main(string[] args)
        {
            var start = System.DateTime.Now;

            int line_with_max = 0;
            int max_chars = 0;
            int num_lines = 0;
            string max_line =string.Empty;
            int max_bytes = 0;
            string line;
            string filename = args[0];
            using (var file = new System.IO.StreamReader(filename))
            {
                while ((line = file.ReadLine()) != null)
                {
                    if (line.Length > max_chars)
                    {
                        max_chars = line.Length;
                        max_line = line;
                        max_bytes = System.Text.Encoding.UTF8.GetBytes(max_line).Length;
                        line_with_max = num_lines;
                    }
                    num_lines++;
                }

                file.Close();
            }

            var end = System.DateTime.Now;
            var duration = end - start;

            System.Console.WriteLine("Total time in seconds: {0}", duration.TotalSeconds);
            System.Console.WriteLine("Total lines: {0}", num_lines);
            System.Console.WriteLine("Max chars in line: {0}", max_chars);
            System.Console.WriteLine("Max bytes in line: {0}", max_bytes);
            System.Console.WriteLine("line number: {0}", line_with_max);
        }
    }
}
