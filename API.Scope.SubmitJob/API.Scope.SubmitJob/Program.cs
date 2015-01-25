using System;

namespace API.Scope.SubmitJob
{
    using VcClient;

    class Program
    {
        static void Main(string[] args)
        {


            string script = @"
rs0 = 
    EXTRACT
        name:string,
        num:int
    FROM @""/my/input.tsv""
    USING DefaultTextExtractor();

rs1 =
    SELECT name, SUM(num) AS total
    FROM rs0;

OUTPUT rs1 
TO @""/my/output.tsv""
USING DefaultTextOutputter();
";
            string vc = "vc://cosmos08/sandbox";
            string temp_folder = System.IO.Path.GetTempPath();
            string data_filename = System.IO.Path.Combine(temp_folder, "input.tsv");
            string remote_path = "/my/input.tsv";
            string script_filename = System.IO.Path.Combine(temp_folder, "test.script");

            VcClient.VC.Setup(vc, VcClient.VC.NoProxy, null);

            // First Create a stream we can yse
            UploadInputStream(data_filename, remote_path);

            // Create a Script and submit it
            System.IO.File.WriteAllText(script_filename, script);
            var subParams = new ScopeClient.SubmitParameters(script_filename);
            var jobinfo = ScopeClient.Scope.Submit(subParams);

            // Wait
            WaitUntilJobFinished(jobinfo);

            // At this point the job is finished

            System.Console.WriteLine("Press any key to continue");
            System.Console.ReadKey();
        }

        private static void WaitUntilJobFinished(JobInfo jobinfo)
        {
            // The submission is done. Now we wait until the job is done
            bool use_compression = true;
            int seconds_to_sleep = 5;
            var wait_time = new System.TimeSpan(0, 0, 0, seconds_to_sleep);
            while (true)
            {
                jobinfo = VcClient.VC.GetJobInfo(jobinfo.ID, use_compression);
                Console.WriteLine("Job State = {0}", jobinfo.State);
                if (jobinfo.State == VcClient.JobInfo.JobState.Cancelled || jobinfo.State == VcClient.JobInfo.JobState.Completed
                    || jobinfo.State == VcClient.JobInfo.JobState.CompletedFailure
                    || jobinfo.State == VcClient.JobInfo.JobState.CompletedSuccess)
                {
                    Console.WriteLine("Job Stopped Running");
                    break;
                }

                System.Threading.Thread.Sleep(wait_time);
            }
        }

        private static bool UploadInputStream(string data_filename, string remote_path)
        {
            bool use_compression = true;

            string data = @"FOO,1
BAR,3
FOO,7
BAR,10
FOO,1
BAR,9";

            data = data.Trim().Replace(",", "\t");
            System.IO.File.WriteAllText(data_filename, data);

            if (VcClient.VC.StreamExists(remote_path))
            {
                VcClient.VC.Delete(remote_path);
            }
            VcClient.VC.Upload(data_filename, remote_path, use_compression);
            return use_compression;
        }
    }
}


