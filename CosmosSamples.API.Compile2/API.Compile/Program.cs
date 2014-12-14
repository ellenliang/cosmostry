using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CosmosSamples.API.Compile2
{
    class Program
    {
        static void Main(string[] args)
        {
            // Verify the CPP SDK is there
            System.Environment.SetEnvironmentVariable("SCOPE_CPP_SDK", "d:\\CppSdk_12");
            string cppsdk = System.Environment.GetEnvironmentVariable("SCOPE_CPP_SDK");

            if (cppsdk == null)
            {
                throw new System.ArgumentException("SCOPE_CPP_SDK environment variable not defined");
            }

            cppsdk = cppsdk.Trim();

            if (!System.IO.Directory.Exists(cppsdk))
            {
                throw new System.ArgumentException("SCOPE_CPP_SDK does not point to a directory that exists");
            }

            ScopeClient.Scope.RunStepsInSeparateProcess = true;

            System.Console.WriteLine("Scope WorkingRoot: {0}", ScopeClient.ScopeEnvironment.Instance.WorkingRoot);
            System.Console.WriteLine("Scope Path: {0}", ScopeClient.ScopeEnvironment.Instance.ScopePath);
            System.Console.WriteLine("Scope InputStreamPath: {0}", ScopeClient.ScopeEnvironment.Instance.InputStreamPath);
            System.Console.WriteLine("Scope OutputStreamPath: {0}", ScopeClient.ScopeEnvironment.Instance.OutputStreamPath);
            System.Console.WriteLine("Scope TmpDirectory: {0}", ScopeClient.ScopeEnvironment.Instance.TmpDirectory);

            ScopeClient.Scope.Run(@"test.script");
        }
    }
}
