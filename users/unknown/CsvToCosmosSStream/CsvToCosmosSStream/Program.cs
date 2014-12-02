using Microsoft.Cosmos.ScopeStudio.BusinessObjects.Debugger;
using System;
using System.IO;
using VcClient;

namespace RunItLocal.script
{
    class SampleConverter
    {
        private readonly static string FromCsvFile = Directory.GetCurrentDirectory() + "\\original.csv";
        private readonly static string ToSStreamFile = Directory.GetCurrentDirectory() + "\\new.ss";
        private readonly static string WorkingFolder = Directory.GetCurrentDirectory();
        private readonly static string TempFolder = Directory.GetCurrentDirectory();

        private readonly static string VcName = "http://cosmos05.osdinfra.net:88/cosmos/MicrosoftStores.test";
        private readonly static string SStreamFileOnCosmos = "/my/new.ss";

        static void Main(string[] args)
        {
            // transform csv file to structured stream
            SampleConverter.Step1();
            SampleConverter.Step2();
            SampleConverter.Step3();

            // upload the structured stream to VC
            VC.Setup(VcName, null, null);
            if (VC.StreamExists(SStreamFileOnCosmos))
            {
                VC.Delete(SStreamFileOnCosmos);
            }
            VC.Upload(ToSStreamFile, SStreamFileOnCosmos, false, TimeSpan.MaxValue);
        }

        public static void Step1()
        {
            ScopeDebugRunStepHelper.RunStep("-scopeVertex SV1_Extract_Split -i " + FromCsvFile + " -o " + TempFolder + "\\scopetmpfile._SV1_Extract_Split_out0 -o " + TempFolder + "\\scopetmpfile._SV1_Extract_Split_out1", WorkingFolder, WorkingFolder, true);
        }

        public static void Step2()
        {
            ScopeDebugRunStepHelper.RunStep("-scopeVertex SV2_Aggregate -i " + TempFolder + "\\scopetmpfile._SV1_Extract_Split_out1 -ichannel SV1_Extract_Split_out1 -o " + TempFolder + "\\scopetmpfile._SV2_Aggregate_out0", WorkingFolder, WorkingFolder, true);
        }

        public static void Step3()
        {
            ScopeDebugRunStepHelper.RunStep("-concatenate -i " + TempFolder + "\\scopetmpfile._SV1_Extract_Split_out0 -ichannel SV1_Extract_Split_out0 -i " + TempFolder + "\\scopetmpfile._SV2_Aggregate_out0 -ichannel SV2_Aggregate_out0 -o " + ToSStreamFile, WorkingFolder, WorkingFolder, true);
        }
    }
}
