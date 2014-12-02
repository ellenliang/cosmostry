using CsvToCosmosSStream;
using ScopeRuntime;

public class ___Scope_Generated_Classes___
{

    #region ==BEGIN READ BY SCOPERUNTIME.METHODS.DOALL==

    public static int __RuntimeVersion__ = 0;
    public static int __WriteSStreamVersion__ = 3;

    public static System.Type[] __UdtTypeTable__ = { };

    public static class __OperatorFactory__
    {
        public static ScopeRuntime.Extractor Create_Extract_0()
        {
            return new ScopeRuntime.DefaultTextExtractor();
        }
        public static ScopeRuntime.SStreamOutputter Create_SStreamOutput_1()
        {
            return new ___Scope_Generated_Classes___.ScopeSStreamOutput1();
        }
    }

    #endregion ==END READ BY SCOPERUNTIME.METHODS.DOALL==

    #region ==BEGIN USER CODE==
    #endregion ==END USER CODE==

    public class ScopeSStreamOutput1 : SStreamOutputter
    {
        CsvRow csvRow = new CsvRow();
        public ScopeSStreamOutput1()
        {
            _isSerial = true;
        }
        public override void OutputRow(Row row)
        {
            csvRow.CopyFrom(row);
            _writers[0].AppendRow(csvRow);

        }
    }

}

static public class UDTExtensionWrapper
{
}