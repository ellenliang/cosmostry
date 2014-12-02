using System.Collections.Generic;
using System.Linq;
using ScopeRuntime;

namespace RowsetLib
{
    public static class Helper
    {
        public static string GetCommaSeparatedColumnNames(List<ColumnInfo> output_cols)
        {
            return string.Join(",", output_cols.Select(c => c.Name));
        }
    }
}