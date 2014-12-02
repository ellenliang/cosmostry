using System.Collections.Generic;

namespace RowsetLib
{
    public class Keep : ColumnFilter
    {
        public override string Resolve(string name, IEnumerable<Parameter> parameters, string[] args)
        {
            var filter_action = TextUtil.FilterAction.Include;
            return FilterColumns(name, parameters, filter_action);
        }
    }
}
