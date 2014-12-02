using System;using System.Collections.Generic;
using System.IO;
using System.Text;
using ScopeRuntime;
using System.Linq;

public class DynamicStreamSetResolver : ScopeRuntime.ViewResolver
{
    public override string Resolve(string path, IDictionary<string, object> parameters, string[] args)
    {
        var viewbuilder = new DynamicViewUtils.StreamSetBuilder(parameters);
        return viewbuilder.GetViewDefinition();
    }
}
