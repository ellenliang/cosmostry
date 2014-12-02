using System.Collections.Generic;

namespace DynamicViewUtils
{
    public class EnumExpansion : Expansion
    {
        public char Separator;

        public static string Prefix = "enum";

        public EnumExpansion(string name, string definition) :
            base(name)
        {

            this.VirtualColumnType = "string";
            definition = RemovePrefix(definition);

            this.Separator = ';';
            var separators = new[] { this.Separator };
            var tokens = definition.Split(separators);
            foreach (string token in tokens)
            {
                var item = new StringExpandedValue(token);
                this.Items.Add(item);
            }
        }

        public override string GetVirtualColumn(ExpandedValue value)
        {
            string s = string.Format(@" {0} AS {1} ", string.Format("\"{0}\"", value.GetText()), this.Name);
            return s;
        }

    }
}