using System.Collections.Generic;

namespace DynamicViewUtils
{
    public class IntExpansion : Expansion
    {
        public static string Prefix = "intrange";

        public IntExpansion(string name, string definition) :
            base(name)
        {
            this.VirtualColumnType = "int";
            if (!definition.StartsWith(Prefix))
            {
                throw new System.ArgumentException("Does not start with prefix");
            }

            definition = RemovePrefix(definition);

            if (!definition.Contains("..."))
            {
                throw new System.ArgumentException();                
            }

            var separators = new[] { "..." };
            var tokens = definition.Split(separators, System.StringSplitOptions.None);
            if (tokens.Length != 2)
            {
                throw new System.ArgumentException();
            }
            int min = int.Parse(tokens[0]);
            int max = int.Parse(tokens[1]);
            if (min > max)
            {
                throw new System.ArgumentException();                    
            }
            if (min < 0)
            {
                throw new System.ArgumentException();                                        
            }

            for (int j = min; j <= max; j++)
            {
                var item = new IntExpandedValue(j);
                this.Items.Add(item);
            }
        }

        public override string GetVirtualColumn(ExpandedValue value)
        {
            string s = string.Format(@" ({0}) AS {1} ", value.GetText(), this.Name);
            return s;
        }
    }
}