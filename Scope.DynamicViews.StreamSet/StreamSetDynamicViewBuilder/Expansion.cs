using System;
using System.Collections.Generic;

namespace DynamicViewUtils
{
    public class Expansion
    {
        public string Name;
        public string ViewParameterType="string";
        public string VirtualColumnType = "string";
        public List<ExpandedValue> Items;

        public Expansion(string name)
        {
            this.Name = name;
            this.Items = new List<ExpandedValue>();
        }

        private string ReplacementString
        {
            get { return "<" + this.Name + ">"; }
        }

        public virtual string Replace(string s, ExpandedValue ev)
        {
            return s.Replace(this.ReplacementString, ev.GetText());
        }

        public string RemovePrefix(string definition)
        {
            // All expansion definitions begin with some identifying prefix and then a definition string like "foo:bar"
            // this function removes the prefix

            var tokens = definition.Split(new char[] {':'}, 2);
            //string pre = tokens[0].Trim();
            string post = tokens[1].Trim();
            return post;
        }

        public virtual string GetVirtualColumn(ExpandedValue value)
        {
            throw new NotImplementedException();
        }
    }
}