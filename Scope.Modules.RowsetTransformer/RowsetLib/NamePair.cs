using System.Collections.Generic;

namespace RowsetLib
{
    public class NamePair
    {
        public string First;
        public string Second;

        public NamePair(string first, string second)
        {
            this.First = first;
            this.Second = second;
        }

        public static List<NamePair> Parse(IList<string> texts)
        {
            var pairs = new List<NamePair>(texts.Count);
            foreach (string text in texts)
            {
                var pair = Parse(text);
                pairs.Add(pair);
            }
            return pairs;
        }

        private static NamePair Parse(string text)
        {
            var tokens = text.Split('>');
            if (tokens.Length != 2)
            {
                string msg = string.Format("Incorrect format {0}", text);
                throw new System.ArgumentException(msg);
            }

            string old_name = tokens[0].Trim();
            string new_name = tokens[1].Trim();

            var pair = new NamePair(old_name, new_name);
            return pair;
        }
    }
}