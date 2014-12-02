using System;
using System.Collections.Generic;

namespace DynamicViewUtils
{
    public class DateExpansion : Expansion
    {
        public static string Prefix = "daterange";

        public DateExpansion(string name, string definition) :
            base(name)
        {
            this.VirtualColumnType = "DateTime";

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
            var min = System.DateTime.Parse(tokens[0]);
            var max = System.DateTime.Parse(tokens[1]);
            if (min > max)
            {
                throw new System.ArgumentException();
            }

            var cur_day = min;
            while (cur_day <= max)
            {
                var item = new DateTimeExpandedValue(cur_day);
                this.Items.Add(item);

                cur_day = cur_day.AddDays(1);
            }
        }

        public override string Replace(string s, ExpandedValue v)
        {
            if (!(v is DateTimeExpandedValue))
            {
                throw new ArgumentException();
            }

            var dv = v as DateTimeExpandedValue;

            string day_s = string.Format("<{0}.Day>", this.Name);
            string month_s = string.Format("<{0}.Month>", this.Name);
            string year_s = string.Format("<{0}.Year>", this.Name);

            var dt = dv.Value;

            string t = s;
            t = s.Replace( day_s     , dt.Day.ToString("00"));
            t = t.Replace( month_s   , dt.Month.ToString("00"));
            t = t.Replace( year_s    , dt.Year.ToString("0000"));
            return t;
        }

        public override string GetVirtualColumn(ExpandedValue value)
        {
            string s = string.Format(@" (System.DateTime.Parse(""{0}"")) AS {1} ", value.GetText(), this.Name);
            return s;
        }
    }
}