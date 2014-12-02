namespace DynamicViewUtils
{
    public class StringExpandedValue : ExpandedValue
    {
        public string Value;

        public StringExpandedValue(string value) 
        {
            this.Value = value;
        }

        public override string GetText()
        {
            return this.Value;
        }
    }
}