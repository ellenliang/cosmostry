namespace DynamicViewUtils
{
    public class DateTimeExpandedValue : ExpandedValue
    {
        public System.DateTime Value;

        public DateTimeExpandedValue(System.DateTime value) 
        {
            this.Value  = value;
        }

        public override string GetText()
        {
            return this.Value.ToString();
        }
    }
}