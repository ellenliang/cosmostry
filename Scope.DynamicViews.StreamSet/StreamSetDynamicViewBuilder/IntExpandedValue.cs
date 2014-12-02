namespace DynamicViewUtils
{
    public class IntExpandedValue : ExpandedValue
    {
        public int Value;

        public IntExpandedValue(int value) 
        {
            this.Value = value;
        }

        public override string GetText()
        {
            return this.Value.ToString(System.Globalization.CultureInfo.InvariantCulture);
        }
    }
}