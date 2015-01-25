using System.Collections.Generic;

namespace DemoResolvers
{
    public class ColorsViewResolver : ScopeRuntime.ViewResolver
    {
        public override string Resolve(string path, IDictionary<string, object> parameters, string[] args)
        {
            string err_msg = "Must provide either \"typed\" or \"untyped\" as argument to View Resolver";

            if (args.Length == 0)
            {
                throw new System.ArgumentException(err_msg);
            }

            if (args[0] == "typed")
            {

                return @"
        CREATE VIEW Colors 
            SCHEMA 
                (Color:string, 
                HexCode:uint, 
                RGB:string, 
                Issued:String, 
                Retired:string, 
                Notes:string) 
        PARAMS(
            Input string)
        AS BEGIN
            colors = EXTRACT Color:string, 
                HexCode:string, 
                RGB:string, 
                Issued:String, 
                Retired:string, 
                Notes:string
            FROM @Input USING DefaultTextExtractor();

            colors2 = SELECT 
                            Color, 
                            Convert.ToUInt32(HexCode.Substring(1), 16) AS HexCode,
                            RGB,
                            Issued,
                            Retired,
                            Notes
                FROM colors;
        END;                
        ";
            }
            else if (args[0] == "untyped")
            {
                return @"
        CREATE VIEW Colors 
            SCHEMA 
                (Color:string, 
                HexCode:string, 
                RGB:string, 
                Issued:String, 
                Retired:string, 
                Notes:string) 
        PARAMS(
            Input string)
        AS BEGIN
            EXTRACT Color:string, 
                HexCode:string, 
                RGB:string, 
                Issued:String, 
                Retired:string, 
                Notes:string
            FROM @Input USING DefaultTextExtractor();
        END;                
        ";

            }
            else
            {
                throw new System.ArgumentException(err_msg);
            }

        }
    }
}
