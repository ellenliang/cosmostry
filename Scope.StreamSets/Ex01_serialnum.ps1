data1 =
    EXTRACT Name:string, Value:string
    FROM
        STREAMSET @"/my/CosmosSamples/StreamSets/Inputs_01"
        PATTERN @"input_%n.txt"
        RANGE __serialnum=["8", "12"]
   USING DefaultTextExtractor();

data2 = 
    EXTRACT Name:string, Value:string
    FROM @"/my/CosmosSamples/StreamSets/Inputs_01/input_%n.txt?serialnum=8...12"
    USING DefaultTextExtractor();

OUTPUT data1 TO @"/my/CosmosSamples/StreamSets/Outputs/Output1.tsv" USING DefaultTextOutputter();
OUTPUT data2 TO @"/my/CosmosSamples/StreamSets/Outputs/Output2.tsv" USING DefaultTextOutputter();

