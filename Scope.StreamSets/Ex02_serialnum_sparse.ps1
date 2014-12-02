data1 =
    EXTRACT Name:string, Value:string
    FROM
        SPARSE STREAMSET @"/my/CosmosSamples/StreamSets/Inputs_01_sparse"
        PATTERN @"input_%n.txt"
        RANGE __serialnum=["8", "12"]
   USING DefaultTextExtractor();

data2 = 
    EXTRACT Name:string, Value:string
    FROM @"/my/CosmosSamples/StreamSets/Inputs_01_sparse/input_%n.txt?serialnum=8...12&sparsestreamset=true"
    USING DefaultTextExtractor();

OUTPUT data1 TO @"/my/CosmosSamples/StreamSets/Outputs/Output1.tsv" USING DefaultTextOutputter();
OUTPUT data2 TO @"/my/CosmosSamples/StreamSets/Outputs/Output2.tsv" USING DefaultTextOutputter();

