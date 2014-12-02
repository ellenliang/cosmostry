using ScopeRuntime;
using System;
using System.IO;

namespace CsvToCosmosSStream
{
    public class CsvRow : Row
    {
        public static System.Tuple<string, ColumnDataType, Type, Type>[] _columnTable = 
        {
            new System.Tuple<string,ColumnDataType,Type,Type>("f1", ColumnDataType.String, typeof(string), typeof(StringColumnData)),
            new System.Tuple<string,ColumnDataType,Type,Type>("f2", ColumnDataType.String, typeof(string), typeof(StringColumnData)),
        };

        public static string SchemaString = "f1:string,f2:string";

        public CsvRow(Schema schema, ColumnData[] columns) : base(schema, columns) { }
        public CsvRow(Schema schema)
            : base(schema)
        {
            for (int i = 0; i < _columns.Length; ++i)
            {
                this[i] = _columns[i];
            }
        }
        public CsvRow()
            : this(new Schema(SchemaString))
        {

        }

        private static Schema BuildSchema()
        {
            Schema schema = new Schema();
            for (int i = 0; i < _columnTable.Length; ++i)
            {
                schema.Add(_columnTable[i].Item2 != ColumnDataType.UDT ? new ScopeRuntime.ColumnInfo(_columnTable[i].Item1, _columnTable[i].Item2) : new ScopeRuntime.ColumnInfo(_columnTable[i].Item1, _columnTable[i].Item3));
            }
            return schema;
        }
        private static ColumnData[] BuildColumnData()
        {
            ColumnData[] columnData = new ColumnData[_columnTable.Length];
            for (int i = 0; i < _columnTable.Length; ++i)
            {
                columnData[i] = _columnTable[i].Item2 != ColumnDataType.UDT ? ColumnDataFactory.Create(_columnTable[i].Item2) : (ColumnData)Activator.CreateInstance(_columnTable[i].Item4);
            }
            return columnData;
        }
        public override ColumnData this[int index]
        {
            get { return _columns[index]; }
            set
            {
                _columns[index] = value;
            }
        }
        public override void Serialize(BinaryWriter writer)
        {
            writer.Write((byte)0);
            for (int i = 0; i < _columns.Length; i++)
            {
                _columns[i].Serialize(writer);
            }
        }

        public override void Deserialize(BinaryReader reader)
        {
            byte firstByte = reader.ReadByte();
            if (firstByte == 0) // can not be null in our sample
            {
                for (int i = 0; i < _columns.Length; i++)
                {
                    _columns[i].Deserialize(reader);
                }
            }
        }

        internal void CopyFrom(Row from)
        {
            for (int i = 0; i < _columns.Length; i++)
            {
                this[i] = from[i];
            }
        }
    }
}
