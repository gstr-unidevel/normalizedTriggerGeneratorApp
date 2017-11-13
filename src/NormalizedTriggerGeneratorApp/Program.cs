using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace NormalizedTriggerGeneratorApp
{
    class Program
    {
        static void Main(string[] args)
        {
            var program = Run();
            program.Wait();

            var result = program.Result;
            File.WriteAllText("out.sql", result);

            Console.WriteLine(result);
            Console.ReadLine();
        }

        const string retrieveStructureSql = "SELECT sys.schemas.name AS SchemaName, tables.name AS TableName, all_columns.name AS ColumnName, sys.types.name AS TypeName, all_columns.max_length as MaxLength, all_columns.precision AS Precision, all_columns.scale as Scale, all_columns.is_nullable as IsNullable, all_columns.is_identity as IsIdentity FROM sys.all_columns JOIN sys.tables on sys.tables.object_id = sys.all_columns.object_id JOIN sys.types on sys.types.system_type_id = sys.all_columns.system_type_id JOIN sys.schemas on sys.tables.schema_id = sys.schemas.schema_id WHERE sys.types.name NOT IN ('sysname') AND not tables.name like '%Audit' ORDER BY tables.name, all_columns.is_identity DESC, all_columns.name";
        const string connectionString = "data source=(local); initial catalog=tdc; integrated security=true";

        static async Task<string> Run()
        {
            var tableColumns = await getTableColumns();
            var tables = from tc in tableColumns
                         group tc by new { SchemaName = tc.SchemaName, TableName = tc.TableName } into t
                         select new Table() { SchemaName = t.Key.SchemaName, TableName = t.Key.TableName, Columns = t.ToArray(), HasImageColumns = t.Max(p=>p.IsImage) };

            var tablesToGet = new List<string>();

            tablesToGet.AddRange(new string[] { "UserAccount", "Driver", "DriverTag", "Limit", "LimitTracking", "Zone", "Tracking", "Shape", "TokenPosession" });
            tablesToGet.AddRange(new string[] { "Task", "TaskGroup", "TaskGroupTemplate", "TaskGroupTemplateNotificationTrigger", "TaskTemplate" });

            tables = from t in tables where tablesToGet.Contains(t.TableName) select t;

            var b = new StringBuilder();

            foreach (var table in tables)
            {
                validateTable(table);
                b.AppendLine(generateDropAuditTriggers(table));

                //if (table.HasImageColumns)
                //{
                //    foreach (var ic in table.Columns.Where(p=>p.IsImage && (!p.IsNullable)).Select(p =>p))
                //    {
                //        b.AppendLine($"ALTER TABLE [{ic.SchemaName}].[{ic.TableName}Audit] ALTER COLUMN [{ic.ColumnName}] {getSqlTypeName(ic)} NULL");
                //    }
                //    b.AppendLine($"GO");
                //    b.AppendLine($"");

                //    b.AppendLine($"DROP INDEX IF EXISTS [IX_{table.TableName}Audit_{table.TableName}Id] ON [{table.SchemaName}].[{table.TableName}]");
                //    b.AppendLine($"GO");
                //}

                b.AppendLine(generateDropAuditTable(table));
                b.AppendLine(generateAuditTable(table));
                b.AppendLine(generateAuditTriggers(table));
            }            

            return b.ToString();
        }

        private static void validateTable(Table table)
        {
            if (table.HasImageColumns)
            {
                var identityColumns = table.Columns.Where(p => p.IsIdentity).Select(p => p).ToArray();
                if (identityColumns.Length == 0) throw new InvalidOperationException($"Table '{table.TableName}' has image columns and doesn't have identity column.");
                if (identityColumns.Length > 1) throw new InvalidOperationException($"Table '{table.TableName}' has image columns and has identity on several columns (?).");

                if (String.Compare(identityColumns[0].ColumnName, $"{table.TableName}Id", true)!=0) throw new InvalidOperationException($"Table '{table.TableName}' has image column which is not '{table.TableName}Id'.");
                if ((String.Compare(identityColumns[0].TypeName, "int", true) != 0) && (String.Compare(identityColumns[0].TypeName, "bigint", true) != 0) && (String.Compare(identityColumns[0].TypeName, "smallint", true) != 0)) throw new InvalidOperationException($"Table '{table.TableName}' has image column which is not '{table.TableName}Id'.");
            }
        }

        private static string generateAuditTable(Table table)
        {
            var b = new StringBuilder();

            b.AppendLine($"CREATE TABLE [{table.SchemaName}].[{table.TableName}Audit]");
            b.AppendLine($"(");
            b.AppendLine($"   [{table.TableName}AuditId] [bigint] IDENTITY(1,1) NOT NULL,");
            b.AppendLine($"   [AuditType] [char](1) NOT NULL,");
            b.AppendLine($"   [AuditOn] [datetime] NOT NULL,");
            b.AppendLine($"   [AuditBy] [nvarchar] (50) NOT NULL,");

            foreach (var column in table.Columns)
            {
                b.AppendLine($"   [{column.ColumnName}] {getSqlTypeName(column)} {(column.IsNullable || column.IsImage ? "NULL" : "NOT NULL")},");
            }

            b.AppendLine($"   CONSTRAINT[PK_{table.TableName}Audit] PRIMARY KEY CLUSTERED([{table.TableName}AuditId] ASC)");
            b.AppendLine($")");
            b.AppendLine($"GO");
            b.AppendLine($"ALTER TABLE [{table.SchemaName}].[{table.TableName}Audit] ADD CONSTRAINT [DF_{table.TableName}Audit_AuditOn] DEFAULT(getutcdate()) FOR [AuditOn]");
            b.AppendLine($"GO");
            b.AppendLine($"ALTER TABLE [{table.SchemaName}].[{table.TableName}Audit] ADD CONSTRAINT [DF_{table.TableName}Audit_AuditBy] DEFAULT(suser_sname()) FOR[AuditBy]");
            b.AppendLine($"GO");

            if (table.HasImageColumns)
            {
                b.AppendLine($"CREATE NONCLUSTERED INDEX[IX_{table.TableName}Audit_{table.TableName}ID] ON [{table.SchemaName}].[{table.TableName}Audit]([{table.TableName}Id] ASC)");
                b.AppendLine($"GO");
            }

            b.AppendLine($"");

            return b.ToString();
        }

        private static string getSqlTypeName(TableColumn column)
        {
            switch (column.TypeName.ToLower())
            {
                case "varchar":
                case "nvarchar":
                case "varbinary":
                    return $"[{column.TypeName}]({(column.MaxLength == -1 ? "max" : column.MaxLength.ToString())})";
                case "float":
                    if ((column.Precision == 53) && (column.Scale == 0))
                        return "float";
                    else
                        return $"[{column.TypeName}]({column.Precision},{column.Scale})";
                case "double":
                    return $"[{column.TypeName}]({column.Precision},{column.Scale})";
                case "decimal":
                case "numeric":
                    if ((column.Precision == 18) && (column.Scale == 0))
                        return $"[{column.TypeName}]";
                    else
                        return $"[{column.TypeName}]({column.Precision},{column.Scale})";
                default:
                    return $"[{column.TypeName}]";
            }
        }

        private static readonly string[] imageColumnTypes = new string[] { "ntext", "text", "image" };

        private static bool isColumnImage(string columnTypeName)
        {
            return imageColumnTypes.Contains(columnTypeName.ToLower());
        }

        private static string getColumnConvertType(TableColumn column)
        {
            if (!isColumnImage(column.TypeName)) throw new ArgumentException("Image column must be provided.", nameof(column));

            switch (column.TypeName.ToLower())
            {
                case "ntext": return "nvarchar(max)";
                case "text": return "varchar(max)";
                case "image": return "varbinary(max)";
                default:
                    throw new NotSupportedException($"Column type '{column.TypeName}' is not supported.");
            }
        }

        private static string generateAuditTriggers(Table table)
        {
            var b = new StringBuilder();

            var columnNames = table.Columns.Select(p => $"   [{p.ColumnName}]").ToArray();
            var columnNamesBigIndent = table.Columns.Select(p => $"     [{p.ColumnName}]").ToArray();

            var columnNamesAsInserted = table.Columns.Select(p => $"   [{(p.IsImage ? "c" : "i")}].[{p.ColumnName}]").ToArray();
            var columnNamesAsRemoved = table.Columns.Select(p => $"   [{(p.IsImage ? "l" : "d")}].[{p.ColumnName}]").ToArray();

            var columnNamesBigIndentAsInserted = table.Columns.Select(p => $"     [{(p.IsImage ? "c" : "i")}].[{p.ColumnName}]").ToArray();

            // DELETE

            b.AppendLine($"CREATE TRIGGER [{table.SchemaName}].[{table.TableName}OnDelete] ON [{table.SchemaName}].[{table.TableName}] AFTER DELETE");
            b.AppendLine($"AS");
            b.AppendLine($"");

            if (table.HasImageColumns)
            {
                b.AppendLine($"DECLARE @lastId BIGINT");
                b.AppendLine($"DECLARE @lastAuditId BIGINT");
                b.AppendLine($"");
                b.AppendLine($"SELECT @lastId = [{table.TableName}Id] FROM [deleted]");
                b.AppendLine($"SELECT @lastAuditId = MAX([{table.TableName}AuditId]) FROM [{table.SchemaName}].[{table.TableName}Audit] WHERE [{table.SchemaName}].[{table.TableName}Audit].[{table.TableName}Id] = @lastId");
                b.AppendLine($"");
            }

            b.AppendLine($"INSERT [{table.SchemaName}].[{table.TableName}Audit]");
            b.AppendLine($"(");
            b.AppendLine($"   [AuditType],[AuditOn],[AuditBy],");

            b.AppendLine(String.Join($",{Environment.NewLine}", columnNames));

            b.AppendLine($")");
            b.AppendLine($"SELECT");
            b.AppendLine($"   'D',GETDATE(),SUSER_SNAME(),");
            b.AppendLine(String.Join($",{Environment.NewLine}", columnNamesAsRemoved));
            b.AppendLine($"FROM");
            b.AppendLine($"   [deleted] AS [d]");

            if (table.HasImageColumns)
            {
                b.AppendLine($"LEFT JOIN");
                b.AppendLine($"   [{table.SchemaName}].[{table.TableName}Audit] AS [l] ON [l].[{table.TableName}AuditId] = @lastAuditID");
            }

            b.AppendLine($"GO");
            b.AppendLine($"");

            // INSERT

            b.AppendLine($"CREATE TRIGGER [{table.SchemaName}].[{table.TableName}OnInsert] ON [{table.SchemaName}].[{table.TableName}] AFTER INSERT");
            b.AppendLine($"AS");
            b.AppendLine($"");
            b.AppendLine($"INSERT [{table.SchemaName}].[{table.TableName}Audit]");
            b.AppendLine($"(");
            b.AppendLine($"   [AuditType],[AuditOn],[AuditBy],");

            b.AppendLine(String.Join($",{Environment.NewLine}", columnNames));

            b.AppendLine($")");
            b.AppendLine($"SELECT");
            b.AppendLine($"   'I',GETDATE(),SUSER_SNAME(),");
            b.AppendLine(String.Join($",{Environment.NewLine}", columnNamesAsInserted));
            b.AppendLine($"FROM");
            b.AppendLine($"   [inserted] AS [i]");

            if (table.HasImageColumns)
            {
                b.AppendLine($"JOIN");
                b.AppendLine($"   [{table.SchemaName}].[{table.TableName}] AS [c] ON [c].[{table.TableName}Id] = [i].[{table.TableName}Id]");
            }

            b.AppendLine($"GO");
            b.AppendLine($"");

            // UPDATE

            b.AppendLine($"CREATE TRIGGER [{table.SchemaName}].[{table.TableName}OnUpdate] ON [{table.SchemaName}].[{table.TableName}] AFTER UPDATE");
            b.AppendLine($"AS");
            b.AppendLine($"");

            if (table.HasImageColumns)
            {
                b.AppendLine($"DECLARE @lastId BIGINT");
                b.AppendLine($"DECLARE @lastAuditId BIGINT");
                b.AppendLine($"");
                b.AppendLine($"SELECT @lastId = [{table.TableName}Id] FROM [deleted]");
                b.AppendLine($"SELECT @lastAuditId = MAX([{table.TableName}AuditId]) FROM [{table.SchemaName}].[{table.TableName}Audit] WHERE [{table.SchemaName}].[{table.TableName}Audit].[{table.TableName}Id] = @lastId");
                b.AppendLine($"");
            }

            b.AppendLine($"DECLARE @isSame AS TINYINT");
            b.AppendLine($"");
            b.AppendLine($"SELECT @isSame = ");
            b.AppendLine($"  (SELECT COUNT(*) FROM inserted AS i, deleted AS d");

            if (table.HasImageColumns)
            {
                b.AppendLine($"    LEFT JOIN [{table.TableName}Audit] AS [l] ON [l].[{table.TableName}AuditId] = @lastAuditId");
                b.AppendLine($"    JOIN [{table.TableName}] AS [c] ON [c].[{table.TableName}Id] = @lastId");
            }

            b.AppendLine($"  WHERE");

            var conditions = new List<string>();

            foreach (var column in table.Columns)
            {
                if (column.IsImage)
                {
                    conditions.Add($"   (((CONVERT({getColumnConvertType(column)}, [c].[{column.ColumnName}])) = CONVERT({getColumnConvertType(column)},[l].[{column.ColumnName}])) OR ([c].[{column.ColumnName}] IS NULL AND [l].[{column.ColumnName}] IS NULL))");
                }
                else
                {
                    if (column.IsNullable)
                    {
                        conditions.Add($"   (([i].[{column.ColumnName}] = [d].[{column.ColumnName}]) OR ([i].[{column.ColumnName}] IS NULL AND [d].[{column.ColumnName}] IS NULL))");
                    }
                    else
                    {
                        conditions.Add($"   ([i].[{column.ColumnName}] = [d].[{column.ColumnName}])");
                    }
                }
            }

            b.AppendLine(String.Join($" AND{Environment.NewLine}", conditions));

            b.AppendLine($"  )");
            b.AppendLine($"IF @isSame = 0");
            b.AppendLine($"BEGIN");

            b.AppendLine($"  INSERT [{table.SchemaName}].[{table.TableName}Audit]");
            b.AppendLine($"  (");
            b.AppendLine($"     [AuditType],[AuditOn],[AuditBy],");

            b.AppendLine(String.Join($",{Environment.NewLine}", columnNamesBigIndent));

            b.AppendLine($"  )");
            b.AppendLine($"  SELECT");
            b.AppendLine($"     'U',GETDATE(),SUSER_SNAME(),");
            b.AppendLine(String.Join($",{Environment.NewLine}", columnNamesBigIndentAsInserted));
            b.AppendLine($"  FROM");
            b.AppendLine($"     [inserted] AS [i]");

            if (table.HasImageColumns)
            {
                b.AppendLine($"  JOIN");
                b.AppendLine($"     [{table.SchemaName}].[{table.TableName}] AS [c] ON [c].[{table.TableName}Id] = [i].[{table.TableName}Id]");
            }

            b.AppendLine($"END");
            b.AppendLine($"GO");
            b.AppendLine($"");

            return b.ToString();
        }

        private static string generateDropAuditTable(Table table)
        {
            var b = new StringBuilder();

            if (table.HasImageColumns)
            {
                // b.AppendLine($"DROP INDEX [IX_{table.TableName}Audit_{table.TableName}ID] ON [{table.SchemaName}].[{table.TableName}]");
                b.AppendLine($"IF EXISTS(SELECT * FROM sys.indexes WHERE object_id = object_id('[{table.SchemaName}].[{table.TableName}Audit]') AND NAME ='IX_{table.TableName}Audit_{table.TableName}ID') DROP INDEX [IX_{table.TableName}Audit_{table.TableName}ID] ON [{table.SchemaName}].[{table.TableName}Audit];");
                b.AppendLine($"GO");
            }

            b.AppendLine($"IF OBJECT_ID ('[{table.SchemaName}].[{table.TableName}Audit]', 'TABLE') IS NOT NULL DROP TABLE [{table.SchemaName}].[{table.TableName}Audit];");
            // b.AppendLine($"DROP TABLE IF EXISTS [{table.SchemaName}].[{table.TableName}Audit]");
            b.AppendLine($"GO");

            return b.ToString();
        }

        private static string generateDropAuditTriggers(Table table)
        {
            var b = new StringBuilder();

            b.AppendLine($"IF OBJECT_ID ('[{table.SchemaName}].[{table.TableName}OnDelete]', 'TR') IS NOT NULL DROP TRIGGER [{table.SchemaName}].[{table.TableName}OnDelete];");
            // b.AppendLine($"DROP TRIGGER IF EXISTS [{table.SchemaName}].[{table.TableName}OnDelete]");
            b.AppendLine($"GO");

            b.AppendLine($"IF OBJECT_ID ('[{table.SchemaName}].[{table.TableName}OnInsert]', 'TR') IS NOT NULL DROP TRIGGER [{table.SchemaName}].[{table.TableName}OnInsert];");
            // b.AppendLine($"DROP TRIGGER IF EXISTS [{table.SchemaName}].[{table.TableName}OnInsert]");
            b.AppendLine($"GO");

            b.AppendLine($"IF OBJECT_ID ('[{table.SchemaName}].[{table.TableName}OnUpdate]', 'TR') IS NOT NULL DROP TRIGGER [{table.SchemaName}].[{table.TableName}OnUpdate];");
            // b.AppendLine($"DROP TRIGGER IF EXISTS [{table.SchemaName}].[{table.TableName}OnUpdate]");
            b.AppendLine($"GO");

            return b.ToString();
        }

        private static async Task<IEnumerable<TableColumn>> getTableColumns()
        {
            var tableColumns = new List<TableColumn>();

            using (var sqlConnection = new SqlConnection(connectionString))
            {
                await sqlConnection.OpenAsync();

                using (var sqlCmd = new SqlCommand(retrieveStructureSql, sqlConnection))
                {
                    using (var reader = await sqlCmd.ExecuteReaderAsync())
                    {
                        while (await reader.ReadAsync())
                        {
                            tableColumns.Add(new TableColumn()
                            {
                                SchemaName = reader.GetString(0),
                                TableName = reader.GetString(1),
                                ColumnName = reader.GetString(2),
                                TypeName = reader.GetString(3),
                                MaxLength = reader.GetInt16(4),
                                Precision = reader.GetByte(5),
                                Scale = reader.GetByte(6),
                                IsNullable = reader.GetBoolean(7),
                                IsIdentity = reader.GetBoolean(8),
                                IsImage = isColumnImage(reader.GetString(3))
                            });
                        }
                    }
                }
            }

            return tableColumns;
        }
    }

    class Table
    {
        public string SchemaName { get; set; }
        public string TableName { get; set; }
        public bool HasImageColumns { get; set; }
        public TableColumn[] Columns { get; set; }
    }

    class TableColumn
    {
        public string SchemaName { get; set; }
        public string TableName { get; set; }
        public string ColumnName { get; set; }
        public string TypeName { get; set; }
        public int MaxLength { get; set; }
        public int Precision { get; set; }
        public int Scale { get; set; }
        public bool IsNullable { get; set; }
        public bool IsIdentity { get; set; }
        public bool IsImage { get; set; }
    }
}
