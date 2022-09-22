using Dapper;
using System.Data;
using System.Data.SqlClient;

namespace DbTableSync
{
    class Trade
    {
        public long Id { get; set; }
        public string Asset { get; set; }
        public string Symbol { get; set; }
        public bool IsBuyer { get; set; }
        public bool IsMaker { get; set; }
        public double Price { get; set; }
        public double Quantity { get; set; }
        public double QuoteQuantity { get; set; }
        public DateTimeOffset TradeTime { get; set; }
    }

    internal class Program
    {
        static async Task Main(string[] args)
        {
            string tableName = "Trades";

            var sourceConnectionString = "Server=.; Database=TraderDb; Trusted_Connection=True;";
            using var sourceConnection = new SqlConnection(sourceConnectionString);
            var guids = sourceConnection.Query<long>($"SELECT Id FROM {tableName} ORDER BY Id").ToList();

            var destinationConnectionString = "Server=.; Database=Sandbox; Trusted_Connection=True;";
            await ClearTable(destinationConnectionString, tableName);

            //IDbTransfer dapperDbTransfer = new DapperDbTransfer();
            IDbTransfer sqlBulkCopyDbTransfer = new SqlBulkCopyDbTransfer();
            foreach (var segment in guids.Chunk(2000))
            {
                //await sqlBulkCopyDbTransfer.TransferDataAsync(sourceConnectionString, destinationConnectionString, tableName, segment.ToList());
                await sqlBulkCopyDbTransfer.TransferDataAsync<Trade>(sourceConnectionString, destinationConnectionString, tableName, segment.ToList());
            }
        }

        private static async Task<int> ClearTable(string connectionString, string tableName)
        {
            using var dbConnection = new SqlConnection(connectionString);

            var sql = $"DELETE FROM {tableName}";
            return await dbConnection.ExecuteAsync(sql);
        }
    }

    public class DapperDbTransfer: IDbTransfer
    {
        public DapperDbTransfer()
        {
        }

        public async Task<int> TransferDataAsync(string sourceConnectionString, string destinationConnectionString, string tableName, List<long> ids)
        {
            using var sourceConnection = new SqlConnection(sourceConnectionString);

            var querySql = $"SELECT * FROM {tableName} WHERE Id IN @ids";
            var sourceData = (await sourceConnection.QueryAsync(querySql, new { ids })).ToList();

            if (sourceData.Any())
            {
                var queriedColumns = sourceData.GetPropertyNames();

                var columnNames = string.Join(", ", queriedColumns);
                var parameterNames = string.Join(", ", queriedColumns.Select(cn => $"@{cn}"));
                var insertSql = $"INSERT INTO {tableName} ({columnNames}) VALUES ({parameterNames})";

                using var destinationConnection = new SqlConnection(destinationConnectionString);
                return await destinationConnection.ExecuteAsync(insertSql, sourceData);
            }

            return 0;
        }

        public async Task<int> TransferDataAsync<T>(string sourceConnectionString, string destinationConnectionString, string tableName, List<long> ids)
        {
            using var sourceConnection = new SqlConnection(sourceConnectionString);

            var querySql = $"SELECT * FROM {tableName} WHERE Id IN @ids";
            var sourceData = (await sourceConnection.QueryAsync<T>(querySql, new { ids })).ToList();

            if (sourceData.Any())
            {
                var queriedColumns = sourceData.GetPropertyNames();

                var columnNames = string.Join(", ", queriedColumns);
                var parameterNames = string.Join(", ", queriedColumns.Select(cn => $"@{cn}"));
                var insertSql = $"INSERT INTO {tableName} ({columnNames}) VALUES ({parameterNames})";

                using var destinationConnection = new SqlConnection(destinationConnectionString);
                return await destinationConnection.ExecuteAsync(insertSql, sourceData);
            }

            return 0;
        }
    }

    public class SqlBulkCopyDbTransfer : IDbTransfer
    {
        public SqlBulkCopyDbTransfer() {
        }

        public async Task<int> TransferDataAsync(string sourceConnectionString, string destinationConnectionString, string tableName, List<long> ids)
        {
            using var sourceConnection = new SqlConnection(sourceConnectionString);

            var querySql = $"SELECT * FROM {tableName} WHERE Id IN @ids";
            var sourceData = (await sourceConnection.QueryAsync(querySql, new { ids })).ToList();

            if (sourceData.Any())
            {
                using (var bc = new SqlBulkCopy(destinationConnectionString, SqlBulkCopyOptions.CheckConstraints & SqlBulkCopyOptions.FireTriggers))
                {
                    var dt = ConvertToDataTable(sourceData);

                    bc.DestinationTableName = tableName;

                    await bc.WriteToServerAsync(dt);
                }

                return sourceData.Count();
            }

            return 0;
        }

        public async Task<int> TransferDataAsync<T>(string sourceConnectionString, string destinationConnectionString, string tableName, List<long> ids)
        {
            using var sourceConnection = new SqlConnection(sourceConnectionString);

            var querySql = $"SELECT * FROM {tableName} WHERE Id IN @ids";
            var sourceData = (await sourceConnection.QueryAsync<T>(querySql, new { ids })).ToList();

            if (sourceData.Any())
            {
                using (var bc = new SqlBulkCopy(destinationConnectionString, SqlBulkCopyOptions.CheckConstraints & SqlBulkCopyOptions.FireTriggers))
                {
                    var dt = ConvertToDataTable(sourceData);

                    bc.DestinationTableName = tableName;

                    await bc.WriteToServerAsync(dt);
                }

                return sourceData.Count();
            }

            return 0;
        }

        private DataTable ConvertToDataTable<T>(List<T> items)
        {
            var table = new DataTable();

            var propertyNames = items.GetPropertyNames();

            foreach (var name in propertyNames)
            {
                table.Columns.Add(name);
            }

            foreach (var item in items)
            {
                var data = item as IDictionary<string, object>;

                var row = table.NewRow();
                foreach (var name in propertyNames)
                {
                    if (data?.ContainsKey(name) == true)
                    {
                        row[name] = data[name];
                    }
                }
                table.Rows.Add(row);
            }

            return table;
        }

        private DataTable ConvertToDataTable(List<dynamic> items)
        {
            var table = new DataTable();

            var propertyNames = items.GetPropertyNames();

            foreach (var name in propertyNames)
            {
                table.Columns.Add(name);
            }

            foreach (var item in items)
            {
                var data = item as IDictionary<string, object>;

                var row = table.NewRow();
                foreach (var name in propertyNames)
                {
                    if (data?.ContainsKey(name) == true)
                    {
                        row[name] = data[name];
                    }
                }
                table.Rows.Add(row);
            }

            return table;
        }
    }

    public interface IDbTransfer
    {
        Task<int> TransferDataAsync(string sourceConnectionString, string destinationConnectionString, string tableName, List<long> ids);
        Task<int> TransferDataAsync<T>(string sourceConnectionString, string destinationConnectionString, string tableName, List<long> ids);
    }

    public static class ListExtensions
    {
        public static List<string> GetPropertyNames(this List<dynamic> items)
        {
            if (items == null) return Enumerable.Empty<string>().ToList();

            var firstRecord = items.FirstOrDefault();
            var lookup = firstRecord as IDictionary<string, object>;
            return lookup?.Keys.ToList() ?? Enumerable.Empty<string>().ToList();
        }
        public static List<string> GetPropertyNames<T>(this List<T> _)
        {
            var propertyNames = typeof(T).GetProperties().Select(p => p.Name).ToList();
            return propertyNames;
        }
    }
}