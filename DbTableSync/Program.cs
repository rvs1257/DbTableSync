using Dapper;
using System.Data;
using System.Data.SqlClient;

namespace DbTableSync
{
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

            IDbTransfer dbIntegration = new DapperDbTransfer();
            foreach (var segment in guids.Chunk(2000))
            {
                await dbIntegration.TransferDataAsync(sourceConnectionString, destinationConnectionString, tableName, segment.ToList());
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
            var sourceData = await sourceConnection.QueryAsync(querySql, new { ids });

            var firstRecord = sourceData.FirstOrDefault();
            var lookup = firstRecord as IDictionary<string, object>;

            if (lookup != null)
            {
                var queriedColumns = lookup.Keys;

                var columnNames = string.Join(", ", queriedColumns);
                var parameterNames = string.Join(", ", queriedColumns.Select(cn => $"@{cn}"));
                var insertSql = $"INSERT INTO {tableName} ({columnNames}) VALUES ({parameterNames})";

                using var destinationConnection = new SqlConnection(destinationConnectionString);
                return await destinationConnection.ExecuteAsync(insertSql, sourceData);
            }

            return 0;
        }
    }

    public interface IDbTransfer
    {
        Task<int> TransferDataAsync(string sourceConnectionString, string destinationConnectionString, string tableName, List<long> ids);
    }
}