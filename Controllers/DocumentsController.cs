using Microsoft.AspNetCore.Mvc;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Options;
using System.Data;
using System.Reflection.Metadata;
using System.Text.Json;

namespace StreamingNoSQL.Controllers
{
    [ApiController]
    [Route("api/documents")]
    public class DocumentsController : ControllerBase
    {
        private readonly string _connectionString;

        public DocumentsController(IConfiguration configuration)
        {
            _connectionString = configuration.GetConnectionString("DefaultConnection");
        }

        [HttpPost("bulk")]
        public async Task<IActionResult> PostBulk()
        {

            // Stream JSON from request body
            var options = new JsonSerializerOptions
            {
                PropertyNameCaseInsensitive = true
            };

            // Stream JSON from request body
            var documents = JsonSerializer.DeserializeAsyncEnumerable<JsonElement>(Request.Body, options);
            var batch = new List<string>();
            const int batchSize = 1000;

            await foreach (var doc in documents)
            {
                // Convert JsonElement back to string
                string jsonString = doc.GetRawText();
                batch.Add(jsonString);

                if (batch.Count >= batchSize)
                {
                    await SaveBatchToDatabase(batch);
                    batch.Clear();
                }
            }

            if (batch.Count > 0)
            {
                await SaveBatchToDatabase(batch);
            }

            return Ok();
        }

        private async Task SaveBatchToDatabase(List<string> jsonDocuments)
        {
            using var connection = new SqlConnection(_connectionString);
            await connection.OpenAsync();

            using var bulkCopy = new SqlBulkCopy(connection)
            {
                DestinationTableName = "Documents"
            };

            bulkCopy.ColumnMappings.Add("DocData", "DocData");

            var dataTable = new DataTable();
            dataTable.Columns.Add("DocData", typeof(string));

            foreach (var json in jsonDocuments)
            {
                dataTable.Rows.Add(json);
            }

            await bulkCopy.WriteToServerAsync(dataTable);
        }
    }
}
