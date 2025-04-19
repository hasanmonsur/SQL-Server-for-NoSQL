using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Data.SqlClient;
using StreamingNoSQL.Models;
using System.Data;
using System.Text.Json;

namespace StreamingNoSQL.Controllers
{
    [ApiController]
    [Route("api/contacts")]
    public class ContactsController : ControllerBase
    {
        private readonly string _connectionString;

        public ContactsController(IConfiguration configuration)
        {
            _connectionString = configuration.GetConnectionString("DefaultConnection");
        }

        [HttpPost("bulk")]
        public async Task<IActionResult> PostBulk()
        {
            // Stream JSON from request body
            var contacts = JsonSerializer.DeserializeAsyncEnumerable<Contact>(Request.Body, new JsonSerializerOptions
            {
                PropertyNameCaseInsensitive = true
            });

            var batch = new List<Contact>();
            const int batchSize = 1000;

            await foreach (var contact in contacts)
            {
                batch.Add(contact);
                if (batch.Count >= batchSize)
                {
                    await SaveBatchToDatabase(batch);
                    batch.Clear();
                }
            }

            // Save remaining items
            if (batch.Count > 0)
            {
                await SaveBatchToDatabase(batch);
            }

            return Ok();
        }

        private async Task SaveBatchToDatabase(List<Contact> contacts)
        {
            using var connection = new SqlConnection(_connectionString);
            await connection.OpenAsync();

            using var bulkCopy = new SqlBulkCopy(connection);
            bulkCopy.DestinationTableName = "Contacts";
            bulkCopy.ColumnMappings.Add("Name", "Name");
            bulkCopy.ColumnMappings.Add("Email", "Email");

            var dataTable = new DataTable();
            dataTable.Columns.Add("Name", typeof(string));
            dataTable.Columns.Add("Email", typeof(string));

            foreach (var contact in contacts)
            {
                dataTable.Rows.Add(contact.Name, contact.Email);
            }

            await bulkCopy.WriteToServerAsync(dataTable);
        }
    }
}
