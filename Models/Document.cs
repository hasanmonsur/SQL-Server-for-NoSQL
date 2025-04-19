using System.Text.Json;

namespace StreamingNoSQL.Models
{
    public class Document
    {
        public int Id { get; set; }

        // This will represent the JSON content stored in DocData
        public string DocData { get; set; }

        // Optional convenience property if you also want to work with deserialized JSON
        public string DocType
        {
            get
            {
                if (string.IsNullOrWhiteSpace(DocData)) return null;
                using var doc = JsonDocument.Parse(DocData);
                if (doc.RootElement.TryGetProperty("type", out var typeElement))
                {
                    return typeElement.GetString();
                }
                return null;
            }
        }
    }
}
