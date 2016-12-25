using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Threading.Tasks;
using System.Web.Http;
using Microsoft.Azure.Documents;
using Microsoft.Azure.Documents.Client;
using Swashbuckle.Swagger.Annotations;
using TRex.Metadata;

namespace DocDBNotificationApi.Controllers
{
    public class PatientController : ApiController
    {
        /// <summary>
        ///     Gets the new or modified patient documents.
        /// </summary>
        /// <param name="resourceType">Type of the resource.</param>
        /// <returns></returns>
        [Metadata("GetNewOrModifiedPatientDocuments",
             "Query for new or Modifed FHIR Documents By Resource Type"
         )]
        [SwaggerResponse(HttpStatusCode.OK, type: typeof(Task<dynamic>))]
        [SwaggerResponse(HttpStatusCode.NotFound, "No New or Modifed Documents ")]
        [SwaggerOperation("GetNewOrModifiedPatientDocuments")]
        public async Task<dynamic> GetNewOrModifiedPatientDocuments(
            [Metadata("Resource Type")] string resourceType)
        {
            var collectionLink = UriFactory.CreateDocumentCollectionUri(DocumentDbContext.DatabaseId,
                DocumentDbContext.CollectionId);
            var context = new DocumentDbContext();
            var docs = new List<dynamic>();
            var partitionKeyRanges = new List<PartitionKeyRange>();
            FeedResponse<PartitionKeyRange> pkRangesResponse;

            do
            {
                pkRangesResponse = await context.Client.ReadPartitionKeyRangeFeedAsync(collectionLink);
                partitionKeyRanges.AddRange(pkRangesResponse);
            } while (pkRangesResponse.ResponseContinuation != null);

            foreach (var pkRange in partitionKeyRanges)
            {
                var changeFeedOptions = new ChangeFeedOptions
                {
                    StartFromBeginning = true,
                    RequestContinuation = null,
                    MaxItemCount = -1,
                    PartitionKeyRangeId = pkRange.Id
                };

                var query = context.Client.CreateDocumentChangeFeedQuery(collectionLink, changeFeedOptions);

                do
                {
                    var response = await query.ExecuteNextAsync<dynamic>();
                    if (response.Count > 0)
                        docs.AddRange(
                            response.AsEnumerable().Where(d => d.ResourceType == resourceType).Cast<Document>());
                } while (query.HasMoreResults);
            }
            return docs;
        }
    }
}