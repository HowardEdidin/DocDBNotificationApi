MIT License

Copyright (c) 2017 Howard Edidin

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.



using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Threading.Tasks;
using System.Web.Http;
using Microsoft.Azure.Documents;
using Microsoft.Azure.Documents.Client;
using Swashbuckle.Swagger.Annotations;
using TRex.Metadata;

namespace DocDBNotificationApi.Controllers
{
    /// <summary>
    ///     FHIR Resource Type Controller
    /// </summary>
    /// <seealso cref="System.Web.Http.ApiController" />
    public class FhirResourceTypeController : ApiController
    {
        /// <summary>
        ///     Gets the new or modified FHIR documents from Last Run Date
        /// </summary>
        /// <param name="resourceType">FHIR resource type value</param>
        /// <param name="lastDateTime">Date Time Last Run</param>
        /// <param name="databaseId"></param>
        /// <param name="collectionId"></param>
        /// <returns></returns>
        [Metadata("Get New Or Modified FHIR Documents",
             "Query for new or modifed FHIR Documents By Resource Type " +
             "from Last Run Date"
         )] 
        [SwaggerResponse(HttpStatusCode.OK, type: typeof(Task<dynamic>))]
        [SwaggerResponse(HttpStatusCode.NotFound, "No New or Modifed Documents found")]
        [SwaggerOperation("GetNewOrModifiedFHIRDocuments")]
        public async Task<dynamic> GetNewOrModifiedFhirDocuments(
            [Metadata("Database Id", "Database Id")] string databaseId,
            [Metadata("Collection Id", "Collection Id")] string collectionId,
            [Metadata("Resource Type", "FHIR resource type value")] string resourceType,
            [Metadata("Last Run Date", "Start Date value ")] DateTime lastDateTime)
        {
            // Convert lastDateTime to double
            var timeStamp = ConvertToTimestamp(lastDateTime);

            var collectionLink = UriFactory.CreateDocumentCollectionUri(databaseId, collectionId);

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

                using (var query = context.Client.CreateDocumentChangeFeedQuery(collectionLink, changeFeedOptions))
                {
                    do
                    {
                        if (query != null)
                        {
                            var response = await query.ExecuteNextAsync<dynamic>();
                            if (response.Count > 0)
                                docs.AddRange(
                                    response.AsEnumerable()
                                        .Where(r => (r.ResourceType == resourceType) && (r._ts >= timeStamp))
                                        .Cast<Document>());
                        }
                        else
                        {
                            var response = new HttpResponseMessage
                            {
                                StatusCode = HttpStatusCode.NotFound,
                                ReasonPhrase = "No New or Modifed Documents found"
                            };

                            return response;
                        }
                    } while (query.HasMoreResults);
                }
            }
            return docs;
        }

        private static double ConvertToTimestamp(DateTime currentdateTime)
        {
            double result;

            try
            {
                var newDateTime = DateTime.Parse(currentdateTime.ToString(CultureInfo.InvariantCulture));

                //create Timespan by subtracting the value provided from the Unix Epoch
                var span = newDateTime - new DateTime(1970, 1, 1, 0, 0, 0, 0).ToLocalTime();

                //return the total seconds (which is a UNIX timestamp)
                result = span.TotalSeconds;
            }
            catch (Exception e)
            {
                throw new Exception("unable to convert to Timestamp", e.InnerException);
            }

            return result;
        }
    }
}
