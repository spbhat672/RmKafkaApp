using RM_API_Kafka.Models;
using RM_API_Kafka.WebMethod;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Http;
using System.Web;
using System.Web.Http;
using System.Web.Mvc;

namespace RM_API_Kafka.Controllers
{
    [System.Web.Http.RoutePrefix("ResourceInfo")]
    public class ResourceInfoController : ApiController
    {       
        [System.Web.Http.HttpGet]
        [System.Web.Http.Route("api/GetResource")]
        public HttpResponseMessage GetResource([FromUri]long? id)
        {
            try
            {
                List<ResourceWithValue> resourceList = ResourceRepository.GetResourceInfo(id);
                return Request.CreateResponse(System.Net.HttpStatusCode.OK, resourceList);
            }
            catch (Exception ex)
            {
                return Request.CreateErrorResponse(System.Net.HttpStatusCode.NotFound, "Server - Error Fetching resource Information");
            }
        }

        [System.Web.Http.HttpPost]
        [System.Web.Http.Route("api/PostResource")]
        public HttpResponseMessage PostResource([FromBody] ResourceWithValue model)
        {
            try
            {   
                long resourceId = ResourceRepository.AddResourceInfo(model);
                ResourceWithValue res = new ResourceWithValue();
                res = model;
                res.Id = resourceId;
                KafkaService.PostResource(model);
                return Request.CreateResponse(System.Net.HttpStatusCode.OK, 202);
            }
            catch (Exception ex)            
            {
                return Request.CreateErrorResponse(System.Net.HttpStatusCode.BadRequest, ex);
            }
        }

        [System.Web.Http.HttpPut]
        [System.Web.Http.Route("api/PutResource")]
        public HttpResponseMessage PutResource([FromBody] ResourceWithValue model)
        {
            try
            {
                long resourceId = ResourceRepository.UpdateResourceInfo(model);
                ResourceWithValue res = new ResourceWithValue();
                res = model;
                res.Id = resourceId;
                //KafkaService.PostResource(model);
                return Request.CreateResponse(System.Net.HttpStatusCode.OK, 202);
            }
            catch (Exception ex)
            {
                return Request.CreateErrorResponse(System.Net.HttpStatusCode.NotFound, ex);
            }
        }

        [System.Web.Http.HttpDelete]
        [System.Web.Http.Route("api/DeleteResource")]
        public HttpResponseMessage DeleteResource([FromUri] long id)
        {
            try
            {
                ResourceRepository.DeleteResourceInfo(id);
                return Request.CreateResponse(System.Net.HttpStatusCode.OK, 202);
            }
            catch (Exception ex)
            {
                return Request.CreateErrorResponse(System.Net.HttpStatusCode.NotFound, ex);
            }
        }

        [System.Web.Http.HttpGet]
        [System.Web.Http.Route("api/GetTypeList")]
        public HttpResponseMessage GetTypeList()
        {
            try
            {
                var typeList = new List<Models.Type>();
                typeList = ResourceRepository.GetTypeInfo();
                return Request.CreateResponse(System.Net.HttpStatusCode.OK, typeList);
            }
            catch (Exception ex)
            {
                return Request.CreateErrorResponse(System.Net.HttpStatusCode.NotFound, "Server - Error Fetching type data");
            }
        }

        [System.Web.Http.HttpGet]
        [System.Web.Http.Route("api/GetStatusList")]
        public HttpResponseMessage GetStatusList()
        {
            try
            {
                var statusList = new List<Models.Status>();
                statusList = ResourceRepository.GetStatusInfo();
                return Request.CreateResponse(System.Net.HttpStatusCode.OK, statusList);
            }
            catch (Exception ex)
            {
                return Request.CreateErrorResponse(System.Net.HttpStatusCode.NotFound, "Server - Error Fetching status data");
            }
        }
    }
}