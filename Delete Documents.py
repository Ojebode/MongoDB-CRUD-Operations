from SiemplifyAction import SiemplifyAction
from SiemplifyUtils import unix_now, convert_unixtime_to_datetime, output_handler
from ScriptResult import EXECUTION_STATE_COMPLETED, EXECUTION_STATE_FAILED,EXECUTION_STATE_TIMEDOUT
from MongoDBManager import MongoDBManager
import json

INTEGRATION_NAME = "MongoDBV2"


@output_handler
def main():
    siemplify = SiemplifyAction()

    status = EXECUTION_STATE_COMPLETED  # used to flag back to siemplify system, the action final status
    output_message = "output message :"  # human readable message, showed in UI as the action result
    result_value = 'false'  # Set a simple result value, used for playbook if\else and placeholders.


   # INIT INTEGRATION CONFIGURATION:
    integration_conf  = siemplify.get_configuration(INTEGRATION_NAME)
    
   # INIT ACTION PARAMETERS:
    collection = siemplify.parameters.get("Collection Name")
    database = siemplify.parameters.get("Database Name")
    query = siemplify.parameters.get("Filter")
    
    
    try:
        query = json.loads(query)

    except Exception as e:
        siemplify.end("Invalid json query. Please try again. {0}".format(e),
                      'false')
                      
                      
    mongodb_manager = MongoDBManager(integration_conf)
    
    res = mongodb_manager.delete_documents(database, collection, query)
    
    if res:
        siemplify.LOGGER.info('successfully deleted {} document(s)'.format(res.deleted_count))
        output_message = 'successful: {} document(s) deleted.'.format(res.deleted_count)
        result_value = 'true'
    else:
        siemplify.LOGGER.error('Error deleting data')
        
        
    siemplify.LOGGER.info("\n  status: {}\n  result_value: {}\n  output_message: {}".format(status,result_value, output_message))
    siemplify.end(output_message, result_value, status)


if __name__ == "__main__":
    main()
