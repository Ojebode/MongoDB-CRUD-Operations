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
    database = siemplify.parameters.get("Database Name")
    collection = siemplify.parameters.get("Collection Name")
    data = siemplify.parameters.get("Data")
    
    try:
        document = json.loads(data)

    except Exception as e:
        siemplify.end("Invalid json query. Please try again. {0}".format(e),
                      'false')

    mongodb_manager = MongoDBManager(integration_conf)
    
    res, ids = mongodb_manager.insert_documents(database, collection, document)

    if res:
        siemplify.LOGGER.info('Successfully added data to database \nID: {}'.format(ids))
        output_message = 'Successfully added data to database. \n  ID: {} '.format(ids)
        result_value = 'true'

        
    siemplify.LOGGER.info("\n  status: {}\n  result_value: {}\n  output_message: {}".format(status,result_value, output_message))
    siemplify.end(output_message, result_value, status)


if __name__ == "__main__":
    main()
