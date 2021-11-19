from SiemplifyUtils import output_handler
from SiemplifyAction import SiemplifyAction
from MongoDBManager import MongoDBManager
import json


INTEGRATION_NAME = "MongoDBV2"


@output_handler
def main():
    siemplify = SiemplifyAction()
    
    # INIT INTEGRATION CONFIGURATION:
    integration_conf  = siemplify.get_configuration(INTEGRATION_NAME)

   # INIT ACTION PARAMETERS:
    database = siemplify.parameters.get("Database Name")
    collection = siemplify.parameters.get("Collection Name")
    query = siemplify.parameters.get("Query")
    show_simple_json = siemplify.parameters.get("Return a single JSON result")

    #show_simple_json = extract_action_param(siemplify, param_name="Return a single JSON result", is_mandatory=False, default_value=False, print_value=True)

    
    try:
        query = json.loads(query)

    except Exception as e:
        siemplify.end("Invalid json query. Please try again. {0}".format(e),
                      'false')
        
    mongodb_manager = MongoDBManager(integration_conf)

    # Run search query
    results = mongodb_manager.execute_query(query, database, collection) or []
    
    # Close the connection
    mongodb_manager.close_connection()

    if results and not show_simple_json:
        for i, document in enumerate(results, 1):
            siemplify.result.add_json("Query Results - Document {0}".format(i),
                                      json.dumps(document))

        siemplify.end(
            "Successfully finished search. Found {0} matching documents.".format(
                len(results)), 'true')
        
    if results and show_simple_json:
        for i, document in enumerate(results, 1):
            siemplify.result.add_result_json(json.dumps(results))

        siemplify.end(
            "Successfully finished search. Found {0} matching documents.".format(
                len(results)), 'true')

    siemplify.result.add_result_json(json.dumps(results or []))

    siemplify.end(
        "Cannot find query results. Please check your query {0}".format(query),
        'true')


if __name__ == "__main__":
    main()

