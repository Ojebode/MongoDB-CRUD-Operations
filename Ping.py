from SiemplifyUtils import output_handler
from SiemplifyAction import SiemplifyAction
from MongoDBManager import MongoDBManager


INTEGRATION_NAME = "MongoDBV2"

@output_handler
def main():
    siemplify = SiemplifyAction()
    
    integration_conf  = siemplify.get_configuration(INTEGRATION_NAME)

    mongodb_manager = MongoDBManager(integration_conf)

    # Check if the connection is established or not.
    mongodb_manager.test_connectivity()

    # If no exception, connection is successful
    output_message = "Successfully connected to MongoDB at {0}:{1}.".format(integration_conf['Server Address'], integration_conf['Port'])
    siemplify.end(output_message, True)


if __name__ == '__main__':
    main()
