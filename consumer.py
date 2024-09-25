import argparse

from confluent_kafka import Consumer
from confluent_kafka.serialization import SerializationContext, MessageField
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer

class Customer(object):
    """
    Customer record

    Args:
        customer_id (int): Customer ID

        first_name (str): First name

        last_name (str): Last name

        creation_date (datetime): Creation date
    """

    def __init__(self, customer_id=None, first_name=None, last_name=None, creation_date=None):
        self.customer_id = customer_id
        self.first_name = first_name
        self.last_name = last_name
        self.creation_date = creation_date


def dict_to_customer(obj, ctx):
    """
    Converts object literal(dict) to a Customer instance.

    Args:
        obj (dict): Object literal(dict)

        ctx (SerializationContext): Metadata pertaining to the serialization
            operation.
    """

    if obj is None:
        return None

    print("date: {}".format( obj['creation_date'] ))
    return Customer(customer_id=obj['customer_id'],
                first_name=obj['first_name'],
                last_name=obj['last_name'],
                creation_date=obj['creation_date'])


def main(args):
    topic = args.topic

    sr_conf = {'url': args.schema_registry}
    schema_registry_client = SchemaRegistryClient(sr_conf)

    avro_deserializer = AvroDeserializer(schema_registry_client,None,
                                         dict_to_customer)

    consumer_conf = {'bootstrap.servers': args.bootstrap_servers,
                     'group.id': args.group,
                     'auto.offset.reset': "earliest"}

    consumer = Consumer(consumer_conf)
    consumer.subscribe([topic])

    while True:
        try:
            msg = consumer.poll(1.0)
            if msg is None:
                continue

            customer = avro_deserializer(msg.value(), SerializationContext(msg.topic(), MessageField.VALUE))
            if customer is not None:
                print("Customer record {}: customer_id: {}\n"
                      "\tfirst_name: {}\n"
                      "\tlast_name: {}\n"
                      "\tcreation_date: {}\n"
                      .format(msg.key(), customer.customer_id,
                              customer.first_name, customer.last_name,
                              customer.creation_date))
        except KeyboardInterrupt:
            break

    consumer.close()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="AvroDeserializer example")
    parser.add_argument('-b', dest="bootstrap_servers", required=True,
                        help="Bootstrap broker(s) (host[:port])")
    parser.add_argument('-s', dest="schema_registry", required=True,
                        help="Schema Registry (http(s)://host[:port]")
    parser.add_argument('-t', dest="topic", default="example_serde_avro",
                        help="Topic name")
    parser.add_argument('-g', dest="group", default="example_serde_avro",
                        help="Consumer group")

    main(parser.parse_args())