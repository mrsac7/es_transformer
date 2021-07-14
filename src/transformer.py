import argparse
import re

from elasticsearch import Elasticsearch
from elasticsearch.client import indices
from elasticsearch.exceptions import ElasticsearchException, NotFoundError
from elasticsearch.helpers import bulk


class Transformer(object):
    """
    Parses documents, extracts required fields and inserts them to the
    elasticsearch server.
    """

    THRESHOLD = 10000
    TARGET_OUTSET = "2021-06-01T00:00:00.000Z"
    SOURCE_OUTSET = "2021-06-01 00:00:00.000"

    def __init__(self, target_ip="localhost", source_ip="localhost"):
        """
        Establishes connection to the elasticsearch server.

        Parameters
        ----------
        target_ip : str, optional
            ip of the target (default is 'localhost')
        source_ip : str, optional
            ip of the source (default is 'localhost')
        """
        port = 9200
        target_server = [{"host": target_ip, "port": port}]
        source_server = [{"host": source_ip, "port": port}]
        try:
            self.client = Elasticsearch(hosts=target_server, timeout=300)
            self.source = Elasticsearch(hosts=source_server, timeout=300)
        except Exception as e:
            print(e)
            print("Failed to establish connection with ES server.")

    def create_target_config(self, index):
        """
        Creates a config index which stores time range of the documents
        present in the indices.

        Parameters
        ----------
        index : str
            The name of the index
        """
        target_config = index + ".target_config"
        if not self.client.indices.exists(target_config):
            body = {
                "settings": {"number_of_shards": 1, "number_of_replicas": 1},
                "mappings": {
                    "config": {
                        "properties": {
                            "index_id": {"type": "long"},
                            "begin_timestamp": {"type": "date"},  # begin is exclusive
                            "end_timestamp": {"type": "date"},  # end is inclusive
                        }
                    }
                },
            }
            try:
                self.client.indices.create(index=target_config, body=body)
            except ElasticsearchException as e:
                print(e)
                print("Failed to create target config index.")

        response = self.client.cat.count(target_config, params={"format": "json"})

        if int(response[0]["count"]) == 0:
            self.__create_new_index(index, 1, self.TARGET_OUTSET)
            self.client.indices.refresh(target_config)
            print(
                "Created `{}` index for storing metadata of target".format(
                    target_config
                )
            )

    def create_source_config(self, index):
        """
        Creates an index to store information about the scroll_id and graylog
        from which the data is extracted.

        Parameters
        ----------
        index : str
            The name of the index
        """
        source_config = index + ".source_config"
        if not self.client.indices.exists(source_config):
            body = {
                "settings": {"number_of_shards": 1, "number_of_replicas": 1},
                "mappings": {
                    "config": {
                        "properties": {
                            "timestamp": {
                                "type": "date",
                                "format": "yyyy-MM-dd HH:mm:ss.SSS",
                            }
                        }
                    }
                },
            }
            try:
                self.client.indices.create(index=source_config, body=body)
            except ElasticsearchException as e:
                print(e)
                print("Failed to create source config index")

        response = self.client.cat.count(source_config, params={"format": "json"})

        if int(response[0]["count"]) == 0:
            self.client.index(
                index=source_config,
                doc_type="config",
                body={"timestamp": self.SOURCE_OUTSET},
                id=1,
            )
            self.client.indices.refresh(source_config)
            print(
                "Created `{}` index for storing metadata of source".format(
                    source_config
                )
            )

    def create_ingest_pipeline(self, index):
        """
        Creates an ingest pipeline which puts a timestamp when a docuemnts is
        indexed.

        Parameters
        ----------
        index : str
            The name of the index
        """
        pipeline_id = index + ".pipeline"
        try:
            self.client.ingest.get_pipeline(pipeline_id)
        except NotFoundError:
            body = {
                "description": "Creates a timestamp when a document is initially indexed",
                "processors": [
                    {
                        "set": {
                            "field": "_source.created_at",
                            "value": "{{_ingest.timestamp}}",
                        }
                    }
                ],
            }
            try:
                self.client.ingest.put_pipeline(id=pipeline_id, body=body)
                print(
                    "Created `{}` for adding document creation timestamp".format(
                        pipeline_id
                    )
                )
            except ElasticsearchException as e:
                print(e)
                print("Failed to create ingest pipeline.")

    def create_index_template(self, index):
        """
        Creates a template in the easticsearch server based on the given
        pattern.

        Parameters
        ----------
        index : str
            The name of the index
        """
        template_name = index + ".template"
        if not self.client.indices.exists_template(name=template_name):
            pattern = index + "_*"
            pipeline = index + ".pipeline"
            mapping = {
                "user_id": {"type": "long"},
                "client_id": {"type": "long"},
                "partner_id": {"type": "long"},
                "module": {"type": "keyword"},
                "page": {"type": "keyword"},
                "uri": {"type": "keyword"},
                "app_type": {"type": "keyword"},
                "created_at": {"type": "date"},
                "request_time": {"type": "date"},
                "duration": {"type": "long"},
            }
            body = {
                "index_patterns": [pattern],
                "template": {
                    "settings": {
                        "number_of_shards": 1,
                        "number_of_replicas": 1,
                        "index.default_pipeline": pipeline,
                    },
                    "mappings": {"dynamic": "true", "docs": {"properties": mapping}},
                },
            }
            try:
                self.client.indices.put_template(name=template_name, body=body)
                print(
                    "Created `{}` for putting mapping on the indices".format(
                        template_name
                    )
                )
            except ElasticsearchException as e:
                print(e)
                print("Failed to create index template.")

    def reindex(self, source, target, batch_size=100):
        """
        Fetches documents from the source, parses and inserts into the target.

        Parameters
        ----------
        source : str
            The name of the source index
        target : str
            The name of the target index
        batch_size : int
            Batch size for fetching documents (default is 100)
        """
        source_config = target + ".source_config"
        result = self.client.get(index=source_config, doc_type="config", id=1)
        timestamp = result["_source"]["timestamp"]

        search_body = {
            "size": batch_size,
            "query": {"range": {"timestamp": {"gt": timestamp}}},
            "sort": [{"timestamp": "asc"}],
        }
        response = self.source.search(
            index=source,
            body=search_body,
            scroll="10m",
        )
        prev_scroll_id = response["_scroll_id"]

        print("Documents reindexing started...")

        count = 0
        while len(response["hits"]["hits"]):
            documents, new_timestamp = self.parse(response["hits"]["hits"])
            self.insert(target, documents)
            count += len(documents)
            print("Inserted {} Documents".format(count))

            if new_timestamp > timestamp:
                self.client.update(
                    index=source_config,
                    doc_type="config",
                    id=1,
                    body={"doc": {"timestamp": new_timestamp}},
                )
                timestamp = new_timestamp

            return
            response = self.source.scroll(scroll_id=prev_scroll_id, scroll="10m")
            scroll_id = response["_scroll_id"]

        print("Documents reindexing finished successfully.")

    def insert(self, index, documents, batch_size=100):
        """
        Sends request for inserting data into the elasticsearch database.

        Parameters
        ----------
        index : str
            The name of the index
        documents : list
            The list of documents that is to be inserted
        batch_size : int, optional
            Batch size for indexing (default is 100)
        """
        actions = []
        latest_index_id, begin_timestamp = self.__get_latest_index(index)

        for idx, doc in enumerate(documents):
            index_id = latest_index_id

            if doc["request_time"] <= begin_timestamp:
                index_id = self.get_query_index(index, doc["request_time"])

            action = {
                "_index": index + "_" + str(index_id),
                "_type": "docs",
                "_source": doc,
            }
            actions.append(action)

            if len(actions) == batch_size or idx == len(documents) - 1:
                print("Bulk ingesting started...")
                bulk(self.client, actions, raise_on_error=True, request_timeout=200)
                actions.clear()
                print("Bulked ingesting done")
                if self.__get_index_size(index, latest_index_id) >= self.THRESHOLD:
                    begin_timestamp = self.__update_index_timerange(
                        index, latest_index_id
                    )
                    latest_index_id = self.__create_new_index(
                        index, latest_index_id + 1, begin_timestamp
                    )

    def parse(self, documents):
        """
        Extracts required fileds from the given data and creates a list of
        dictionaries.

        Parameters
        ----------
        documents : list
            List of dictionaries containing the data

        Returns
        -------
        tuple (list, str)
            A list of dictionaries containing the parsed data, and
            max value of the timestamp among the documents.
        """

        parsed_documents = []
        max_timestamp = ""
        for data in documents:
            log = data["_source"]

            if "time_taken" not in log or "@timestamp" not in log:
                continue

            duration = log["time_taken"]
            request_time = log["@timestamp"]
            partner_id, client_id, user_id = self.__extract_user_context(log)
            module = self.__extract_module(log)
            uri = self.__extract_uri(log)
            page = log["header_referer"] if "header_referer" in log else "UNKNOWN"
            app_type = log["sprAppType"] if "sprAppType" in log else "UNKNOWN"

            doc = {
                "user_id": user_id,
                "client_id": client_id,
                "partner_id": partner_id,
                "module": module,
                "page": page,
                "uri": uri,
                "app_type": app_type,
                "request_time": request_time,
                "duration": duration,
            }
            parsed_documents.append(doc)

            max_timestamp = max(max_timestamp, log["timestamp"])

        return parsed_documents, max_timestamp

    @staticmethod
    def __extract_user_context(log):
        """
        Extracts `partner_id`, `client_id` and `user_id` of the request from a
        log.

        Parameters
        ----------
        log : dictionary
            a dictionary containing the log data

        Returns
        -------
        tuple
            a tuple containing `partner_id`, `client_id` and `user_id` of the
            request
        """
        user_context = [
            log[level] if level in log else "UNKNOWN"
            for level in ("partner_id", "client_id", "user_id")
        ]
        if "UNKNOWN" in user_context and "header_x-user-context" in log:
            regex = re.findall(r"\d+", log["header_x-user-context"])
            user_context = list(map(int, regex))

        return user_context

    @staticmethod
    def __extract_module(log):
        """
        Extracts `module` of the request from a log.

        Parameters
        ----------
        log : dictionary
            a dictionary containing the log data

        Returns
        -------
        str
            `module` of the request
        """
        module = "UNKNOWN"
        if "module" in log:
            module = log["module"]
        elif "executorName" in log:
            module = log["executorName"]
        elif "http_uri" in log:
            module = Transformer.__extract_module_from_uri(log["http_uri"])
        return module

    @staticmethod
    def __extract_module_from_uri(uri):
        """
        Finds module name given `http_uri` using a text_search

        Paramters
        ---------
        uri : str
            `http_uri` of the request

        Returns
        -------
        str
            module of the request
        """
        modules_dict = {
            "BENCHMARKING": ["BENCHMARKING"],
            "CARE": ["CARE"],
            "CASE_MANAGEMENT": ["UNIVERSAL_CASE"],
            "COMMENT": ["COMMENT"],
            "ENGAGEMENT": ["ENGAGEMENT"],
            "GOVERNANCE": ["GOVERNANCE"],
            "INBOUND_MESSAGE": ["INBOUND_MESSAGE"],
            "LISTENING": ["LISTENING"],
            "MARKETING": ["MARKETING"],
            "METADATA": ["METADATA"],
            "META_CONTENT": ["META_CONTENT"],
            "OUTBOUND": ["OUTBOUND"],
            "OUTBOUND-STREAM-FEED": ["OUTBOUND-STREAM-FEED"],
            "OUTBOUND_MESSAGE": ["OUTBOUND_MESSAGE"],
            "PAID": ["PAID"],
            "PLATFORM": ["PLATFORM"],
            "PUBLISHING": ["PUBLISHING"],
            "RDB_FIREHOSE": ["RDB_FIREHOSE"],
            "REPORTING": ["REPORTING"],
            "SAM": ["SAM", "/sam/"],
            "spellcheck-grammar": ["spellcheck", "grammar"],
            "SPR_TASK": ["SPR_TASK"],
            "SUGGESTION": ["SUGGESTION"],
            "UGC": ["UGC"],
        }
        matching_module = [
            mod
            for mod in modules_dict
            if any(keyword in uri for keyword in modules_dict[mod])
        ]
        if len(matching_module) == 1:
            return matching_module[0]

        else:
            return "UNKNOWN"

    @staticmethod
    def __extract_uri(log):
        """
        Extracts `uri` of the request from a log.

        Parameters
        ----------
        log : dictionary
            a dictionary containing the log data

        Returns
        -------
        str
            `uri` of the request
        """
        uri = "UNKNOWN"
        if "http_uri" in log:
            uri = log["http_uri"]
        elif "uri-category" in log:
            uri = log["uri-category"]
        return uri

    def __get_index_size(self, index, index_id):
        """
        Returns the size of index in megabytes.

        Parameters
        ----------
        index : str
            The name of the index
        index_id : int
            id of the index.

        Returns
        -------
        float
            Size of the index in MB
        """
        index_name = index + "_" + str(index_id)
        if self.client.indices.exists(index_name):
            response = self.client.indices.stats(index_name)
            size = response["_all"]["primaries"]["store"]["size_in_bytes"]
            return size / (10 ** 6)
        return 0

    def __update_index_timerange(self, index, index_id):
        """
        Sets and returns the `end_timestamp` of an index based on the timeranges
        of the documents present inside it.

        Parameters
        ----------
        index : str
            The name of the index
        index_id : int
            id of the index

        Returns
        -------
        str
            `end_timestamp` of the index
        """
        index_name = index + "_" + str(index_id)
        target_config = index + ".target_config"

        begin_timestamp = self.client.get(
            index=target_config, doc_type="config", id=index_id
        )["_source"]["begin_timestamp"]

        end_timestamp = self.client.search(
            index=index_name,
            body={
                "size": 0,
                "aggs": {"end_timestamp": {"max": {"field": "request_time"}}},
            },
        )["aggregations"]["end_timestamp"]["value_as_string"]

        self.client.update(
            index=target_config,
            doc_type="config",
            id=index_id,
            body={
                "doc": {
                    "index_id": index_id,
                    "begin_timestamp": begin_timestamp,
                    "end_timestamp": end_timestamp,
                }
            },
        )
        return end_timestamp

    def __create_new_index(self, index, index_id, begin_timestamp):
        """
        Creates a new index based on the given alias and returns its `index_id`.

        Parameters
        ----------
        index : str
            The name of the index
        index_id : int
            id of the index
        begin_timestamp: str
            begin_timestamp of the index

        Returns
        -------
        int
            `index_id` of the newly created index
        """
        index_name = index + "_" + str(index_id)
        target_config = index + ".target_config"
        self.client.index(
            index=target_config,
            doc_type="config",
            id=index_id,
            body={"index_id": index_id, "begin_timestamp": begin_timestamp},
        )
        return index_id

    def __get_latest_index(self, index):
        """
        Returns the details of the most recent index instance of the given
        alias.

        Parameters
        ----------
        index : str
            The name of the index

        Returns
        -------
        tuple
            `index_id` and `begin_timestamp` of the latest index
        """
        target_config = index + ".target_config"
        body = {"query": {"bool": {"must_not": {"exists": {"field": "end_timestamp"}}}}}
        result = self.client.search(index=target_config, body=body)
        latest_index = result["hits"]["hits"][0]["_source"]

        return latest_index["index_id"], latest_index["begin_timestamp"]


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        prog="transformer",
        description="Parses documents, extracts required fields and inserts them to the elasticsearch server.",
    )
    parser.add_argument(
        "-c", "--config", action="store_true", help="configure metadata for the indices"
    )
    parser.add_argument(
        "--source_ip", type=str, default="localhost", help="ip of source"
    )
    parser.add_argument("--source", type=str, help="name of the source index")

    required = parser.add_argument_group("required arguments")
    required.add_argument(
        "--target_ip",
        type=str,
        default="localhost",
        help="ip of the target",
    )
    required.add_argument(
        "--target", type=str, required=True, help="name of the target index"
    )

    args = parser.parse_args()
    ts = Transformer(args.target_ip, args.source_ip)

    if args.config is None and args.source is None:
        print("No action requested, add --config or --source")

    if args.config is not None:
        ts.create_target_config(args.target)
        ts.create_source_config(args.target)
        ts.create_ingest_pipeline(args.target)
        ts.create_index_template(args.target)

    if args.source is not None:
        ts.reindex(args.source, args.target)
