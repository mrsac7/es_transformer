import os
import sys
import unittest

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from src.transformer import Transformer
from elasticsearch.exceptions import NotFoundError

ts = Transformer()
index = "testindex"


class TestTransformer(unittest.TestCase):
    def test_connection(self):
        self.assertTrue(ts.client.ping())

    def test_target_config(self):
        target_config = index + ".target_config"
        if ts.client.indices.exists(target_config):
            ts.client.indices.delete(target_config)

        ts.create_target_config(index)
        response = ts.client.cat.count(target_config, params={"format": "json"})
        self.assertEqual(int(response[0]["count"]), 1)

    def test_source_config(self):
        source_config = index + ".source_config"
        if ts.client.indices.exists(source_config):
            ts.client.indices.delete(source_config)

        ts.create_source_config(index)
        response = ts.client.cat.count(source_config, params={"format": "json"})
        self.assertEqual(int(response[0]["count"]), 1)

    def test_ingest_pipeline(self):
        pipeline_id = index + ".pipeline"
        try:
            ts.client.ingest.get_pipeline(pipeline_id)
        except NotFoundError:
            ts.create_ingest_pipeline(index)
        else:
            ts.client.ingest.delete_pipeline(pipeline_id)
            ts.create_ingest_pipeline(index)

        ts.client.ingest.get_pipeline(pipeline_id)

    def test_index_mapping(self):
        template_name = index + ".template"

        if ts.client.indices.exists_index_template(template_name):
            ts.client.indices.delete_index_template(template_name)

        ts.create_index_template(index)
        self.assertTrue(ts.client.indices.exists_index_template(template_name))


if __name__ == "__main__":
    unittest.main()
