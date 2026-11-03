"""Client serialization tests; live service tests are in the branch PoC suite."""
import json
from google.protobuf import descriptor_pb2, descriptor_pool, message_factory
from pyspark.sql.connect import proto
from sail_nutmeg import TYPE_URL, extension
from sail_nutmeg.client import ENVELOPE_TYPE_URL, ExtensionRelation


def envelope_class():
    file = descriptor_pb2.FileDescriptorProto(name="nutmeg_test_envelope.proto", package="nutmeg.test", syntax="proto3")
    file.dependency.append(proto.Plan.DESCRIPTOR.file.name)
    message = file.message_type.add(name="Envelope")
    for name, number, kind in [("payload_type_url",1,9),("payload",2,12),("inputs",3,11),("envelope_version",5,13)]:
        field = message.field.add(name=name, number=number, type=kind, label=3 if name == "inputs" else 1)
        if name == "inputs":
            field.type_name = ".spark.connect.Plan"
    pool = descriptor_pool.Default()
    pool.Add(file)
    return message_factory.GetMessageClass(pool.FindMessageTypeByName("nutmeg.test.Envelope"))


def test_manifest_checked_without_importing_native_module():
    manifest = extension().manifest()
    assert manifest["datafusion_version"] == "55.2.0"
    assert manifest["arrow_version"] == "59.2.0"
    assert manifest["relation_types"][0]["type_url"] == TYPE_URL


def test_bare_algorithm_payload_and_plan_id():
    request = {"version":1,"verb":"run","graph":"g","algorithm":"pagerank"}
    relation = ExtensionRelation(request).plan(None)
    assert relation.extension.type_url == TYPE_URL
    assert json.loads(relation.extension.value) == request
    assert relation.HasField("common")


def test_two_plans_are_visible_in_the_host_envelope():
    class Frame:
        _plan = ExtensionRelation({"version":1,"verb":"drop","graph":"input"})
    request = {"version":1,"verb":"stage","graph":"g"}
    relation = ExtensionRelation(request, [Frame(), Frame()]).plan(None)
    assert relation.extension.type_url == ENVELOPE_TYPE_URL
    envelope = envelope_class().FromString(relation.extension.value)
    assert envelope.envelope_version == 1
    assert envelope.payload_type_url == TYPE_URL
    assert json.loads(envelope.payload) == request
    assert len(envelope.inputs) == 2
    assert all(plan.root.extension.type_url == TYPE_URL for plan in envelope.inputs)
