import os
import shutil

query_multiple_tps_list = [
    "query_379", "query_455", "query_456", "query_439", "query_447", "query_289", "query_394", "query_386",
    "query_14", "query_353", "query_424", "query_631", "query_360", "query_346", "query_385", "query_395",
    "query_17", "query_347", "query_167", "query_597", "query_299", "query_425", "query_434", "query_354",
    "query_662", "query_435", "query_288", "query_356", "query_596", "query_679", "query_598", "query_357",
    "query_358", "query_667", "query_396", "query_513", "query_514", "query_355", "query_601", "query_466",
    "query_391", "query_144", "query_599", "query_468", "query_148", "query_646", "query_512", "query_146",
    "query_469"
]
query_opts_list =[
                    'query_455', 'query_42', 'query_278', 'query_44', 'query_267',
                    'query_229', 'query_281', 'query_228', 'query_453', 'query_272',
                    'query_207', 'query_273', 'query_116', 'query_441', 'query_115',
                    'query_283', 'query_45', 'query_110', 'query_142', 'query_330',
                    'query_259', 'query_225', 'query_226', 'query_321', 'query_178',
                    'query_196', 'query_180', 'query_52', 'query_208', 'query_59',
                    'query_418', 'query_391', 'query_371', 'query_430', 'query_359',
                    'query_431', 'query_197', 'query_230', 'query_390', 'query_370',
                    'query_383', 'query_360', 'query_433', 'query_408'
                ]

source_dir = "/GDD/Thi/sage-jena-benchmarks/queries/wdbench-opts"
destination_dir = "/GDD/Thi/sage-jena-benchmarks/selected_queries/wdbench-opts"

if not os.path.exists(destination_dir):
    os.makedirs(destination_dir)

for query in query_opts_list:
    query_file = f"{query}.sparql"
    source_path = os.path.join(source_dir, query_file)
    destination_path = os.path.join(destination_dir, query_file)

    if os.path.exists(source_path):
        shutil.copy(source_path, destination_path)
    else:
        print(f"File {query_file} not found in {source_dir}")
