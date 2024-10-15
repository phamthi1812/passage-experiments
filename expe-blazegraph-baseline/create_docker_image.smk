
PARENT_DIR = "/GDD/Thi/passage-experiments"

rule create_passage_image:
    output:
        out=f"{PARENT_DIR}/expe-blazegraph-baseline/docker/bg-cli.tar"
    input:
        jar=f"{PARENT_DIR}/expe-blazegraph-baseline/blazegraph-cli.jar",
        dockerfile=f"{PARENT_DIR}/expe-blazegraph-baseline/Dockerfile"
    shell:
        """
        mkdir -p {PARENT_DIR}/expe-blazegraph-baseline/docker/ &&
        docker build -t bg-cli:latest -f {input.dockerfile} . &&
        docker save bg-cli:latest > {output.out}
        """