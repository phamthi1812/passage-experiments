


PARENT_DIR = "/GDD/passage-experiments"

rule create_passage_image:
    output:
        out=f"{PARENT_DIR}/expe-passage/docker/passage-latest.tar"
    input:
        jar=f"{PARENT_DIR}/expe-passage/sager-jar-with-dependencies.jar",
        dockerfile=f"{PARENT_DIR}/expe-passage/Dockerfile"
    shell:
        """
        mkdir -p {PARENT_DIR}/expe-passage/docker/ &&
        docker build -t passage:latest -f {input.dockerfile} . &&
        docker save passage:latest > {output.out}
        """

