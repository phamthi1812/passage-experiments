

PARENT_DIR = "/GDD/Thi/passage-experiments"

rule create_passage_image:
    output:
        out=f"{PARENT_DIR}/expe-jena/docker/jena-latest.tar"
    input:
        jar=f"{PARENT_DIR}/expe-jena/jena-cli-jar-with-dependencies.jar",
        dockerfile=f"{PARENT_DIR}/expe-jena/Dockerfile"
    shell:
        """
        mkdir -p {PARENT_DIR}/expe-jena/docker/ &&
        docker build -t jena:latest -f {input.dockerfile} . &&
        docker save jena:latest > {output.out}
        """