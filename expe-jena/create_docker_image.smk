
PARENT_DIR = "/GDD/Thi/sage-jena-benchmarks"

rule create_jena_image:
    output:
        out=f"{PARENT_DIR}/expe-jena/docker/jena-latest.tar"
    input:
        dockerfile=f"{PARENT_DIR}/expe-jena/Dockerfile"
    shell:
        """
        mkdir -p {PARENT_DIR}/expe-jena/docker/ &&
        docker build -t jena:latest -f {input.dockerfile} . &&
        docker save jena:latest > {output.out}
        """