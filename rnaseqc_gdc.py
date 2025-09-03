import os
import subprocess
from concurrent.futures import ProcessPoolExecutor


def run_rnaseqc_docker(bam_file):
    """
    Run RnaSeqC on a single BAM file using Docker.
    
    Args:
    - bam_file: Path to a single BAM file.
    - gene_model_path: Path to the gene model file.
    - output_dir: Directory to store RnaSeqC output.
    - docker_image: Name of the RnaSeqC Docker image.
    """

    gene_model_path = "/home/ls/leibguy/projects/neoantigens_project/splicing/gencode.v36.GRCh38.genes.gtf"
    output_dir = ""
    docker_image = "gcr.io/broad-cga-aarong-gtex/rnaseqc:latest"

    sample_name = os.path.splitext(os.path.basename(bam_file))[0]
    sample_output_dir = os.path.join(output_dir, sample_name)
    
    print(f"Running RnaSeqC analysis for {sample_name}...")
    
    # Ensure individual sample output directory exists
    os.makedirs(sample_output_dir, exist_ok=True)
    
    # Get current user and group IDs
    user_id = os.getuid()
    group_id = os.getgid()

    # Construct the Docker command
    docker_command = [
        'docker', 'run', '--rm',
        '--user', f"{user_id}:{group_id}",
        '-v', f"{os.path.dirname(bam_file)}:/data/bam",
        '-v', f"{gene_model_path}:/data/gene_model.gtf",
        '-v', f"{sample_output_dir}:/data/output",
        docker_image,
        'rnaseqc',
        '/data/gene_model.gtf',
        f"/data/bam/{os.path.basename(bam_file)}",
        '/data/output',
        '-s', sample_name,
        '--verbose'
    ]

    # Run the command
    subprocess.run(docker_command, check=True)
    print(f"RnaSeqC analysis completed for {sample_name}")

bam_dir = ""
bam_files = [os.path.join(bam_dir, f) for f in os.listdir(bam_dir) if f.endswith('.bam')]

# Setup the executor
n_workers = 10
with ProcessPoolExecutor(max_workers=n_workers) as executor:
    input_iterator = iter(bam_files)
    futures = []
    keep_running = True
    for _ in range(n_workers):
        try:
            futures.append(executor.submit(run_rnaseqc_docker, next(input_iterator)))
        except StopIteration:
            keep_running = False
            break

    # Handle completion and submission of new tasks
    while keep_running:
        completed = [future for future in futures if future.done()]
        for future in completed:
            futures.remove(future)
            try:
                futures.append(executor.submit(run_rnaseqc_docker, next(input_iterator)))
            except StopIteration:
                keep_running = False
                break
