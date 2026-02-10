import subprocess
import os
import multiprocessing
from tqdm import tqdm


def bam_to_fastq(bam_file, output_prefix):
    """ Convert BAM to FASTQ using GATK's SamToFastq """
    print(f"Converting BAM to FASTQ for {os.path.basename(bam_file)}...")
    # Update the path to your GATK jar file accordingly
    gatk_path = "/data01/home/ls/leibguy/miniconda3/envs/star_38/share/gatk4-4.3.0.0-0/gatk-package-4.3.0.0-local.jar"
    cmd = [
        'java', '-jar', gatk_path,
        'SamToFastq',
        '-I', bam_file,
        '--FASTQ', f"{output_prefix}_R1.fq.gz",
        '--SECOND_END_FASTQ', f"{output_prefix}_R2.fq.gz"
    ]
    subprocess.run(cmd, check=True)


def process_bam(args):
    """ Process a single BAM file: convert to FASTQ """
    bam_file, output_dir = args  # Unpack the tuple into variables

    output_prefix = os.path.join(output_dir, os.path.basename(bam_file).split('.')[0])
    
    # Check if the final output files already exist
    final_output_files = [f"{output_prefix}_R1.fq.gz", f"{output_prefix}_R2.fq.gz"]
    if all(os.path.exists(f) for f in final_output_files):
        print(f"Output for {os.path.basename(bam_file)} already exists. Skipping.")
        return
    bam_to_fastq(bam_file, output_prefix)


def main(bam_dir, output_dir):
    bam_files = [os.path.join(bam_dir, f) for f in os.listdir(bam_dir) if f.endswith('.bam')]
    with multiprocessing.Pool(processes=50) as pool:
        tasks = [(bam_file, output_dir) for bam_file in bam_files]
        list(tqdm(pool.imap_unordered(process_bam, tasks), total=len(bam_files)))

        
if __name__ == "__main__":
    bam_dir = "/data01/private/resources/to_sort/bams_guy/"
    output_dir = "/data01/private/resources/to_sort/bams_hg38_gatk/"

    main(bam_dir, output_dir)